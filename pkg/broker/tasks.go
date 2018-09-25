package broker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/golang/glog"
	"net/http"
	"os"
	"time"
)

type TaskAction string

const (
	DeleteTask                           TaskAction = "delete"
	ResyncFromProviderTask               TaskAction = "resync-from-provider"
	ResyncFromProviderUntilAvailableTask TaskAction = "resync-until-available"
	NotifyCreateServiceWebhookTask       TaskAction = "notify-create-service-webhook"
	NotifyCreateBindingWebhookTask       TaskAction = "notify-create-binding-webhook"
)

type Task struct {
	Id         string
	Action     TaskAction
	DatabaseId string
	Status     string
	Retries    int64
	Metadata   string
	Result     string
	Started    *time.Time
	Finished   *time.Time
}

type WebhookTaskMetadata struct {
	Url    string `json:"url"`
	Secret string `json:"secret"`
}

func FinishedTask(storage Storage, taskId string, retries int64, result string, status string) {
	var t = time.Now()
	err := storage.UpdateTask(taskId, &status, &retries, nil, &result, nil, &t)
	if err != nil {
		glog.Errorf("Unable to update task %s due to: %s (taskId: %s, retries: %d, result: [%s], status: [%s]\n", taskId, err.Error(), taskId, retries, result, status)
	}
}

func UpdateTaskStatus(storage Storage, taskId string, retries int64, result string, status string) {
	err := storage.UpdateTask(taskId, &status, &retries, nil, &result, nil, nil)
	if err != nil {
		glog.Errorf("Unable to update task %s due to: %s (taskId: %s, retries: %d, result: [%s], status: [%s]\n", taskId, err.Error(), taskId, retries, result, status)
	}
}

func RunPreprovisionTasks(ctx context.Context, o Options, namePrefix string, storage Storage, wait int64) {
	t := time.NewTicker(time.Second * time.Duration(wait))
	dbEntries, err := storage.StartProvisioningTasks()
	if err != nil {
		glog.Errorf("Get pending tasks failed: %s\n", err.Error())
		return
	}
	for _, entry := range dbEntries {
		glog.Infof("Starting preprovisioning database: %s with plan: %s\n", entry.Id, entry.PlanId)

		plan, err := storage.GetPlanByID(entry.PlanId)
		if err != nil {
			glog.Errorf("Unable to provision, cannot find plan: %s, %s\n", entry.PlanId, err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}

		provider, err := GetProviderByPlan(namePrefix, plan)
		if err != nil {
			glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}

		dbInstance, err := provider.Provision(entry.Id, plan, "preprovisioned")
		if err != nil {
			glog.Errorf("Error provisioning database: %s\n", err.Error())
			storage.NukeInstance(entry.Id)
			continue
		}

		if err = storage.UpdateInstance(dbInstance, dbInstance.Plan.ID); err != nil {
			glog.Errorf("Error inserting record into provisioned table: %s\n", err.Error())

			if err = provider.Deprovision(dbInstance, false); err != nil {
				glog.Errorf("Error cleaning up (deprovision failed) after insert record failed but provision succeeded (Database Id:%s Name: %s) %s\n", dbInstance.Id, dbInstance.Name, err.Error())
				if _, err = storage.AddTask(dbInstance.Id, DeleteTask, dbInstance.Name); err != nil {
					glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", dbInstance.Name, err.Error())
				}
			}
			continue
		}
		if dbInstance.Status != "available" {
			if _, err = storage.AddTask(dbInstance.Id, ResyncFromProviderUntilAvailableTask, ""); err != nil {
				glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", dbInstance.Name, err.Error())
			}
		}
		glog.Infof("Finished preprovisioning database: %s with plan: %s\n", entry.Id, entry.PlanId)
		<-t.C
	}
}

func TickTocPreprovisionTasks(ctx context.Context, o Options, namePrefix string, storage Storage) {
	next_check := time.NewTicker(time.Second * 60 * 5)
	for {
		RunPreprovisionTasks(ctx, o, namePrefix, storage, 60)
		<-next_check.C
	}
}

func RunWorkerTasks(ctx context.Context, o Options, namePrefix string, storage Storage) error {

	t := time.NewTicker(time.Second * 60)
	for {
		<-t.C
		storage.WarnOnUnfinishedTasks()

		task, err := storage.PopPendingTask()
		if err != nil && err.Error() != "sql: no rows in result set" {
			glog.Errorf("Getting a pending task failed: %s\n", err.Error())
			return err
		} else if err != nil && err.Error() == "sql: no rows in result set" {
			// Nothing to do...
			continue
		}

		glog.Infof("Started task: %s\n", task.Id)

		if task.Action == DeleteTask {
			glog.Infof("Delete and deprovision database for task: %s\n", task.Id)

			if task.Retries >= 10 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to delete database "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}

			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)

			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			provider, err := GetProviderByPlan(namePrefix, dbInstance.Plan)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get provider: "+err.Error(), "pending")
				continue
			}
			if err = provider.Deprovision(dbInstance, true); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to deprovision: "+err.Error(), "pending")
				continue
			}
			if err = storage.DeleteInstance(dbInstance); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to delete: "+err.Error(), "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ResyncFromProviderTask {
			glog.Infof("Resyncing from provider for task: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			dbEntry, err := storage.GetInstance(task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get database instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get DbEntry: "+err.Error(), "pending")
				continue
			}
			if dbInstance.Status != dbEntry.Status {
				if err = storage.UpdateInstance(dbInstance, dbInstance.Plan.ID); err != nil {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
					continue
				}
			} else {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check", "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ResyncFromProviderUntilAvailableTask {
			glog.Infof("Resyncing from provider until available for task: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for database "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			if err = storage.UpdateInstance(dbInstance, dbInstance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
				continue
			}
			if dbInstance.Status != "available" {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check", "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == NotifyCreateServiceWebhookTask {

			if task.Retries >= 60 {
				FinishedTask(storage, task.Id, task.Retries, "Unable to deliver webhook: "+task.Result, "failed")
				continue
			}

			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			if dbInstance.Status != "available" {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check", "pending")
				continue
			}

			byteData, err := json.Marshal(map[string]interface{}{"state": "succeeded", "description": "available"})
			// seems like this would be more useful, but whatevs: byteData, err := json.Marshal(dbInstance)

			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot marshal dbInstance to json: "+err.Error(), "pending")
				continue
			}

			var taskMetaData WebhookTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to callback on create service: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot unmarshal task metadata to callback on create service: "+err.Error(), "pending")
				continue
			}

			h := hmac.New(sha256.New, []byte(taskMetaData.Secret))
			h.Write(byteData)
			sha := base64.StdEncoding.EncodeToString(h.Sum(nil))

			client := &http.Client{}
			req, err := http.NewRequest("POST", taskMetaData.Url, bytes.NewReader(byteData))
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to create http post request: "+err.Error(), "pending")
				continue
			}
			req.Header.Add("content-type", "application/json")
			req.Header.Add("x-osb-signature", sha)
			resp, err := client.Do(req)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to send http post operation: "+err.Error(), "pending")
				continue
			}
			resp.Body.Close() // ignore it, we dont want to hear it.

			if os.Getenv("RETRY_WEBHOOKS") != "" {
				if resp.StatusCode < 200 || resp.StatusCode > 399 {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Got invalid http status code from hook: "+resp.Status, "pending")
					continue
				}
				FinishedTask(storage, task.Id, task.Retries, resp.Status, "finished")
			} else {
				if resp.StatusCode < 200 || resp.StatusCode > 399 {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Got invalid http status code from hook: "+resp.Status, "failed")
				} else {
					FinishedTask(storage, task.Id, task.Retries, resp.Status, "finished")
				}
			}
		}
		// TODO: create binding NotifyCreateBindingWebhookTask

		glog.Infof("Finished task: %s\n", task.Id)
	}
	return nil
}

func RunBackgroundTasks(ctx context.Context, o Options) error {
	storage, namePrefix, err := InitFromOptions(ctx, o)
	if err != nil {
		return err
	}

	go TickTocPreprovisionTasks(ctx, o, namePrefix, storage)
	return RunWorkerTasks(ctx, o, namePrefix, storage)
}

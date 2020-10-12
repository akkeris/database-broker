package broker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

type TaskAction string

const (
	DeleteTask                           TaskAction = "delete"
	ResyncFromProviderTask               TaskAction = "resync-from-provider"
	ResyncFromProviderUntilAvailableTask TaskAction = "resync-until-available"
	NotifyCreateServiceWebhookTask       TaskAction = "notify-create-service-webhook"
	NotifyCreateBindingWebhookTask       TaskAction = "notify-create-binding-webhook"
	ChangeProvidersTask                  TaskAction = "change-providers"
	ChangePlansTask                      TaskAction = "change-plans"
	RestoreDbTask                        TaskAction = "restore-database"
	PerformPostProvisionTask             TaskAction = "perform-post-provision"
	ResyncReplicasFromProviderTask       TaskAction = "resync-replicas-from-provider-task"
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

type ChangeProvidersTaskMetadata struct {
	Plan string `json:"plan"`
}

type ChangePlansTaskMetadata struct {
	Plan string `json:"plan"`
}

type RestoreDbTaskMetadata struct {
	Backup string `json:"backup"`
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
			glog.Errorf("Error provisioning database (%s): %s\n", plan.ID, err.Error())
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
		if !IsAvailable(dbInstance.Status) {
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

func RestoreBackup(storage Storage, dbInstance *DbInstance, namePrefix string, backup string) error {
	provider, err := GetProviderByPlan(namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to restore backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return err
	}
	if err = provider.RestoreBackup(dbInstance, backup); err != nil {
		glog.Errorf("Unable to restore backup: %s\n", err.Error())
		return err
	}
	return nil
}

func UpgradeWithinProviders(storage Storage, fromDb *DbInstance, toPlanId string, namePrefix string) (string, error) {
	toPlan, err := storage.GetPlanByID(toPlanId)
	if err != nil {
		return "", err
	}
	fromProvider, err := GetProviderByPlan(namePrefix, fromDb.Plan)
	if err != nil {
		return "", err
	}
	if toPlanId == fromDb.Plan.ID {
		return "", errors.New("Cannot upgrade to the same plan")
	}
	if toPlan.Provider != fromDb.Plan.Provider {
		return "", errors.New("Unable to upgrade, different providers were passed in on both plans")
	}

	// This could take a very long time.
	dbInstance, err := fromProvider.Modify(fromDb, toPlan)
	if err != nil && err.Error() == "This feature is not available on this plan." {
		return UpgradeAcrossProviders(storage, fromDb, toPlanId, namePrefix)
	}
	if err != nil {
		return "", err
	}

	if err = storage.UpdateInstance(dbInstance, dbInstance.Plan.ID); err != nil {
		glog.Errorf("ERROR: Cannot update instance in database after upgrade change %s (to plan: %s) %s\n", dbInstance.Name, dbInstance.Plan.ID, err.Error())
		return "", err
	}

	if !IsAvailable(dbInstance.Status) {
		if _, err = storage.AddTask(dbInstance.Id, ResyncFromProviderTask, ""); err != nil {
			glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", dbInstance.Name, err.Error())
		}
	}
	return "", err
}

func UpgradeAcrossProviders(storage Storage, fromDb *DbInstance, toPlanId string, namePrefix string) (string, error) {
	toPlan, err := storage.GetPlanByID(toPlanId)
	if err != nil {
		return "", err
	}
	toProvider, err := GetProviderByPlan(namePrefix, toPlan)
	if err != nil {
		return "", err
	}
	fromProvider, err := GetProviderByPlan(namePrefix, fromDb.Plan)
	if err != nil {
		return "", err
	}
	if toPlanId == fromDb.Plan.ID {
		return "", errors.New("Cannot upgrade to the same plan")
	}
	if fromDb.Engine != "postgres" {
		return "", errors.New("Can only upgrade across providers on postgres")
	}

	origToDb, err := toProvider.Provision(fromDb.Id, toPlan, "")
	if err != nil {
		return "", err
	}

	var toDb *DbInstance = nil
	t := time.NewTicker(time.Second * 30)
	for i := 0; i < 60; i++ {
		toDb, err = toProvider.GetInstance(origToDb.Name, origToDb.Plan)
		if err != nil {
			glog.Errorf("Unable to get instance of db %s because %s\n", origToDb.Name, err.Error())
			if err = toProvider.Deprovision(origToDb, false); err != nil {
				glog.Errorf("Unable to clean up after error, for %s database! %s\n", origToDb.Name, err.Error())
				if _, err = storage.AddTask(origToDb.Id, DeleteTask, origToDb.Name); err != nil {
					glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", origToDb.Name, err.Error())
				}
			}
			return "", errors.New("The database instance could not be obtained.")
		}
		if i == 59 {
			if err = toProvider.Deprovision(origToDb, false); err != nil {
				glog.Errorf("Unable to clean up after error, for %s database! %s\n", origToDb.Name, err.Error())
				if _, err = storage.AddTask(origToDb.Id, DeleteTask, origToDb.Name); err != nil {
					glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", origToDb.Name, err.Error())
				}
			}
			return "", errors.New("The database provisioning never finished.")
		}
		if IsAvailable(toDb.Status) {
			break
		}
		<-t.C
	}
	if toDb == nil {
		return "", errors.New("The database provisioning never finished, toDb was nil.")
	}

	toDb.Id = fromDb.Id
	toDb.Username = origToDb.Username
	toDb.Password = origToDb.Password

	v := strings.Split(fromDb.Endpoint, ":")
	var extras = " "

	if len(v) == 2 {
		u := strings.Split(v[1], "/")
		extras = extras + "-p " + u[0]
	}
	targetUrl := toDb.Scheme + "://" + toDb.Username + ":" + toDb.Password + "@" + toDb.Endpoint

	cmd := exec.Command("sh", "-c", "set -o pipefail ; PGPASSWORD=\""+fromDb.Password+"\" pg_dump -xOc -d "+fromDb.Name+" -h "+v[0]+extras+" -U "+fromDb.Username+" | psql "+targetUrl)
	var out bytes.Buffer
	cmd.Stderr = &out
	if err = cmd.Run(); err != nil {
		return "", err
	}

	if err = storage.UpdateInstance(toDb, toDb.Plan.ID); err != nil {
		glog.Errorf("Cannot update instance in database after provider change %s (to plan: %s) %s\n", toDb.Name, toDb.Plan.ID, err.Error())
		if err = toProvider.Deprovision(toDb, false); err != nil {
			glog.Errorf("Cannot deprovision database after failure in recording provider change %s %s\n", toDb.Name, err.Error())
			if _, err = storage.AddTask(toDb.Id, DeleteTask, toDb.Name); err != nil {
				glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", toDb.Name, err.Error())
			}
		}
		return "", err
	}

	if err = fromProvider.Deprovision(fromDb, true); err != nil {
		glog.Errorf("Cannot deprovision existing database during provider change %s %s\n", fromDb.Name, err.Error())
		// Do not add this as a task, since the instance id stayed the same and was upgrade it fromDb.Id now
		// represents the samething as toDb. We can only write out to lthe logs and hope someone picks this up.
		glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! Name: %s, Plan Id: %s, Error: %s\n", fromDb.Name, fromDb.Plan.ID, err.Error())
	}

	return out.String(), nil
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
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			provider, err := GetProviderByPlan(namePrefix, dbInstance.Plan)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get provider: "+err.Error(), "pending")
				continue
			}
			replicas, err := storage.HasReplicas(dbInstance)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to check for replicas: "+err.Error(), "pending")
				continue
			}
			if replicas > 0 {
				if err = provider.DeleteReadReplica(dbInstance); err != nil {
					UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to remove replicas: "+err.Error(), "pending")
					continue
				}
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
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			dbEntry, err := storage.GetInstance(task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get database instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get DbEntry: "+err.Error(), "pending")
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
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			if err = storage.UpdateInstance(dbInstance, dbInstance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(dbInstance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check ("+dbInstance.Status+")", "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ResyncReplicasFromProviderTask {
			glog.Infof("Resyncing from provider until available for replica: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to resync information from provider for replica "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			dbInstance, err := GetReplicaById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			if err = storage.UpdateReplica(dbInstance); err != nil {
				glog.Infof("Failed to update replica in database for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot update replica: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(dbInstance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check ("+dbInstance.Status+")", "pending")
				continue
			}
			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == PerformPostProvisionTask {
			glog.Infof("Resyncing from provider until available (for perform post provision) for task: %s\n", task.Id)
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
			if !IsAvailable(dbInstance.Status) {
				glog.Infof("Status did not change at provider for task: %s\n", task.Id)
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "No change in status since last check ("+dbInstance.Status+")", "pending")
				continue
			}

			provider, err := GetProviderByPlan(namePrefix, dbInstance.Plan)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get provider: "+err.Error(), "pending")
				continue
			}

			newDbInstance, err := provider.PerformPostProvision(dbInstance)
			if err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance: "+err.Error(), "pending")
				continue
			}

			if err = storage.UpdateInstance(newDbInstance, newDbInstance.Plan.ID); err != nil {
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Failed to update instance after post provision: "+err.Error(), "pending")
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
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			if !IsAvailable(dbInstance.Status) {
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
		} else if task.Action == ChangePlansTask {
			glog.Infof("Changing plans for database: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to change plans for database "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			var taskMetaData ChangePlansTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to change providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot unmarshal task metadata to change providers: "+err.Error(), "pending")
				continue
			}
			output, err := UpgradeWithinProviders(storage, dbInstance, taskMetaData.Plan, namePrefix)
			if err != nil {
				glog.Infof("Cannot change plans for: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries+1, "Cannot change plans: "+err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, output, "finished")
		} else if task.Action == RestoreDbTask {
			glog.Infof("Restoring database for: %s\n", task.Id)
			if task.Retries >= 60 {
				glog.Infof("Retry limit was reached for task: %s %d\n", task.Id, task.Retries)
				FinishedTask(storage, task.Id, task.Retries, "Unable to restore database "+task.DatabaseId+" as it failed multiple times ("+task.Result+")", "failed")
				continue
			}
			dbInstance, err := GetInstanceById(namePrefix, storage, task.DatabaseId)
			if err != nil {
				glog.Infof("Failed to get provider instance for task: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot get dbInstance: "+err.Error(), "pending")
				continue
			}
			var taskMetaData RestoreDbTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to restore databases: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot unmarshal task metadata to restore databases: "+err.Error(), "pending")
				continue
			}
			if err = RestoreBackup(storage, dbInstance, namePrefix, taskMetaData.Backup); err != nil {
				glog.Infof("Cannot restore backups for: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot restore backup: "+err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, "", "finished")
		} else if task.Action == ChangeProvidersTask {
			glog.Infof("Changing providers for database: %s\n", task.Id)
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
			var taskMetaData ChangeProvidersTaskMetadata
			err = json.Unmarshal([]byte(task.Metadata), &taskMetaData)
			if err != nil {
				glog.Infof("Cannot unmarshal task metadata to change providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot unmarshal task metadata to change providers: "+err.Error(), "pending")
				continue
			}
			output, err := UpgradeAcrossProviders(storage, dbInstance, taskMetaData.Plan, namePrefix)
			if err != nil {
				glog.Infof("Cannot switch providers: %s, %s\n", task.Id, err.Error())
				UpdateTaskStatus(storage, task.Id, task.Retries, "Cannot switch providers: "+err.Error(), "pending")
				continue
			}

			FinishedTask(storage, task.Id, task.Retries, output, "finished")
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

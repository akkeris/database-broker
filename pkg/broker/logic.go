package broker

import (
	"context"
	"encoding/json"
	"github.com/golang/glog"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
)

type BusinessLogic struct {
	ActionBase
	storage    Storage
	namePrefix string
}

func NewBusinessLogic(ctx context.Context, o Options) (*BusinessLogic, error) {
	storage, namePrefix, err := InitFromOptions(ctx, o)
	if err != nil {
		return nil, err
	}

	bl := BusinessLogic{
		storage:    storage,
		namePrefix: namePrefix,
	}

	bl.AddActions("list_backups", "backups", "GET", bl.ActionListBackups)
	bl.AddActions("get_backup", "backups/{backup}", "GET", bl.ActionGetBackup)
	bl.AddActions("create_backup", "backups", "POST", bl.ActionCreateBackup)
	bl.AddActions("restore_backup", "backups/{backup}", "PUT", bl.ActionRestoreBackup)

	bl.AddActions("list_roles", "roles", "GET", bl.ActionListRoles)
	bl.AddActions("get_role", "roles/{role}", "GET", bl.ActionGetRole)
	bl.AddActions("create_role", "roles", "POST", bl.ActionCreateRole)
	bl.AddActions("rotate_role", "roles/{role}", "PUT", bl.ActionRotateRole)
	bl.AddActions("delete_role", "roles/{role}", "DELETE", bl.ActionDeleteRole)

	bl.AddActions("list_logs", "logs", "GET", bl.ActionGetLogs)
	bl.AddActions("get_logs", "logs/{dir}/{file}", "GET", bl.ActionListLogs)

	bl.AddActions("restart", "restart", "PUT", bl.ActionRestart)

	bl.AddActions("get_replica", "replica", "GET", bl.ActionGetReplica)
	bl.AddActions("create_replica", "replica", "PUT", bl.ActionCreateReplica)
	bl.AddActions("delete_replica", "replica", "DELETE", bl.ActionDeleteReplica)

	return &bl, nil
}

func (b *BusinessLogic) GetCatalog(c *broker.RequestContext) (*broker.CatalogResponse, error) {
	response := &broker.CatalogResponse{}
	services, err := b.storage.GetServices()
	if err != nil {
		return nil, err
	}
	osbResponse := &osb.CatalogResponse{Services: services}

	glog.Infof("catalog response: %#+v", osbResponse)

	response.CatalogResponse = *osbResponse
	return response, nil
}

func (b *BusinessLogic) ActionGetReplica(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	dbUrl, err := b.storage.GetReplicas(dbInstance)
	if err != nil {
		glog.Errorf("Unable to get replica: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return dbUrl, nil
}

func (b *BusinessLogic) ActionCreateReplica(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	if dbInstance.Engine != "postgres" {
		return nil, ConflictErrorWithMessage("I do not know how to do this on anything other than postgres.")
	}

	b.Lock()
	defer b.Unlock()

	amount, err := b.storage.HasReplicas(dbInstance)
	if err != nil {
		glog.Errorf("Error determining if database has replicas: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if amount != 0 {
		return nil, ConflictErrorWithMessage("Cannot create a replica, database already has one attached.")
	}

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to create read replica on db, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	newDbInstance, err := provider.CreateReadReplica(dbInstance)
	if err != nil {
		glog.Errorf("Unable to create read replica on db, CreateReadReplica failed: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = b.storage.AddReplica(newDbInstance); err != nil {
		// TODO: Clean up.
		glog.Errorf("Error inserting record into provisioned_replicas table: %s\n", err.Error())
		provider.DeleteReadReplica(newDbInstance)
		if err != nil {
			glog.Errorf("Error cleaning up unrecorded database replica: %#v because %s\n", newDbInstance, err.Error())
			// TODO add task to remove it later?
		}
		return nil, InternalServerError()
	}

	return DatabaseUrlSpec{
		Username: newDbInstance.Username,
		Password: newDbInstance.Password,
		Endpoint: newDbInstance.Endpoint,
	}, nil
}

func (b *BusinessLogic) ActionDeleteReplica(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to delete read replica on db, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	readDbReplica, err := provider.GetReadReplica(dbInstance)

	if err = provider.DeleteReadReplica(readDbReplica); err != nil {
		glog.Errorf("Unable to delete read replica on db, CreateReadReplica failed: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = b.storage.DeleteReplica(readDbReplica); err != nil {
		glog.Errorf("Unable to delete replica: %s\n", err.Error())
		return nil, InternalServerError()
	}

	return map[string]interface{}{"status": "OK"}, nil
}

func (b *BusinessLogic) ActionListRoles(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	roles, err := b.storage.ListRoles(dbInstance)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return []DatabaseUrlSpec{}, nil
	} else if err != nil {
		glog.Errorf("Cannot list roles for instance %s: %s\n", InstanceID, err.Error())
		return nil, InternalServerError()
	}
	return roles, nil
}

func (b *BusinessLogic) ActionGetRole(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	role, err := b.storage.GetRole(dbInstance, vars["role"])
	if err != nil {
		glog.Errorf("Unable to get role in action: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return role, nil
}

func (b *BusinessLogic) ActionCreateRole(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to create read only role on db, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	dbUrl, err := provider.CreateReadOnlyUser(dbInstance)
	if err != nil {
		glog.Errorf("Unable to create read only role, CreateReadOnlyUser failed: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if _, err = b.storage.AddRole(dbInstance, dbUrl.Username, dbUrl.Password); err != nil {
		if delerr := provider.DeleteReadOnlyUser(dbInstance, dbUrl.Username); delerr != nil {
			glog.Errorf("Unable to remove read only role when trying to unwind changes, orphaned read only user: %s on db %s: %s\n", dbUrl.Username, dbInstance.Name, delerr.Error())
		}
		glog.Errorf("Unable to insert the role: %s\n", err.Error())
		return nil, InternalServerError()
	}

	return dbUrl, nil
}

func (b *BusinessLogic) ActionRotateRole(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}

	role := vars["role"]

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to rotate read only password for role, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	dbUrl, err := provider.RotatePasswordReadOnlyUser(dbInstance, role)
	if err != nil {
		glog.Errorf("Unable to rotate password on read only role, RotatePasswordReadOnlyUser failed: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if _, err = b.storage.UpdateRole(dbInstance, role, dbUrl.Password); err != nil {
		glog.Errorf("Error: Unable to record password change for database %s and read only user %s with new password %s\n", dbInstance.Name, role, dbUrl.Password)
		return nil, InternalServerError()
	}

	return dbUrl, nil
}

func (b *BusinessLogic) ActionDeleteRole(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	if dbInstance.Engine != "postgres" {
		return nil, ConflictErrorWithMessage("I do not know how to do this on anything other than postgres.")
	}
	role := vars["role"]

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to rotate read only password for role, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	// ensure the role exists first
	amount, err := b.storage.HasRole(dbInstance, role)
	if err != nil {
		glog.Errorf("Unable to determine if database has role, %s\n", err.Error())
		return nil, InternalServerError()
	}
	if amount == 0 {
		return nil, NotFound()
	}

	if err = provider.DeleteReadOnlyUser(dbInstance, role); err != nil {
		glog.Errorf("Unable to rotate password on read only role, RotatePasswordReadOnlyUser failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if err = b.storage.DeleteRole(dbInstance, role); err != nil {
		glog.Errorf("Unable to delete database role, %s\n", err.Error())
		return nil, InternalServerError()
	}

	return map[string]interface{}{"status": "OK"}, nil
}

func (b *BusinessLogic) ActionListLogs(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to list logs on db, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	logs, err := provider.ListLogs(dbInstance)
	if err != nil {
		glog.Errorf("Unable to get a list of logs: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return logs, nil
}

func (b *BusinessLogic) ActionGetLogs(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	path := vars["dir"] + "/" + vars["file"]
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to get db logs, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	logs, err := provider.GetLogs(dbInstance, path)
	if err != nil {
		glog.Errorf("Unable to get logs, %s\n", err.Error())
		return nil, InternalServerError()
	}
	return logs, nil
}

func (b *BusinessLogic) ActionRestart(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to restart db, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	err = provider.Restart(dbInstance)
	if err != nil {
		glog.Errorf("Unable to restart db, %s\n", err.Error())
		return nil, InternalServerError()
	}
	return map[string]interface{}{"status": "OK"}, nil
}

func (b *BusinessLogic) ActionRestoreBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to restore backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	err = provider.RestoreBackup(dbInstance, vars["backup"])
	if err != nil {
		glog.Errorf("Unable to restore backup: %s\n", err.Error())
	}
	return map[string]interface{}{"status": "OK"}, nil
}

func (b *BusinessLogic) ActionCreateBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to create backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backup, err := provider.CreateBackup(dbInstance)
	if err != nil {
		glog.Errorf("Unable to create backup, create backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backup, nil
}

func (b *BusinessLogic) ActionListBackups(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to list backups, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backups, err := provider.ListBackups(dbInstance)
	if err != nil {
		glog.Errorf("Unable to list backups, create backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backups, nil
}

func (b *BusinessLogic) ActionGetBackup(InstanceID string, vars map[string]string, context *broker.RequestContext) (interface{}, error) {
	dbInstance, err := b.GetInstanceById(InstanceID)
	if err != nil {
		return nil, NotFound()
	}
	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to create backup, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}
	backup, err := provider.GetBackup(dbInstance, vars["backup"])
	if err != nil && err.Error() == "Not found" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to get backup, get backup failed: %s\n", err.Error())
		return nil, InternalServerError()
	}
	return backup, nil
}

func GetInstanceById(namePrefix string, storage Storage, Id string) (*DbInstance, error) {
	entry, err := storage.GetInstance(Id)
	if err != nil {
		return nil, err
	}

	plan, err := storage.GetPlanByID(entry.PlanId)
	if err != nil {
		return nil, err
	}

	provider, err := GetProviderByPlan(namePrefix, plan)
	if err != nil {
		return nil, err
	}

	dbInstance, err := provider.GetInstance(entry.Name, plan)
	if err != nil {
		return nil, err
	}

	dbInstance.Id = entry.Id
	dbInstance.Username = entry.Username
	dbInstance.Password = entry.Password
	dbInstance.Plan = plan
	return dbInstance, nil
}

func (b *BusinessLogic) GetInstanceById(Id string) (*DbInstance, error) {
	return GetInstanceById(b.namePrefix, b.storage, Id)
}

func (b *BusinessLogic) GetUnclaimedInstance(PlanId string, InstanceId string) (*DbInstance, error) {
	dbEntry, err := b.storage.GetUnclaimedInstance(PlanId, InstanceId)
	if err != nil {
		return nil, err
	}
	dbInstance, err := b.GetInstanceById(dbEntry.Id)
	if err != nil {
		if err = b.storage.ReturnClaimedInstance(dbEntry.Id); err != nil {
			return nil, err
		}
		return nil, err
	}
	return dbInstance, nil
}

// A peice of advice, never try to make this syncronous by waiting for a to return a response. The problem is
// that can take up to 10 minutes in my experience (depending on the provider), and aside from the API call timing
// out the other issue is it can cause the mutex lock to make the entire API unresponsive.
// TODO: Clustered instances must be provisioned differently
// TODO: Support the concept of callbacks once a provision has happened?
func (b *BusinessLogic) Provision(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error) {
	b.Lock()
	defer b.Unlock()
	response := broker.ProvisionResponse{}

	if !request.AcceptsIncomplete {
		return nil, UnprocessableEntityWithMessage("AsyncRequired", "The query parameter accepts_incomplete=true MUST be included the request.")
	}
	if request.InstanceID == "" {
		return nil, UnprocessableEntityWithMessage("InstanceRequired", "The instance ID was not provided.")
	}

	plan, err := b.storage.GetPlanByID(request.PlanID)
	if err != nil && err.Error() == "Not found" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to provision (GetPlanByID failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	dbInstance, err := b.GetInstanceById(request.InstanceID)

	if err == nil {
		if dbInstance.Plan.ID != request.PlanID {
			return nil, ConflictErrorWithMessage("InstanceID in use")
		}
		response.Exists = true
	} else if err != nil && err.Error() == "Cannot find database instance" {
		response.Exists = false
		dbInstance, err = b.GetUnclaimedInstance(request.PlanID, request.InstanceID)

		if err != nil && err.Error() == "Cannot find database instance" {
			// Create a new one
			provider, err := GetProviderByPlan(b.namePrefix, plan)
			if err != nil {
				glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
				return nil, InternalServerError()
			}
			dbInstance, err = provider.Provision(request.InstanceID, plan, request.OrganizationGUID)
			if err != nil {
				glog.Errorf("Error provisioning database: %s\n", err.Error())
				return nil, InternalServerError()
			}

			if err = b.storage.AddInstance(dbInstance); err != nil {
				glog.Errorf("Error inserting record into provisioned table: %s\n", err.Error())

				if err = provider.Deprovision(dbInstance, false); err != nil {
					glog.Errorf("Error cleaning up (deprovision failed) after insert record failed but provision succeeded (Database Id:%s Name: %s) %s\n", dbInstance.Id, dbInstance.Name, err.Error())
					if _, err = b.storage.AddTask(dbInstance.Id, DeleteTask, dbInstance.Name); err != nil {
						glog.Errorf("Error: Unable to add task to delete instance, WE HAVE AN ORPHAN! (%s): %s\n", dbInstance.Name, err.Error())
					}
				}
				return nil, InternalServerError()
			}
			if dbInstance.Status != "available" {
				if _, err = b.storage.AddTask(dbInstance.Id, ResyncFromProviderUntilAvailableTask, ""); err != nil {
					glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", dbInstance.Name, err.Error())
				}
				// This is a hack to support callbacks, hopefully this will become an OSB standard.
				if c.Request.URL.Query().Get("webhook") != "" && c.Request.URL.Query().Get("secret") != "" {
					// Schedule a callback
					byteData, err := json.Marshal(WebhookTaskMetadata{Url: c.Request.URL.Query().Get("webhook"), Secret: c.Request.URL.Query().Get("secret")})
					if err != nil {
						glog.Errorf("Error: failed to marshal webhook task metadata: %s\n", err)
					}
					if _, err = b.storage.AddTask(dbInstance.Id, NotifyCreateServiceWebhookTask, string(byteData)); err != nil {
						glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", dbInstance.Name, err.Error())
					}
				}
			}
		} else if err != nil {
			glog.Errorf("Got fatal error from unclaimed instance endpoint: %s\n", err.Error())
			return nil, InternalServerError()
		}
	} else {
		glog.Errorf("Unable to get instances: %s\n", err.Error())
		return nil, InternalServerError()
	}

	if request.AcceptsIncomplete && dbInstance.Ready == false {
		opkey := osb.OperationKey(request.InstanceID)
		response.Async = !dbInstance.Ready
		response.OperationKey = &opkey
	} else if request.AcceptsIncomplete && dbInstance.Ready == true {
		response.Async = false
	}

	response.ExtensionAPIs = b.ConvertActionsToExtensions(dbInstance.Id)

	return &response, nil
}

func (b *BusinessLogic) Deprovision(request *osb.DeprovisionRequest, c *broker.RequestContext) (*broker.DeprovisionResponse, error) {
	b.Lock()
	defer b.Unlock()

	response := broker.DeprovisionResponse{}
	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during deprovision) from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = provider.Deprovision(dbInstance, true); err != nil {
		glog.Errorf("Error failed to deprovision: (Id: %s Name: %s) %s\n", dbInstance.Id, dbInstance.Name, err.Error())
		if _, err = b.storage.AddTask(dbInstance.Id, DeleteTask, dbInstance.Name); err != nil {
			glog.Errorf("Error: Unable to schedule delete from provider! (%s): %s\n", dbInstance.Name, err.Error())
			return nil, InternalServerError()
		} else {
			glog.Errorf("Successfully scheduled db to be removed.")
			response.Async = false
			return &response, nil
		}
	}
	if err = b.storage.DeleteInstance(dbInstance); err != nil {
		glog.Errorf("Error removing record from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}
	response.Async = false
	return &response, nil
}

func (b *BusinessLogic) Update(request *osb.UpdateInstanceRequest, c *broker.RequestContext) (*broker.UpdateInstanceResponse, error) {
	response := broker.UpdateInstanceResponse{}
	if !request.AcceptsIncomplete {
		return nil, UnprocessableEntity()
	}
	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during deprovision) from provisioned table: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if request.PlanID == nil {
		return nil, UnprocessableEntity()
	}

	if dbInstance.Status != "available" {
		return nil, UnprocessableEntityWithMessage("ConcurrencyError", "Clients MUST wait until pending requests have completed for the specified resources.")
	}

	if *request.PlanID == dbInstance.Plan.ID {
		return nil, UnprocessableEntityWithMessage("UpgradeError", "Cannot upgrade to the same plan.")
	}

	target_plan, err := b.storage.GetPlanByID(*request.PlanID)
	if err != nil {
		glog.Errorf("Unable to provision RDS (GetPlanByID failed): %s\n", err.Error())
		return nil, err
	}

	if dbInstance.Plan.Provider != target_plan.Provider {
		return nil, UnprocessableEntityWithMessage("UpgradeError", "Upgrading a database must have the same provider.")
	}

	provider, err := GetProviderByPlan(b.namePrefix, target_plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, UnprocessableEntityWithMessage("UpgradeError", "Invalid Provider")
	}

	newDbInstance, err := provider.Modify(dbInstance, target_plan)
	if err != nil {
		glog.Errorf("Error modifying the plan on database (%s): %s\n", dbInstance.Name, err.Error())
		return nil, InternalServerError()
	}

	if err = b.storage.UpdateInstance(dbInstance, newDbInstance.Plan.ID); err != nil {
		glog.Errorf("Error updating record in provisioned table to change plan (%s): %s\n", dbInstance.Name, err.Error())
		return nil, InternalServerError()
	}
	if newDbInstance.Status != "available" {
		if _, err = b.storage.AddTask(dbInstance.Id, ResyncFromProviderTask, ""); err != nil {
			glog.Errorf("Error: Unable to schedule resync from provider! (%s): %s\n", dbInstance.Name, err.Error())
		}
	}
	if request.AcceptsIncomplete {
		response.Async = true
	}
	return &response, nil
}

func (b *BusinessLogic) LastOperation(request *osb.LastOperationRequest, c *broker.RequestContext) (*broker.LastOperationResponse, error) {
	response := broker.LastOperationResponse{}
	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Unable to get RDS (%s) status: %s\n", request.InstanceID, err.Error())
		return nil, InternalServerError()
	}
	b.storage.UpdateInstance(dbInstance, dbInstance.Plan.ID)
	response.Description = &dbInstance.Status
	if dbInstance.Ready == true {
		response.State = osb.StateSucceeded
	} else if InProgress(dbInstance.Status) {
		response.State = osb.StateInProgress
	} else {
		response.State = osb.StateFailed
	}
	return &response, nil
}

func (b *BusinessLogic) Bind(request *osb.BindRequest, c *broker.RequestContext) (*broker.BindResponse, error) {
	b.Lock()
	defer b.Unlock()
	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, InternalServerError()
	}
	if request.BindResource == nil || request.BindResource.AppGUID == nil {
		return nil, UnprocessableEntityWithMessage("RequiresApp", "The app_guid MUST be included in the request body.")
	}
	if dbInstance.Ready == false {
		return nil, UnprocessableEntity()
	}

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = provider.Tag(dbInstance, "Binding", request.BindingID); err != nil {
		glog.Errorf("Error tagging: %s with %s, got %s\n", request.InstanceID, *request.BindResource.AppGUID, err.Error())
		return nil, InternalServerError()
	}
	if err = provider.Tag(dbInstance, "App", *request.BindResource.AppGUID); err != nil {
		glog.Errorf("Error tagging: %s with %s, got %s\n", request.InstanceID, *request.BindResource.AppGUID, err.Error())
		return nil, InternalServerError()
	}

	dbUrl, err := b.storage.GetReplicas(dbInstance)
	if err != nil && err.Error() == "sql: no rows in result set" {
		response := broker.BindResponse{
			BindResponse: osb.BindResponse{
				Async: false,
				Credentials: map[string]interface{}{
					"DATABASE_URL": dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint,
				},
			},
		}
		return &response, nil
	} else if err == nil {
		response := broker.BindResponse{
			BindResponse: osb.BindResponse{
				Async: false,
				Credentials: map[string]interface{}{
					"DATABASE_URL":          dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint,
					"DATABASE_READONLY_URL": dbInstance.Scheme + "://" + dbUrl.Username + ":" + dbUrl.Password + "@" + dbUrl.Endpoint,
				},
			},
		}
		return &response, nil
	} else {
		glog.Errorf("Error: Get binding, replica table returned error: %s\n", err.Error())
		return nil, InternalServerError()
	}
}

func (b *BusinessLogic) Unbind(request *osb.UnbindRequest, c *broker.RequestContext) (*broker.UnbindResponse, error) {
	b.Lock()
	defer b.Unlock()

	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, InternalServerError()
	}
	if dbInstance.Ready == false {
		return nil, UnprocessableEntity()
	}

	provider, err := GetProviderByPlan(b.namePrefix, dbInstance.Plan)
	if err != nil {
		glog.Errorf("Unable to provision, cannot find provider (GetProviderByPlan failed): %s\n", err.Error())
		return nil, InternalServerError()
	}

	if err = provider.Untag(dbInstance, "Binding"); err != nil {
		glog.Errorf("Error untagging: %s\n", err.Error())
		return nil, InternalServerError()
	}
	if err = provider.Untag(dbInstance, "App"); err != nil {
		glog.Errorf("Error untagging: got %s\n", err.Error())
		return nil, InternalServerError()
	}

	return &broker.UnbindResponse{
		UnbindResponse: osb.UnbindResponse{
			Async: false,
		},
	}, nil
}

func (b *BusinessLogic) ValidateBrokerAPIVersion(version string) error {
	return nil
}

func (b *BusinessLogic) GetBinding(request *osb.GetBindingRequest, context *broker.RequestContext) (*osb.GetBindingResponse, error) {
	dbInstance, err := b.GetInstanceById(request.InstanceID)
	if err != nil && err.Error() == "Cannot find database instance" {
		return nil, NotFound()
	} else if err != nil {
		glog.Errorf("Error finding instance id (during getbinding): %s\n", err.Error())
		return nil, err
	}

	dbUrl, err := b.storage.GetReplicas(dbInstance)
	if err != nil && err.Error() == "sql: no rows in result set" {
		response := osb.GetBindingResponse{
			Credentials: map[string]interface{}{
				"DATABASE_URL": dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint,
			},
		}
		return &response, nil
	} else if err == nil {
		response := osb.GetBindingResponse{
			Credentials: map[string]interface{}{
				"DATABASE_URL":          dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint,
				"DATABASE_READONLY_URL": dbInstance.Scheme + "://" + dbUrl.Username + ":" + dbUrl.Password + "@" + dbUrl.Endpoint,
			},
		}
		return &response, nil
	}
	glog.Errorf("Error getting replicas during get binding: %s\n", err.Error())
	return nil, err
}

var _ broker.Interface = &BusinessLogic{}

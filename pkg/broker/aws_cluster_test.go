package broker

import (
	"context"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"os"
	"testing"
	_ "github.com/lib/pq"
	"fmt"
	"time"
)

func TestAwsClusterProvision(t *testing.T) {
	if os.Getenv("TEST_AWS_CLUSTER") == "" {
		return
	}
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	var instanceId string = RandomString(12)
	var err error
	Convey("Given a fresh provisioner.", t, func() {

		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)

		Convey("Ensure we can get the catalog and target plan exists", func() {
			rc := broker.RequestContext{}
			catalog, err = logic.GetCatalog(&rc)
			So(err, ShouldBeNil)
			So(catalog, ShouldNotBeNil)
			So(len(catalog.Services), ShouldEqual, 2)
			plan = catalog.Services[1].Plans[0]
			So(plan.Name, ShouldEqual, "premium-0")
			So(plan.ID, ShouldEqual, "bb660450-61d3-1c13-a3fd-d37999793222")
		})

		Convey("Ensure provisioner for aws clusters works", func() {
			var request osb.ProvisionRequest
			var c broker.RequestContext
			request.AcceptsIncomplete = false
			res, err := logic.Provision(&request, &c)
			So(res, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Status: 422; ErrorMessage: <nil>; Description: The query parameter accepts_incomplete=true MUST be included the request.; ResponseError: AsyncRequired")

			request.AcceptsIncomplete = true
			request.InstanceID = instanceId
			request.PlanID = plan.ID
			res, err = logic.Provision(&request, &c)

			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)

			var dbInstance *DbInstance = nil
			t := time.NewTicker(time.Second * 30)
			for i := 0; i < 30; i++ {
				dbInstance, err = logic.GetInstanceById(instanceId)
				So(err, ShouldBeNil)
				fmt.Printf(".")
				if dbInstance.Ready == true && dbInstance.Status == "available" {
					break;
				}
				<-t.C
			}
			So(dbInstance, ShouldNotBeNil)
			So(dbInstance.Ready, ShouldEqual, true)
		})

		Convey("Get and create service bindings", func() {
			var request osb.LastOperationRequest = osb.LastOperationRequest{InstanceID: instanceId}
			var c broker.RequestContext
			res, err := logic.LastOperation(&request, &c)
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
			So(res.State, ShouldEqual, osb.StateSucceeded)

			var guid = "123e4567-e89b-12d3-a456-426655440000"
			var resource osb.BindResource = osb.BindResource{AppGUID: &guid}
			var brequest osb.BindRequest = osb.BindRequest{InstanceID: instanceId, BindingID: "foo", BindResource: &resource}
			dres, err := logic.Bind(&brequest, &c)
			So(err, ShouldBeNil)
			So(dres, ShouldNotBeNil)
			So(dres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "mysql://")

			var gbrequest osb.GetBindingRequest = osb.GetBindingRequest{InstanceID: instanceId, BindingID: "foo"}
			gbres, err := logic.GetBinding(&gbrequest, &c)
			So(err, ShouldBeNil)
			So(gbres, ShouldNotBeNil)
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "mysql://")
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, dres.Credentials["DATABASE_URL"].(string))

		})

		Convey("Ensure logging works for instance", func() {
			var c broker.RequestContext
			logsres, err := logic.ActionListLogs(instanceId, map[string]string{}, &c)
			So(err, ShouldBeNil)
			So(logsres, ShouldNotBeNil)
			logs := logsres.([]DatabaseLogs)
			So(len(logs), ShouldBeGreaterThan, 0)
			So(logs[0].Name, ShouldNotBeNil)

			logpath := strings.Split(*logs[0].Name, "/")
			logdataresp, err := logic.ActionGetLogs(instanceId, map[string]string{"dir":logpath[0], "file":logpath[1]}, &c)
			So(err, ShouldBeNil)
			So(logdataresp, ShouldNotBeNil)

			logdata := logdataresp.(string)
			So(logdata, ShouldNotEqual, "")

		})

		Convey("Ensure restarting aws cluster works", func() {			
			var c broker.RequestContext
			_, err = logic.ActionRestart(instanceId, map[string]string{}, &c)
			So(err, ShouldBeNil)
			dbInstance, err := logic.GetInstanceById(instanceId)
			So(err, ShouldBeNil)
			So(InProgress(dbInstance.Status), ShouldEqual, true)

			t := time.NewTicker(time.Second * 30)
			for i := 0; i < 30; i++ {
				dbInstance, err = logic.GetInstanceById(instanceId)
				fmt.Printf(".")
				if dbInstance.Ready == true && dbInstance.Status == "available" {
					break;
				}
				<-t.C
			}
			So(dbInstance, ShouldNotBeNil)
			So(dbInstance.Ready, ShouldEqual, true)
		})


		Convey("Ensure backup and restores work", func() {
			var c broker.RequestContext
			var dbInstance *DbInstance = nil
			
			backupsresp, err := logic.ActionListBackups(instanceId, map[string]string{}, &c)
			So(err, ShouldBeNil)

			backups := backupsresp.([]DatabaseBackupSpec)
			So(len(backups), ShouldEqual, 0)

			backupresp, err := logic.ActionCreateBackup(instanceId, map[string]string{}, &c)
			So(err, ShouldBeNil)

			backup := backupresp.(DatabaseBackupSpec)

			gbackupresp, err := logic.ActionGetBackup(instanceId, map[string]string{"backup":*backup.Id}, &c)
			So(err, ShouldBeNil)
			gbackup := gbackupresp.(DatabaseBackupSpec)
			So(*backup.Id, ShouldEqual, *gbackup.Id)

			t := time.NewTicker(time.Second * 30)
			for i := 0; i < 30; i++ {
				gbackupresp, err = logic.ActionGetBackup(instanceId, map[string]string{"backup":*backup.Id}, &c)
				So(err, ShouldBeNil)
				gbackup = gbackupresp.(DatabaseBackupSpec)
				fmt.Printf(".")
				if gbackup.Status != nil && *gbackup.Status == "available" {
					break;
				}
				<-t.C
			}
			
			_, err = logic.ActionRestoreBackup(instanceId, map[string]string{"backup":*backup.Id}, &c)
			So(err, ShouldBeNil)

			for i := 0; i < 30; i++ {
				dbInstance, err = logic.GetInstanceById(instanceId)
				So(err, ShouldBeNil)
				fmt.Printf(".")
				if dbInstance.Ready == true && dbInstance.Status == "available" {
					break;
				}
				<-t.C
			}
			So(dbInstance, ShouldNotBeNil)

		})

		Convey("Ensure unbind for aws cluster works", func() {
			var c broker.RequestContext
			var urequest osb.UnbindRequest = osb.UnbindRequest{InstanceID: instanceId, BindingID: "foo"}
			ures, err := logic.Unbind(&urequest, &c)
			So(err, ShouldBeNil)
			So(ures, ShouldNotBeNil)
		})

		Convey("Ensure deprovisioner for aws cluster works", func() {
			var request osb.LastOperationRequest = osb.LastOperationRequest{InstanceID: instanceId}
			var c broker.RequestContext
			res, err := logic.LastOperation(&request, &c)
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
			So(res.State, ShouldEqual, osb.StateSucceeded)

			var drequest osb.DeprovisionRequest = osb.DeprovisionRequest{InstanceID: instanceId}
			dres, err := logic.Deprovision(&drequest, &c)

			So(err, ShouldBeNil)
			So(dres, ShouldNotBeNil)

		})
	})
}

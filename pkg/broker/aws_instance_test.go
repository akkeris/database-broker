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
	"database/sql"
	"fmt"
	"time"
	"net/url"
)

func TestAwsProvision(t *testing.T) {
	if os.Getenv("TEST_AWS_INSTANCE") == "" {
		return
	}
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	var dbUrl string
	var instanceId string = RandomString(12)
	var err error
	var namePrefix = "test"
	Convey("Given a fresh provisioner.", t, func() {

		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: namePrefix})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)

		Convey("Ensure we can get the catalog and target plan exists", func() {
			rc := broker.RequestContext{}
			catalog, err = logic.GetCatalog(&rc)
			So(err, ShouldBeNil)
			So(catalog, ShouldNotBeNil)
			So(len(catalog.Services), ShouldEqual, 2)
			//service = catalog.Services[0]
			//plan = catalog.Services[0].Plans[2]
			var foundPremium = false
			for _, p := range catalog.Services[0].Plans {
				if p.Name == "premium-0" {
					plan = p
					foundPremium = true
				}
			}

			So(foundPremium, ShouldEqual, true)
		})

		Convey("Ensure provisioner for aws instances works", func() {
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

			task, err := logic.storage.PopPendingTask()
			So(err, ShouldBeNil)
			So(task.Action, ShouldEqual, PerformPostProvisionTask)
			FinishedTask(logic.storage, task.Id, task.Retries, "", "finished")

			var dbInstance *DbInstance = nil
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
			So(dres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "postgres://")

			var gbrequest osb.GetBindingRequest = osb.GetBindingRequest{InstanceID: instanceId, BindingID: "foo"}
			gbres, err := logic.GetBinding(&gbrequest, &c)
			So(err, ShouldBeNil)
			So(gbres, ShouldNotBeNil)
			dbUrl = gbres.Credentials["DATABASE_URL"].(string)
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "postgres://")
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, dres.Credentials["DATABASE_URL"].(string))

		})

		Convey("Ensure logging works for instance", func() {
			var c broker.RequestContext
			logsres, err := logic.ActionListLogs(instanceId, map[string]string{}, &c)
			logs := logsres.([]DatabaseLogs)
			So(err, ShouldBeNil)
			So(logs, ShouldNotBeNil)
			So(len(logs), ShouldBeGreaterThan, 0)
			So(logs[0].Name, ShouldNotBeNil)

			logpath := strings.Split(*logs[0].Name, "/")
			logdataresp, err := logic.ActionGetLogs(instanceId, map[string]string{"dir":logpath[0], "file":logpath[1]}, &c)
			So(err, ShouldBeNil)
			So(logdataresp, ShouldNotBeNil)

			logdata := logdataresp.(string)
			So(logdata, ShouldNotEqual, "")

		})

		Convey("Ensure restarting aws instance works", func() {			
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

		Convey("Ensure creation of roles, rotating roles and removing roles successfully works.", func() {
			So(dbUrl, ShouldNotEqual, "")
			
			var c broker.RequestContext
			resp, err := logic.ActionCreateRole(instanceId, map[string]string{}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			dbReadOnlySpec := resp.(DatabaseUrlSpec)
			dbFullUrl, err := url.Parse(dbUrl)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			So(dbFullUrl.User.Username(), ShouldNotEqual, dbReadOnlySpec.Username)
			So(dbFullUrl.Host + dbFullUrl.Path, ShouldEqual, dbReadOnlySpec.Endpoint)

			resps, err := logic.ActionListRoles(instanceId, map[string]string{}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			roles := resps.([]DatabaseUrlSpec)
			So(len(roles), ShouldEqual, 1)
			So(roles[0].Username, ShouldEqual, dbReadOnlySpec.Username)

			resprole, err := logic.ActionGetRole(instanceId, map[string]string{"role":dbReadOnlySpec.Username}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			getrole := resprole.(DatabaseUrlSpec)
			So(getrole.Username, ShouldEqual, dbReadOnlySpec.Username)

			rotroleresp, err := logic.ActionRotateRole(instanceId, map[string]string{"role":dbReadOnlySpec.Username}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			rotrole := rotroleresp.(DatabaseUrlSpec)
			So(err, ShouldBeNil)
			So(rotrole.Username, ShouldEqual, dbReadOnlySpec.Username)
			So(rotrole.Password, ShouldNotEqual, dbReadOnlySpec.Password)

			_, err = logic.ActionDeleteRole(instanceId, map[string]string{"role":dbReadOnlySpec.Username}, &c)
			So(err, ShouldBeNil)
		})

		// This should be near the end of the tests always,
		// it causes a DNS change on the database for the IP so 
		// there may be connect failures if you do the role tests
		// after.
		Convey("Ensure backup and restores work", func() {
			var c broker.RequestContext
			var dbInstance *DbInstance = nil


			So(dbUrl, ShouldNotEqual, "")

			var randStr = RandomString(15)
			db, err := sql.Open("postgres", dbUrl)
			So(err, ShouldBeNil)
			defer db.Close()
			_, err = db.Exec("create table testing (foo text);")
			So(err, ShouldBeNil)
			_, err = db.Exec("insert into testing (foo) values ('" + randStr + "');")
			So(err, ShouldBeNil)
			
			backupsresp, err := logic.ActionListBackups(instanceId, map[string]string{}, &c)
			So(err, ShouldBeNil)

			backups := backupsresp.([]DatabaseBackupSpec)
			So(len(backups), ShouldBeGreaterThan, 0)

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
				if gbackupresp != nil {
					gbackup = gbackupresp.(DatabaseBackupSpec)
					if gbackup.Status != nil && *gbackup.Status == "available" {
						break;
					}
				}
				fmt.Printf(".")
				<-t.C
			}

			_, err = db.Exec("delete from testing")
			So(err, ShouldBeNil)
			_, err = db.Exec("insert into testing (foo) values ('invalid');")
			So(err, ShouldBeNil)
			
			_, err = logic.ActionRestoreBackup(instanceId, map[string]string{"backup":*backup.Id}, &c)
			So(err, ShouldBeNil)

			dbInstance, err = logic.GetInstanceById(instanceId)
			So(err, ShouldBeNil)
			task, err := logic.storage.PopPendingTask()
			So(err, ShouldBeNil)
			So(task.Action, ShouldEqual, RestoreDbTask)
			RestoreBackup(logic.storage, dbInstance, namePrefix, *backup.Id)
			FinishedTask(logic.storage, task.Id, task.Retries, "", "finished")

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

			db2, err := sql.Open("postgres", dbUrl)
			defer db2.Close()
			var randOut string
			err = db2.QueryRow("select foo from testing").Scan(&randOut)
			So(err, ShouldBeNil)
			So(randStr, ShouldEqual, randOut)

		})

		Convey("Ensure unbind for aws instance works", func() {
			var c broker.RequestContext
			var urequest osb.UnbindRequest = osb.UnbindRequest{InstanceID: instanceId, BindingID: "foo"}
			ures, err := logic.Unbind(&urequest, &c)
			So(err, ShouldBeNil)
			So(ures, ShouldNotBeNil)
		})

		Convey("Ensure deprovisioner for aws instance works", func() {

			t := time.NewTicker(time.Second * 30)
			for i := 0; i < 30; i++ {
				dbInstance, err := logic.GetInstanceById(instanceId)
				fmt.Printf(".")
				if err == nil && dbInstance.Ready == true && dbInstance.Status == "available" {
					break;
				}
				<-t.C
			}
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

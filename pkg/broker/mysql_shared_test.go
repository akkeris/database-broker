package broker

import (
	"context"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"database/sql"
	"fmt"
	"net/url"
)

func TestMysqlProvision(t *testing.T) {
	if os.Getenv("TEST_SHARED_MYSQL") == "" {
		return
	}
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	var dbUrl string
	var instanceId string = RandomString(12)
	Convey("Given a fresh provisioner.", t, func() {
		So(os.Getenv("DATABASE_URL"), ShouldNotEqual, "")
		So(os.Getenv("MYSQL_URL"), ShouldNotEqual, "")
		var err error
		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)
		testplansdb, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
		So(err, ShouldBeNil)
		Convey("Add test data", func() {
			_, err := testplansdb.Exec("insert into services (service, name, human_name, description, categories, image, beta, deprecated) values ('11bb60d2-f2bb-64c0-4c8b-111222aabbcc','test-shared-mysql', 'Test Shared MySQL', 'Dedicated and scalable MySQL (aurora) relational SQL database.',   'Data Stores,mysql',    'https://upload.wikimedia.org/wikipedia/en/thumb/6/62/MySQL.svg/1280px-MySQL.svg.png', false, false) ")
			So(err, ShouldBeNil)
			_, err = testplansdb.Exec(`insert into plans (plan, service, name, human_name, description, version, type, scheme, categories, cost_cents, preprovision, attributes, provider, provider_private_details) values ('aaaaaaaa-bbcc-dd13-a3fd-d379997932fa', '11bb60d2-f2bb-64c0-4c8b-111222aabbcc', 'shared',  'Shared (10.4)', 'Mysql 5.7 - 1xCPU 1GB Ram 512MB Storage', '5.7', 'mysql', 'mysql', 'Data Stores', 0, 1, '{"compliance":"", "supports_extensions":false, "ram":"1GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"512MB", "data_clips":false, "connection_limit":20,   "high_availability":false,  "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'mysql-shared', '{"master_uri":"${MYSQL_URL}", "engine":"mysql", "engine_version":"5.7", "scheme_type":"dsn"}')`)
			So(err, ShouldBeNil)
		})

		Convey("Ensure preprovisioner and storage object on mysql target works", func() {


			storage, err := InitStorage(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
			So(err, ShouldBeNil)
			RunPreprovisionTasks(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"}, "test", storage, 1)

			storage.WarnOnUnfinishedTasks()
			task, err := storage.PopPendingTask()
			So(task, ShouldBeNil)
			So(err, ShouldNotBeNil)

			entry, err := storage.GetUnclaimedInstance("aaaaaaaa-bbcc-dd13-a3fd-d379997932fa", "my-new-test-instance")
			So(err, ShouldBeNil)

			So(entry.Id, ShouldEqual, "my-new-test-instance")
			So(entry.PlanId, ShouldEqual, "aaaaaaaa-bbcc-dd13-a3fd-d379997932fa")
			So(entry.Claimed, ShouldEqual, true)
			So(entry.Status, ShouldEqual, "available")

			err = storage.ReturnClaimedInstance("my-new-test-instance")
			So(err, ShouldBeNil)
		})

		Convey("Ensure we can get the catalog and target plan exists", func() {
			rc := broker.RequestContext{}
			catalog, err = logic.GetCatalog(&rc)
			So(err, ShouldBeNil)
			So(catalog, ShouldNotBeNil)
			So(len(catalog.Services), ShouldEqual, 3)

			var foundHobby = false
			var service osb.Service
			for _, s := range catalog.Services {
				if s.ID == "11bb60d2-f2bb-64c0-4c8b-111222aabbcc" {
					service = s
				}
			}
			So(service, ShouldNotBeNil)
			for _, p := range service.Plans {
				if p.Name == "shared" && p.ID == "aaaaaaaa-bbcc-dd13-a3fd-d379997932fa" {
					plan = p
					foundHobby = true
				}
			}
			So(foundHobby, ShouldEqual, true)
		})

		Convey("Ensure provisioner for shared mysql can provision a database", func() {
			var request osb.ProvisionRequest
			var c broker.RequestContext
			request.AcceptsIncomplete = false
			res, err := logic.Provision(&request, &c)
			So(res, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "Status: 422; ErrorMessage: <nil>; Description: The query parameter accepts_incomplete=true MUST be included the request.; ResponseError: AsyncRequired")

			request.AcceptsIncomplete = true
			request.PlanID = "does not exist"
			request.InstanceID = "asfdasdf"
			res, err = logic.Provision(&request, &c)
			So(err.Error(), ShouldEqual, "Status: 404; ErrorMessage: <nil>; Description: Not Found; ResponseError: <nil>")

			request.InstanceID = instanceId
			request.PlanID = plan.ID
			res, err = logic.Provision(&request, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			So(res, ShouldNotBeNil)
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

			dbUrl = dres.Credentials["DATABASE_URL"].(string)

			var gbrequest osb.GetBindingRequest = osb.GetBindingRequest{InstanceID: instanceId, BindingID: "foo"}
			gbres, err := logic.GetBinding(&gbrequest, &c)
			So(err, ShouldBeNil)
			So(gbres, ShouldNotBeNil)
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, dres.Credentials["DATABASE_URL"].(string))

		})

		Convey("Ensure creation of roles, rotating roles and removing roles successfully works.", func() {
			db, err := sql.Open("mysql", dbUrl)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE mytable (somefield text)")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			_, err = db.Exec("insert into mytable (somefield) values ('fooo')")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
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

		Convey("Ensure unbind for shared mysql works", func() {
			var c broker.RequestContext
			var urequest osb.UnbindRequest = osb.UnbindRequest{InstanceID: instanceId, BindingID: "foo"}
			ures, err := logic.Unbind(&urequest, &c)
			So(err, ShouldBeNil)
			So(ures, ShouldNotBeNil)
		})

		Convey("Ensure deprovisioner for shared mysql works", func() {
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

		Convey("Ensure mysql can be deprovisioned when a user (readonly and service account) are connected", func() {
			// create database
			c := broker.RequestContext{}
			var request osb.ProvisionRequest
			instanceId = RandomString(12)
			request.AcceptsIncomplete = true
			request.InstanceID = instanceId
			request.PlanID = plan.ID
			_, err := logic.Provision(&request, &c)
			var guid = "123e4567-e89b-12d3-a456-426655440000"
			var resource osb.BindResource = osb.BindResource{AppGUID: &guid}
			var brequest osb.BindRequest = osb.BindRequest{InstanceID: instanceId, BindingID: "foo", BindResource: &resource}
			dres, err := logic.Bind(&brequest, &c)
			So(err, ShouldBeNil)
			So(dres, ShouldNotBeNil)
			dbServiceUrl := dres.Credentials["DATABASE_URL"].(string)

			// create read only user
			resp, err := logic.ActionCreateRole(instanceId, map[string]string{}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			dbReadOnlySpec := resp.(DatabaseUrlSpec)
			var dbReadonlyUrl = dbReadOnlySpec.Username + ":" + dbReadOnlySpec.Password + "@" + dbReadOnlySpec.Endpoint

			// create a connection via both service and read only rolls
			dbServiceConn, err := sql.Open("mysql", dbServiceUrl)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			defer dbServiceConn.Close()
			_, err = dbServiceConn.Exec("CREATE TABLE mytable (somefield text)")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			var random = RandomString(55)
			_, err = dbServiceConn.Exec("insert into mytable (somefield) values ('" + random + "')")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			dbReadonlyConn, err := sql.Open("mysql", dbReadonlyUrl)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			defer dbReadonlyConn.Close()
			var readRandom string
			err = dbReadonlyConn.QueryRow("select somefield from mytable").Scan(&readRandom)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			So(random, ShouldEqual, readRandom)

			// Deprovision the instance
			var drequest osb.DeprovisionRequest = osb.DeprovisionRequest{InstanceID: instanceId}
			dres2, err := logic.Deprovision(&drequest, &c)
			So(err, ShouldBeNil)
			So(dres2, ShouldNotBeNil)

			// ensure the existing connections have been closed and return an error. 
			err = dbServiceConn.QueryRow("select somefield from mytable").Scan(&readRandom)
			So(err, ShouldNotBeNil)
			err = dbReadonlyConn.QueryRow("select somefield from mytable").Scan(&readRandom)
			So(err, ShouldNotBeNil)

			// Ensure we can no longer connect with the read only account or service account.
			dbServiceConn2, err := sql.Open("mysql", dbServiceUrl)
			if err == nil {
				defer dbServiceConn2.Close()
				err = dbServiceConn2.Ping()
			}
			So(err, ShouldNotBeNil)
			dbReadonlyConn2, err := sql.Open("mysql", dbReadonlyUrl)
			if err == nil {
				dbReadonlyConn2.Close()
				err = dbReadonlyConn2.Ping()
			}
			So(err, ShouldNotBeNil)
		})
		Convey("Remove test data", func() {
			_, err := testplansdb.Exec("delete from roles using databases, plans, services where roles.database = databases.id and databases.plan = plans.plan and services.service = plans.service and services.service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			_, err = testplansdb.Exec("delete from replicas using databases, plans, services where replicas.database = databases.id and databases.plan = plans.plan and services.service = plans.service and services.service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			_, err = testplansdb.Exec("delete from tasks using databases, plans, services where tasks.database = databases.id and databases.plan = plans.plan and services.service = plans.service and services.service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			_, err = testplansdb.Exec("delete from databases using services, plans where databases.plan = plans.plan and services.service = plans.service and services.service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			_, err = testplansdb.Exec("delete from plans using services where services.service = plans.service and services.service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			_, err = testplansdb.Exec("delete from services where service='11bb60d2-f2bb-64c0-4c8b-111222aabbcc'")
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
		})

	})
}

package broker

import (
	"context"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	_ "github.com/lib/pq"
	"database/sql"
	"fmt"
	"net/url"
)

func TestProvision(t *testing.T) {
	if os.Getenv("TEST_SHARED_POSTGRES") == "" {
		return
	}
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	var dbUrl string
	var instanceId string = RandomString(12)
	var err error
	Convey("Given a fresh provisioner.", t, func() {
		os.Setenv("PG_HOBBY_9_URI", os.Getenv("DATABASE_URL"))
		os.Setenv("PG_HOBBY_10_URI", os.Getenv("DATABASE_URL"))
		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)

		Convey("Ensure preprovisioner and storage object on postgres target works", func() {
			storage, err := InitStorage(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
			So(err, ShouldBeNil)
			RunPreprovisionTasks(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"}, "test", storage, 1)

			storage.WarnOnUnfinishedTasks()
			task, err := storage.PopPendingTask()
			So(task, ShouldBeNil)
			So(err, ShouldNotBeNil)

			entry, err := storage.GetUnclaimedInstance("50660450-61d3-2c13-a3fd-d379997932fa", "my-new-test-instance")
			So(err, ShouldBeNil)

			So(entry.Id, ShouldEqual, "my-new-test-instance")
			So(entry.PlanId, ShouldEqual, "50660450-61d3-2c13-a3fd-d379997932fa")
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
			So(len(catalog.Services), ShouldEqual, 2)
			//service = catalog.Services[0]

			var foundHobby = false
			for _, p := range catalog.Services[0].Plans {
				if p.Name == "hobby-v9" {
					plan = p
					foundHobby = true
				}
			}
			So(foundHobby, ShouldEqual, true)
		})

		Convey("Ensure provisioner for shared postrges can provision a database", func() {
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
			So(dres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "postgres://")

			dbUrl = dres.Credentials["DATABASE_URL"].(string)

			var gbrequest osb.GetBindingRequest = osb.GetBindingRequest{InstanceID: instanceId, BindingID: "foo"}
			gbres, err := logic.GetBinding(&gbrequest, &c)
			So(err, ShouldBeNil)
			So(gbres, ShouldNotBeNil)
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "postgres://")
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, dres.Credentials["DATABASE_URL"].(string))

		})

		Convey("Ensure creation of roles, rotating roles and removing roles successfully works.", func() {
			db, err := sql.Open("postgres", dbUrl + "?sslmode=disable")
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
			// TODO: ensure you cant login
		})

		Convey("Ensure unbind for shared postgres works", func() {
			var c broker.RequestContext
			var urequest osb.UnbindRequest = osb.UnbindRequest{InstanceID: instanceId, BindingID: "foo"}
			ures, err := logic.Unbind(&urequest, &c)
			So(err, ShouldBeNil)
			So(ures, ShouldNotBeNil)
		})

		Convey("Ensure deprovisioner for shared postgres works", func() {
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

		Convey("Ensure postgres can be deprovisioned when a user (readonly and service account) are connected", func() {
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
			So(dbServiceUrl, ShouldStartWith, "postgres://")

			// create read only user
			resp, err := logic.ActionCreateRole(instanceId, map[string]string{}, &c)
			if err != nil {
				fmt.Println(err.Error())
			}
			So(err, ShouldBeNil)
			dbReadOnlySpec := resp.(DatabaseUrlSpec)
			var dbReadonlyUrl = "postgres://" + dbReadOnlySpec.Username + ":" + dbReadOnlySpec.Password + "@" + dbReadOnlySpec.Endpoint

			// create a connection via both service and read only rolls
			dbServiceConn, err := sql.Open("postgres", dbServiceUrl + "?sslmode=disable")
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

			dbReadonlyConn, err := sql.Open("postgres", dbReadonlyUrl + "?sslmode=disable")
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
			dbServiceConn2, err := sql.Open("postgres", dbServiceUrl + "?sslmode=disable")
			if err == nil {
				defer dbServiceConn2.Close()
				err = dbServiceConn2.Ping()
			}
			So(err, ShouldNotBeNil)
			dbReadonlyConn2, err := sql.Open("postgres", dbReadonlyUrl + "?sslmode=disable")
			if err == nil {
				dbReadonlyConn2.Close()
				err = dbReadonlyConn2.Ping()
			}
			So(err, ShouldNotBeNil)
		})
	})
}

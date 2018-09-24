package broker

import (
	"context"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestProvision(t *testing.T) {
	var logic *BusinessLogic
	var catalog *broker.CatalogResponse
	var plan osb.Plan
	//var service osb.Service
	var instanceId string = RandomString(12)
	var err error
	Convey("Given a fresh provisioner.", t, func() {

		os.Setenv("PG_HOBBY_9_URI", os.Getenv("DATABASE_URL"))
		os.Setenv("PG_HOBBY_10_URI", os.Getenv("DATABASE_URL"))
		logic, err = NewBusinessLogic(context.TODO(), Options{DatabaseUrl: os.Getenv("DATABASE_URL"), NamePrefix: "test"})
		So(err, ShouldBeNil)
		So(logic, ShouldNotBeNil)

		Convey("Ensure we can get the catalog and target plan exists", func() {
			rc := broker.RequestContext{}
			catalog, err = logic.GetCatalog(&rc)
			So(err, ShouldBeNil)
			So(catalog, ShouldNotBeNil)
			So(len(catalog.Services), ShouldEqual, 1)
			//service = catalog.Services[0]
			plan = catalog.Services[0].Plans[0]
			So(plan.Name, ShouldEqual, "hobby-v9")
		})

		Convey("Ensure provisioner for shared postrges works", func() {
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

		Convey("Get and create sevice bindings", func() {
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
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, "postgres://")
			So(gbres.Credentials["DATABASE_URL"].(string), ShouldStartWith, dres.Credentials["DATABASE_URL"].(string))

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
	})
}

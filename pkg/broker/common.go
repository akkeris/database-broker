package broker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"strings"
	"strconv"
	"text/template"
	"time"
)

var randomSource = rand.NewSource(time.Now().UnixNano())

func ApplyParamsToStatement(statement string, args ...string) string {
	for i, arg := range args {
		idx := strconv.Itoa(i + 1)
		statement = strings.Replace(statement, "$" + idx, arg, -1)
	}
	return statement
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandomString(n int) string {
	b := make([]byte, n)

	// A randomSource.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, randomSource.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = randomSource.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func truePtr() *bool {
	b := true
	return &b
}

func falsePtr() *bool {
	b := false
	return &b
}

func HttpWrite(w http.ResponseWriter, status int, obj interface{}) {
	data, err := json.Marshal(obj)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(data)
}

func InternalServerError() error {
	description := "Internal Server Error"
	return osb.HTTPStatusCodeError{
		StatusCode:  http.StatusInternalServerError,
		Description: &description,
	}
}

func ConflictErrorWithMessage(description string) error {
	return osb.HTTPStatusCodeError{
		StatusCode:  http.StatusConflict,
		Description: &description,
	}
}

func UnprocessableEntityWithMessage(err string, description string) error {
	return osb.HTTPStatusCodeError{
		ResponseError: errors.New(err),
		StatusCode:    http.StatusUnprocessableEntity,
		Description:   &description,
	}
}

func UnprocessableEntity() error {
	description := "Unprocessable Entity"
	return osb.HTTPStatusCodeError{
		StatusCode:  http.StatusUnprocessableEntity,
		Description: &description,
	}
}

func NotFound() error {
	description := "Not Found"
	return osb.HTTPStatusCodeError{
		StatusCode:  http.StatusNotFound,
		Description: &description,
	}
}

type Action struct {
	name    string
	path    string
	method  string
	handler func(string, map[string]string, *broker.RequestContext) (interface{}, error)
}

type ActionBase struct {
	actions []Action
	sync.RWMutex
}

func InitFromOptions(ctx context.Context, o Options) (Storage, string, error) {
	if o.NamePrefix == "" && os.Getenv("NAME_PREFIX") != "" {
		o.NamePrefix = os.Getenv("NAME_PREFIX")
	}
	if o.NamePrefix == "" {
		return nil, "", errors.New("The name prefix was not specified, set NAME_PREFIX in your environment or provide it via the cli using -name-prefix")
	}
	storage, err := InitStorage(ctx, o)
	return storage, o.NamePrefix, err
}

func (b *ActionBase) ActionSchemaHandler(w http.ResponseWriter, r *http.Request) {
	v := mux.Vars(r)
	instance_id := v["instance_id"]
	var baseUrl = "/v2/service_instances/" + instance_id + "/actions"

	action_name := v["action_name"]
	var found = false
	for _, action := range b.actions {
		if action.name == action_name {
			found = true
			t := template.Must(template.New("openapi3").Parse(`
				{
				  "openapi" : "3.0.0",
				  "servers" : [ {
				    "description" : "Extensions",
				    "url" : "{{.BaseUrl}}/{{.Name}}/schema"
				  }, {
				    "description" : "{{.Name}}",
				    "url" : "{{.BaseUrl}}/{{.Path}}"
				  } ],
				  "info" : {
				    "description" : "{{.Name}} action",
				    "version" : "1.0.0",
				    "title" : "{{.Name}}",
				    "license" : {
				      "name" : "Apache 2.0",
				      "url" : "http://www.apache.org/licenses/LICENSE-2.0.html"
				    }
				  },
				  "paths" : {
				    "{{.BaseUrl}}/{{.Path}}" : {
				      "{{.Method}}" : {
				        "tags" : [ "{{.Name}}" ],
				        "summary" : "{{.Name}}",
				        "operationId" : "{{.Name}}",
				        "description" : "{{.Name}}",
				        "responses" : {
				          "200" : {
				            "description" : "OK"
				          },
				          "400" : {
				            "description" : "invalid input, object invalid"
				          },
				        }
				      }
				    }
				  }
				}
				`))
			var b bytes.Buffer
			wr := bufio.NewWriter(&b)
			err := t.Execute(wr, struct {
				BaseUrl string
				Name    string
				Path    string
				Method  string
			}{BaseUrl: baseUrl, Name: action.name, Path: action.path, Method: action.method})
			if err != nil {
				glog.Errorf("Cannot generate swagger doc: %s\n", err.Error())
				w.WriteHeader(500)
				w.Write([]byte("Cannot generate swagger doc"))
				return
			}
			wr.Flush()
			w.Header().Add("content-type", "application/json")
			w.WriteHeader(200)
			w.Write(b.Bytes())
		}
	}
	if found == false {
		w.WriteHeader(404)
		w.Write([]byte("Not Found"))
		return
	}
}

func (b *ActionBase) RouteActions(router *mux.Router) error {
	for _, action := range b.actions {
		glog.Infof("Adding route %s /v2/service_instances/{instance_id}/actions/%s\n", action.method, action.path)
		var act Action = action
		router.HandleFunc("/v2/service_instances/{instance_id}/actions/" + action.path, func(w http.ResponseWriter, r *http.Request) {
			vars := mux.Vars(r)
			c := broker.RequestContext{Request: r, Writer: w}
			obj, herr := act.handler(vars["instance_id"], vars, &c)
			if herr != nil {
				type e struct {
					ErrorMessage *string `json:"error,omitempty"`
					Description  *string `json:"description,omitempty"`
				}
				if httpErr, ok := osb.IsHTTPError(herr); ok {
					body := &e{}
					if httpErr.Description != nil {
						body.Description = httpErr.Description
					}
					if httpErr.ErrorMessage != nil {
						body.ErrorMessage = httpErr.ErrorMessage
					}
					HttpWrite(w, httpErr.StatusCode, body)
					return
				} else {
					msg := "InternalServerError"
					description := "Internal Server Error"
					body := &e{ErrorMessage:&msg, Description:&description}
					HttpWrite(w, 500, body)
					return
				}
			}
			if obj != nil {
				HttpWrite(w, 200, obj)
			} else {
				HttpWrite(w, 200, map[string]string{})
			}
		}).Methods(action.method)
	}
	router.HandleFunc("/v2/service_instances/{instance_id}/actions/{action_name}/schema", b.ActionSchemaHandler).Methods("GET")
	return nil
}

func (b *ActionBase) ConvertActionsToExtensions(serviceId string) []osb.ExtensionAPI {
	extensions := make([]osb.ExtensionAPI, 0)
	var baseUrl = ""
	for _, action := range b.actions {
		extensions = append(extensions, osb.ExtensionAPI{
			DiscoveryURL: baseUrl + "/v2/service_instances/" + serviceId + "/actions/" + action.name + "/schema",
			ServerURL:    baseUrl + "/v2/service_instances/" + serviceId + "/actions/",
		})
	}
	return extensions
}

func (b *ActionBase) AddActions(name string, path string, method string, handler func(string, map[string]string, *broker.RequestContext) (interface{}, error)) error {
	b.Lock()
	defer b.Unlock()
	b.actions = append(b.actions, Action{
		name:    name,
		path:    path,
		method:  method,
		handler: handler,
	})
	return nil
}

// These are hacks to support more of V2.14 such as get service instance and get service bindings.
func CrudeOSBIHacks(router *mux.Router, b *BusinessLogic) {
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		req := osb.GetBindingRequest{InstanceID: vars["instance_id"], BindingID: vars["binding_id"]}
		c := broker.RequestContext{Request: r, Writer: w}
		resp, err := b.GetBinding(&req, &c)
		if err != nil {
			type e struct {
				ErrorMessage *string `json:"error,omitempty"`
				Description  *string `json:"description,omitempty"`
			}
			if httpErr, ok := osb.IsHTTPError(err); ok {
				body := &e{}
				if httpErr.Description != nil {
					body.Description = httpErr.Description
				}
				if httpErr.ErrorMessage != nil {
					body.ErrorMessage = httpErr.ErrorMessage
				}
				HttpWrite(w, httpErr.StatusCode, body)
				return
			} else {
				msg := "InternalServerError"
				description := "Internal Server Error"
				body := &e{ErrorMessage:&msg, Description:&description}
				HttpWrite(w, 500, body)
				return
			}
		}
		HttpWrite(w, 200, resp)
	}).Methods("GET")
}

package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"net/url"
	"strings"
)

// provider=shared-mysql in database
// These values come out of the plans table provider_private_details column.
type MysqlSharedProviderPrivatePlanSettings struct {
	MasterUri     string `json:"master_uri"`
	Engine        string `json:"engine"`
	EngineVersion string `json:"engine_version"`
	SchemeType    string `json:"scheme_type"` /* Can be 'dsn', 'uri' */
}

func (psppps MysqlSharedProviderPrivatePlanSettings) MasterHost() string {
	db, err := url.Parse(psppps.MasterUri)
	if err != nil {
		return ""
	}
	return db.Host
}

func (psppps MysqlSharedProviderPrivatePlanSettings) GetMasterUriWithDb(dbName string) string {
	db, err := url.Parse(psppps.MasterUri)
	if err != nil {
		return ""
	}
	pass, ok := db.User.Password()
	if ok == true {
		return "mysql://" + db.User.Username() + ":" + pass + "@" + db.Host + "/" + dbName + "?" + db.RawQuery
	} else if db.User.Username() != "" {
		return "mysql://" + db.User.Username() + "@" + db.Host + "/" + dbName + "?" + db.RawQuery
	} else {
		return "mysql://" + db.Host + "/" + dbName + "?" + db.RawQuery
	}
}

func (psppps MysqlSharedProviderPrivatePlanSettings) GetMasterUriWithDbAsDsn(dbName string) string {
	db, err := url.Parse(psppps.MasterUri)
	if err != nil {
		return ""
	}
	port := db.Port()
	if port == "" {
		port = "3306"
	}
	pass, ok := db.User.Password()
	if ok == true {
		return db.User.Username() + ":" + pass + "@tcp(" + db.Hostname() + ":" + port + ")/" + dbName + "?" + db.RawQuery
	} else if db.User.Username() != "" {
		return "tcp(" + db.Hostname() + ":" + port + ")/" + dbName + "?" + db.RawQuery
	} else {
		return db.User.Username() + "@tcp(" + db.Hostname() + ":" + port + ")/" + dbName + "?" + db.RawQuery
	}
}

func (psppps MysqlSharedProviderPrivatePlanSettings) GetMasterUriAsDsn() string {
	return psppps.GetMasterUriWithDbAsDsn("mysql")
}

type MysqlSharedProvider struct {
	Provider
	namePrefix string
}

func NewMysqlSharedProvider(namePrefix string) (MysqlSharedProvider, error) {
	return MysqlSharedProvider{
		namePrefix: namePrefix,
	}, nil
}

func (provider MysqlSharedProvider) GetInstance(name string, plan *ProviderPlan) (*DbInstance, error) {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	var endpoint = settings.MasterHost() + "/" + name
	var scheme = plan.Scheme

	if settings.SchemeType == "dsn" {
		scheme = ""
		endpoint = "tcp(" + settings.MasterHost() + ")/" + name
	}
	return &DbInstance{
		Id:            "",
		ProviderId:    name,
		Name:          name,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      endpoint,
		Status:        "available",
		Ready:         true,
		Engine:        "mysql",
		EngineVersion: settings.EngineVersion,
		Scheme:        scheme,
	}, nil
}

func (provider MysqlSharedProvider) PerformPostProvision(db *DbInstance) (*DbInstance, error) {
	return db, nil
}

func (provider MysqlSharedProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, errors.New("Cannot unmarshal private details: " + err.Error())
	}

	db_name := strings.ToLower(provider.namePrefix + RandomString(8))
	username := strings.ToLower("u" + RandomString(8))
	password := RandomString(16)
	db, err := sql.Open("mysql", settings.GetMasterUriAsDsn())
	if err != nil {
		return nil, errors.New("Cannot provision shared database (connection failure): " + err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("CREATE USER '" + username + "' identified by '" + password + "'"); err != nil {
		return nil, errors.New("Failed to create user with password: " + err.Error())
	}
	if _, err = db.Exec("CREATE DATABASE " + db_name); err != nil {
		return nil, errors.New("Failed to create database with owner on shared tenant " + err.Error())
	}
	if _, err = db.Exec("GRANT all on " + db_name + ".* TO " + username); err != nil {
		return nil, errors.New("Failed to grant access to user on shared tenant " + err.Error())
	}

	var endpoint = settings.MasterHost() + "/" + db_name
	var scheme = plan.Scheme

	if settings.SchemeType == "dsn" {
		scheme = ""
		endpoint = "tcp(" + settings.MasterHost() + ")/" + db_name
	}

	return &DbInstance{
		Id:            Id,
		Name:          db_name,
		ProviderId:    db_name,
		Plan:          plan,
		Username:      username,
		Password:      password,
		Endpoint:      endpoint,
		Status:        "available",
		Ready:         true,
		Engine:        settings.Engine,
		EngineVersion: settings.EngineVersion,
		Scheme:        scheme,
	}, nil
}

// TODO: take snapshot somehow.
func (provider MysqlSharedProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}

	db, err := sql.Open("mysql", settings.GetMasterUriAsDsn())
	if err != nil {
		return errors.New("Cannot deprovision shared database (connection failure): " + err.Error())
	}
	defer db.Close()

	// Get a list of all read only users
	rows, err := db.Query(ApplyParamsToStatement("select grantee as role from information_schema.schema_privileges where table_schema = $1 and grantee not like $2", "'"+dbInstance.Name+"'", "'%"+dbInstance.Username+"%'"))
	if err != nil {
		return errors.New("Failed to query read only users in role: " + err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return errors.New("Failed to scan read only users in role: " + err.Error())
		}
		role = strings.Split(strings.Replace(role, "'", "", -1), "@")[0]
		if err = DeleteMysqlReadOnlyRole(dbInstance, settings.GetMasterUriWithDbAsDsn(dbInstance.Name), role); err != nil {
			return errors.New("Failed to remove read only user while deprovisioning database: " + dbInstance.Name + " error: " + err.Error())
		}
	}
	if err := rows.Err(); err != nil {
		return errors.New("Failed to deprovision database while trying to fetch read only user results: " + dbInstance.Name + " error: " + err.Error())
	}
	if _, err = db.Exec("REVOKE all privileges, grant option from " + dbInstance.Username); err != nil {
		return errors.New("Failed to revoke access from master user to shared tenant user: " + dbInstance.Name + " error: " + err.Error())
	}
	if _, err = db.Exec("DROP DATABASE " + dbInstance.Name); err != nil {
		return errors.New("Failed to drop database shared tenant: " + dbInstance.Name + " error: " + err.Error())
	}
	if _, err = db.Exec("DROP USER " + dbInstance.Username); err != nil {
		return errors.New("Failed to remove user: " + dbInstance.Name + " error: " + err.Error())
	}
	return nil
}

func (provider MysqlSharedProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	// do nothing
	return nil
}

func (provider MysqlSharedProvider) Untag(dbInstance *DbInstance, Name string) error {
	// do nothing
	return nil
}

func (provider MysqlSharedProvider) GetBackup(dbInstance *DbInstance, Id string) (DatabaseBackupSpec, error) {
	return DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) CreateReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) GetReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) DeleteReadReplica(dbInstance *DbInstance) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	return []DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	return DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) Restart(dbInstance *DbInstance) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) ListLogs(dbInstance *DbInstance) ([]DatabaseLogs, error) {
	return []DatabaseLogs{},
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) GetLogs(dbInstance *DbInstance, path string) (string, error) {
	return "",
		errors.New("This feature is not available on this plan.")
}

func (provider MysqlSharedProvider) CreateReadOnlyUser(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return CreateMysqlReadOnlyRole(dbInstance, settings.GetMasterUriWithDbAsDsn(dbInstance.Name))
}

func (provider MysqlSharedProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}
	return DeleteMysqlReadOnlyRole(dbInstance, settings.GetMasterUriWithDbAsDsn(dbInstance.Name), role)
}

func (provider MysqlSharedProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	var settings MysqlSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return RotateMysqlReadOnlyRole(dbInstance, settings.GetMasterUriWithDbAsDsn(dbInstance.Name), role)
}

// Technically the create role functions are used by any provider that implements mysql but we'll place
// them here, but be aware they're not specific to this provider.
func CreateMysqlReadOnlyRole(dbInstance *DbInstance, databaseUri string) (DatabaseUrlSpec, error) {
	if dbInstance.Engine != "mysql" {
		return DatabaseUrlSpec{}, errors.New("I do not know how to do this on anything other than mysql.")
	}

	username := "rdo1" + strings.ToLower(RandomString(7))
	password := RandomString(10)

	db, err := sql.Open("mysql", databaseUri)

	if err != nil {
		return DatabaseUrlSpec{}, err
	}
	defer db.Close()

	if _, err = db.Exec("create user '" + username + "'@'%' identified by '" + password + "'"); err != nil {
		return DatabaseUrlSpec{}, errors.New("Failed to reduce connection limit when deprovisioning: " + dbInstance.Name + " error: " + err.Error())
	}
	if _, err = db.Exec("grant select on " + dbInstance.Name + ".* to '" + username + "'"); err != nil {
		return DatabaseUrlSpec{}, errors.New("Failed to reduce connection limit when deprovisioning: " + dbInstance.Name + " error: " + err.Error())
	}
	return DatabaseUrlSpec{
		Username: username,
		Password: password,
		Endpoint: dbInstance.Endpoint,
		Plan:     dbInstance.Plan.ID,
	}, nil
}

func RotateMysqlReadOnlyRole(dbInstance *DbInstance, databaseUri string, role string) (DatabaseUrlSpec, error) {
	db, err := sql.Open("mysql", databaseUri)
	if err != nil {
		return DatabaseUrlSpec{}, err
	}
	defer db.Close()
	password := RandomString(10)
	if _, err = db.Exec("UPDATE mysql.user SET authentication_string = PASSWORD('" + password + "') WHERE User = '" + role + "'"); err != nil {
		return DatabaseUrlSpec{}, err
	}
	if _, err = db.Exec("flush privileges"); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return DatabaseUrlSpec{
		Username: role,
		Password: password,
		Endpoint: dbInstance.Endpoint,
	}, nil
}

func DeleteMysqlReadOnlyRole(dbInstance *DbInstance, databaseUri string, role string) error {
	db, err := sql.Open("mysql", databaseUri)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.Exec("REVOKE all privileges, grant option from " + role); err != nil {
		return errors.New("Failed to revoke access from master user to shared tenant user: " + dbInstance.Name + " error: " + err.Error())
	}
	if _, err = db.Exec("DROP USER " + role); err != nil {
		return errors.New("Failed to remove user: " + dbInstance.Name + " error: " + err.Error())
	}

	return nil
}

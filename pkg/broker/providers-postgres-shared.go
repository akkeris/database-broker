package broker

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/lib/pq"
	"net/url"
	"strings"
)

// provider=shared-postgres in database
// These values come out of the plans table provider_private_details column.
type PostgresSharedProviderPrivatePlanSettings struct {
	MasterUri     string `json:"master_uri"`
	Engine        string `json:"engine"`
	EngineVersion string `json:engine_version"`
}

func (psppps PostgresSharedProviderPrivatePlanSettings) MasterHost() string {
	db, err := url.Parse(psppps.MasterUri)
	if err != nil {
		return ""
	}
	return db.Host
}

func (psppps PostgresSharedProviderPrivatePlanSettings) GetMasterUriWithDb(dbName string) string {
	db, err := url.Parse(psppps.MasterUri)
	if err != nil {
		return ""
	}
	pass, ok := db.User.Password()
	if ok == true {
		return "postgres://" + db.User.Username() + ":" + pass + "@" + db.Host + "/" + dbName + "?" + db.RawQuery
	} else if db.User.Username() != "" {
		return "postgres://" + db.User.Username() + "@" + db.Host + "/" + dbName + "?" + db.RawQuery
	} else {
		return "postgres://" + db.Host + "/" + dbName + "?" + db.RawQuery
	}
}

type PostgresSharedProvider struct {
	Provider
	namePrefix string
}

func NewPostgresSharedProvider(namePrefix string) (PostgresSharedProvider, error) {
	return PostgresSharedProvider{
		namePrefix: namePrefix,
	}, nil
}

func (provider PostgresSharedProvider) GetInstance(name string, plan *ProviderPlan) (*DbInstance, error) {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	return &DbInstance{
		Id:            "",
		ProviderId:    name,
		Name:          name,
		Plan:          plan,
		Username:      "",
		Password:      "",
		Endpoint:      settings.MasterHost() + "/" + name,
		Status:        "available",
		Ready:         true,
		Engine:        "postgres",
		EngineVersion: settings.EngineVersion,
		Scheme:        "postgres",
	}, nil
}

func (provider PostgresSharedProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, errors.New("Cannot unmarshal private details: " + err.Error())
	}

	db_name := strings.ToLower(provider.namePrefix + RandomString(8))
	username := strings.ToLower("u" + RandomString(8))
	password := RandomString(16)
	db, err := sql.Open("postgres", settings.MasterUri)
	if err != nil {
		return nil, errors.New("Cannot provision shared database (connection failure): " + err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("CREATE USER " + username + " WITH PASSWORD '" + password + "' NOINHERIT"); err != nil {
		return nil, errors.New("Failed to create user with password: " + err.Error())
	}
	if _, err = db.Exec("GRANT " + username + " TO CURRENT_USER"); err != nil {
		return nil, errors.New("Failed to grant access to master user on shared tenant " + err.Error())
	}
	if _, err = db.Exec("CREATE DATABASE " + db_name + " OWNER " + username); err != nil {
		return nil, errors.New("Failed to create database with owner on shared tenant " + err.Error())
	}

	// add postgres extensions
	udb, err := sql.Open("postgres", settings.GetMasterUriWithDb(db_name))
	if err != nil {
		return nil, errors.New("Cannot connect to new provisioned db: " + err.Error())
	}
	defer udb.Close()

	if _, err = udb.Exec("CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public"); err != nil {
		return nil, errors.New("Cannot create extension postgres_fdw on new db: " + err.Error())
	}
	if _, err = udb.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public"); err != nil {
		return nil, errors.New("Cannot create extension pgcrypto on new db: " + err.Error())
	}
	if _, err = udb.Exec("CREATE EXTENSION IF NOT EXISTS tablefunc WITH SCHEMA public"); err != nil {
		return nil, errors.New("Cannot create extension tablefunc on new db: " + err.Error())
	}
	if _, err = udb.Exec("CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public"); err != nil {
		return nil, errors.New("Cannot create extension hstore on new db: " + err.Error())
	}
	if _, err = udb.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" WITH SCHEMA public"); err != nil {
		return nil, errors.New("Cannot create extension uuid-ossp on new db: " + err.Error())
	}
	return &DbInstance{
		Id:            Id,
		Name:          db_name,
		ProviderId:    db_name,
		Plan:          plan,
		Username:      username,
		Password:      password,
		Endpoint:      settings.MasterHost() + "/" + db_name,
		Status:        "available",
		Ready:         true,
		Engine:        settings.Engine,
		EngineVersion: settings.EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

// TODO: take snapshot somehow.
func (provider PostgresSharedProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}

	db, err := sql.Open("postgres", settings.MasterUri)
	if err != nil {
		return errors.New("Cannot deprovision shared database (connection failure): " + err.Error())
	}

	defer db.Close()
	if _, err = db.Exec("DROP DATABASE " + dbInstance.Name); err != nil {
		return errors.New("Failed to drop database shared tenant: " + err.Error())
	}
	if _, err = db.Exec("REVOKE " + dbInstance.Username + " FROM CURRENT_USER"); err != nil {
		return errors.New("Failed to revoke access from master user to shared tenant user: " + err.Error())
	}
	if _, err = db.Exec("DROP USER " + dbInstance.Username); err != nil {
		return errors.New("Failed to remove user: " + dbInstance.Username + " " + err.Error())
	}

	return nil
}

func (provider PostgresSharedProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	// do nothing
	return nil
}

func (provider PostgresSharedProvider) Untag(dbInstance *DbInstance, Name string) error {
	// do nothing
	return nil
}

func (provider PostgresSharedProvider) GetBackup(dbInstance *DbInstance, Id string) (DatabaseBackupSpec, error) {
	return DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) CreateReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) GetReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	return nil,
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) DeleteReadReplica(dbInstance *DbInstance) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	return []DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	return DatabaseBackupSpec{},
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) Restart(dbInstance *DbInstance) error {
	return errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) ListLogs(dbInstance *DbInstance) ([]DatabaseLogs, error) {
	return []DatabaseLogs{},
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) GetLogs(dbInstance *DbInstance, path string) (string, error) {
	return "",
		errors.New("This feature is not available on this plan.")
}

func (provider PostgresSharedProvider) CreateReadOnlyUser(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return CreatePostgresReadOnlyRole(dbInstance, settings.GetMasterUriWithDb(dbInstance.Name))
}

func (provider PostgresSharedProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}
	return DeletePostgresReadOnlyRole(dbInstance, settings.GetMasterUriWithDb(dbInstance.Name), role)
}

func (provider PostgresSharedProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	var settings PostgresSharedProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return RotatePostgresReadOnlyRole(dbInstance, settings.GetMasterUriWithDb(dbInstance.Name), role)
}

func (provider PostgresSharedProvider) UpgradeVersion(dbInstance *DbInstance, proposed string) (*DbInstance, error) {
	return nil, errors.New("This feature is not available on this plan")
}

// Technically the create role functions are used by any provider that implements postgres but we'll place
// them here, but be aware they're not specific to this provider.
func CreatePostgresReadOnlyRole(dbInstance *DbInstance, databaseUri string) (DatabaseUrlSpec, error) {
	if dbInstance.Engine != "postgres" {
		return DatabaseUrlSpec{}, errors.New("I do not know how to do this on anything other than postgres.")
	}
	statement := `
	do $$
	begin
	  create user $1 with login encrypted password $2;
	  grant select on all tables in schema public TO $1;
	  grant usage, select on all sequences in schema public TO $1;
	  grant connect on database $3 to $1;
	  alter default privileges in schema public GRANT SELECT ON TABLES TO $1;
	  REVOKE CREATE ON SCHEMA public FROM $1;
	  GRANT USAGE ON SCHEMA public TO $1;

	  ALTER DEFAULT PRIVILEGES FOR USER $4 IN SCHEMA public GRANT SELECT ON SEQUENCES TO $1;
	  ALTER DEFAULT PRIVILEGES FOR USER $4 IN SCHEMA public GRANT SELECT ON TABLES TO $1;
	end 
	$$;
	`

	app_username := dbInstance.Username
	db, err := sql.Open("postgres", databaseUri)
	if err != nil {
		return DatabaseUrlSpec{}, err
	}
	defer db.Close()

	username := "rdo1" + strings.ToLower(RandomString(7))
	password := RandomString(10)

	_, err = db.Exec(strings.Replace(strings.Replace(strings.Replace(strings.Replace(statement, "$1", username, -1), "$2", "'"+password+"'", -1), "$3", dbInstance.Name, -1), "$4", app_username, -1))
	if err != nil {
		return DatabaseUrlSpec{}, err
	}
	return DatabaseUrlSpec{
		Username: username,
		Password: password,
		Endpoint: dbInstance.Endpoint,
		Plan:     dbInstance.Plan.ID,
	}, nil
}

func RotatePostgresReadOnlyRole(dbInstance *DbInstance, databaseUri string, role string) (DatabaseUrlSpec, error) {
	db, err := sql.Open("postgres", databaseUri)
	if err != nil {
		return DatabaseUrlSpec{}, err
	}
	defer db.Close()
	password := RandomString(10)
	if _, err = db.Exec("alter user " + role + " WITH PASSWORD '" + password + "'"); err != nil {
		return DatabaseUrlSpec{}, err
	}
	return DatabaseUrlSpec{
		Username: role,
		Password: password,
		Endpoint: dbInstance.Endpoint,
	}, nil
}

func DeletePostgresReadOnlyRole(dbInstance *DbInstance, databaseUri string, role string) error {
	statement := `
	do $$
	begin
	  ALTER DEFAULT PRIVILEGES FOR USER $3 IN SCHEMA public REVOKE SELECT ON SEQUENCES FROM $1;
	  ALTER DEFAULT PRIVILEGES FOR USER $3 IN SCHEMA public REVOKE SELECT ON TABLES FROM $1;
	  revoke usage on schema public FROM $1;
	  revoke connect on database $2 from $1;
	  revoke select on all tables in schema public from $1;
	  revoke usage, select on all sequences in schema public from $1;
	  alter default privileges in schema public REVOKE SELECT ON TABLES FROM $1;
	  drop user $1;
	end 
	$$;
	`
	db, err := sql.Open("postgres", databaseUri)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(strings.Replace(strings.Replace(strings.Replace(statement, "$1", role, -1), "$2", dbInstance.Name, -1), "$3", dbInstance.Username, -1))
	if err != nil {
		return err
	}
	return nil
}

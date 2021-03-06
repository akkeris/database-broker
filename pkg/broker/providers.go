package broker

import (
	"errors"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
)

type Providers string

const (
	AWSInstance    Providers = "aws-instance"
	AWSCluster     Providers = "aws-cluster"
	GCloudInstance Providers = "gcloud-instance"
	PostgresShared Providers = "postgres-shared"
	MysqlShared    Providers = "mysql-shared"
	Unknown        Providers = "unknown"
)

func GetProvidersFromString(str string) Providers {
	if str == "aws-instance" {
		return AWSInstance
	} else if str == "aws-cluster" {
		return AWSCluster
	}  else if str == "gcloud-instance" {
		return GCloudInstance
	} else if str == "postgres-shared" {
		return PostgresShared
	} else if str == "mysql-shared" {
		return MysqlShared
	}
	return Unknown
}

type ProviderPlan struct {
	basePlan               osb.Plan  `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	Provider               Providers `json:"provider"`
	providerPrivateDetails string    `json:"-"` /* NEVER allow this to be serialized into a JSON call as it may accidently send sensitive info to callbacks */
	ID                     string    `json:"id"`
	Scheme                 string    `json:"scheme"`
}

type Provider interface {
	GetInstance(string, *ProviderPlan) (*DbInstance, error)
	Provision(string, *ProviderPlan, string) (*DbInstance, error)
	Deprovision(*DbInstance, bool) error
	Modify(*DbInstance, *ProviderPlan) (*DbInstance, error)
	Tag(*DbInstance, string, string) error
	Untag(*DbInstance, string) error
	GetBackup(*DbInstance, string) (DatabaseBackupSpec, error)
	ListBackups(*DbInstance) ([]DatabaseBackupSpec, error)
	CreateBackup(*DbInstance) (DatabaseBackupSpec, error)
	RestoreBackup(*DbInstance, string) error
	Restart(*DbInstance) error
	ListLogs(*DbInstance) ([]DatabaseLogs, error)
	GetLogs(*DbInstance, string) (string, error)
	CreateReadOnlyUser(*DbInstance) (DatabaseUrlSpec, error)
	DeleteReadOnlyUser(*DbInstance, string) error
	RotatePasswordReadOnlyUser(*DbInstance, string) (DatabaseUrlSpec, error)
	CreateReadReplica(*DbInstance) (*DbInstance, error)
	GetReadReplica(*DbInstance) (*DbInstance, error)
	DeleteReadReplica(*DbInstance) error
	PerformPostProvision(*DbInstance) (*DbInstance, error)
}

func GetProviderByPlan(namePrefix string, plan *ProviderPlan) (Provider, error) {
	if plan.Provider == AWSInstance {
		return NewAWSInstanceProvider(namePrefix)
	} else if plan.Provider == AWSCluster {
		return NewAWSClusteredProvider(namePrefix)
	} else if plan.Provider == GCloudInstance {
		return NewGCloudInstanceProvider(namePrefix)
	} else if plan.Provider == MysqlShared {
		return NewMysqlSharedProvider(namePrefix)
	} else if plan.Provider == PostgresShared {
		return NewPostgresSharedProvider(namePrefix)
	} else {
		return nil, errors.New("Unable to find provider for plan.")
	}
}

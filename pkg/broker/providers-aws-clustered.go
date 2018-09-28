package broker

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"os"
	"strings"
)

type AWSClusteredProvider struct {
	Provider
	awsInstanceProvider  *AWSInstanceProvider
	awssvc               *rds.RDS
	namePrefix           string
	awsVpcSecurityGroup  string
}

type AWSClusteredProviderPrivatePlanSettings struct {
	Instance      	rds.CreateDBInstanceInput `json:"Instance"`
	Cluster			rds.CreateDBClusterInput `json:"Cluster"`
}

func NewAWSClusteredProvider(namePrefix string) (*AWSClusteredProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	if os.Getenv("AWS_VPC_SECURITY_GROUPS") == "" {
		return nil, errors.New("Unable to find AWS_VPC_SECURITY_GROUPS environment variable.")
	}
	awsInstanceProvider, err := NewAWSInstanceProvider(namePrefix)
	if err != nil {
		return nil, err
	}
	return &AWSClusteredProvider{
		namePrefix:          namePrefix,
		awsInstanceProvider: awsInstanceProvider,
		awsVpcSecurityGroup: os.Getenv("AWS_VPC_SECURITY_GROUPS"),
		awssvc:              rds.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
	}, nil
}

func (provider AWSClusteredProvider) GetInstance(name string, plan *ProviderPlan) (*DbInstance, error) {
	return provider.awsInstanceProvider.GetInstance(name, plan)
}

func (provider AWSClusteredProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings AWSClusteredProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	settings.Cluster.DatabaseName = aws.String(strings.ToLower(provider.namePrefix + RandomString(8)))
	settings.Cluster.DBClusterIdentifier = settings.Cluster.DatabaseName
	settings.Cluster.MasterUsername = aws.String(strings.ToLower("u" + RandomString(8)))
	settings.Cluster.MasterUserPassword = aws.String(RandomString(16))
	settings.Cluster.Tags = []*rds.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	settings.Cluster.VpcSecurityGroupIds = []*string{aws.String(provider.awsVpcSecurityGroup)}

	settings.Instance.DBName = settings.Cluster.DatabaseName
	settings.Instance.DBInstanceIdentifier = settings.Cluster.DatabaseName
	settings.Instance.MasterUsername = aws.String(strings.ToLower("u" + RandomString(8)))
	settings.Instance.MasterUserPassword = aws.String(RandomString(16))
	settings.Instance.Tags = []*rds.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	settings.Instance.VpcSecurityGroupIds = []*string{aws.String(provider.awsVpcSecurityGroup)}
	settings.Instance.DBClusterIdentifier = settings.Cluster.DatabaseName

	_, err := provider.awssvc.CreateDBCluster(&settings.Cluster)

	if err != nil {
		return nil, err
	}

	return provider.awsInstanceProvider.ProvisionWithSettings(Id, plan, &settings.Instance)
}

func (provider AWSClusteredProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	err := provider.awsInstanceProvider.Deprovision(dbInstance, takeSnapshot)
	if err != nil {
		return nil
	}
	_, err = provider.awssvc.DeleteDBCluster(&rds.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(dbInstance.Name),
		SkipFinalSnapshot:   aws.Bool(!takeSnapshot),
	})
	return err
}

func (provider AWSClusteredProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	if dbInstance.Status != "available" {
		return nil, errors.New("Replicas cannot be created for databases being created, under maintenance or destroyed.")
	}
	var settings AWSClusteredProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	_, err := provider.awssvc.ModifyDBCluster(&rds.ModifyDBClusterInput{
		ApplyImmediately:			aws.Bool(true),
		BacktrackWindow:			settings.Cluster.BacktrackWindow,
		BackupRetentionPeriod:		settings.Cluster.BackupRetentionPeriod,
		DBClusterIdentifier:		aws.String(dbInstance.Name),
		DBClusterParameterGroupName:settings.Cluster.DBClusterParameterGroupName,
		EngineVersion: 				settings.Cluster.EngineVersion,
		OptionGroupName:			settings.Cluster.OptionGroupName,
		Port:						settings.Cluster.Port,
		PreferredBackupWindow:		settings.Cluster.PreferredBackupWindow,
		PreferredMaintenanceWindow: settings.Cluster.PreferredMaintenanceWindow,
		ScalingConfiguration:		settings.Cluster.ScalingConfiguration,
		VpcSecurityGroupIds:		[]*string{aws.String(provider.awsVpcSecurityGroup)},
	})
	if err != nil {
		return nil, err
	}
	return provider.awsInstanceProvider.ModifyWithSettings(dbInstance, plan, &settings.Instance)
}

func (provider AWSClusteredProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	return provider.awsInstanceProvider.Tag(dbInstance, Name, Value)
}

func (provider AWSClusteredProvider) Untag(dbInstance *DbInstance, Name string) error {
	return provider.awsInstanceProvider.Untag(dbInstance, Name)
}

func (provider AWSClusteredProvider) GetBackup(dbInstance *DbInstance, Id string) (DatabaseBackupSpec, error) {
	return provider.awsInstanceProvider.GetBackup(dbInstance, Id)
}

func (provider AWSClusteredProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	return provider.awsInstanceProvider.ListBackups(dbInstance)
}

func (provider AWSClusteredProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	return provider.awsInstanceProvider.CreateBackup(dbInstance)
}

func (provider AWSClusteredProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	return provider.awsInstanceProvider.RestoreBackup(dbInstance, Id)
}

func (provider AWSClusteredProvider) Restart(dbInstance *DbInstance) error {
	return provider.awsInstanceProvider.Restart(dbInstance)
}

func (provider AWSClusteredProvider) ListLogs(dbInstance *DbInstance) ([]DatabaseLogs, error) {
	return provider.awsInstanceProvider.ListLogs(dbInstance)
}

func (provider AWSClusteredProvider) GetLogs(dbInstance *DbInstance, path string) (string, error) {
	return provider.awsInstanceProvider.GetLogs(dbInstance, path)
}

func (provider AWSClusteredProvider) CreateReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	var settings AWSClusteredProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	settings.Instance.DBName = aws.String(dbInstance.Name)
	settings.Instance.DBInstanceIdentifier = aws.String(dbInstance.Name + "-ro")
	settings.Instance.MasterUsername = aws.String(strings.ToLower("u" + RandomString(8)))
	settings.Instance.MasterUserPassword = aws.String(RandomString(16))
	settings.Instance.VpcSecurityGroupIds = []*string{aws.String(provider.awsVpcSecurityGroup)}
	settings.Instance.DBClusterIdentifier = aws.String(dbInstance.Name)

	return provider.awsInstanceProvider.ProvisionWithSettings(dbInstance.Name + "-ro", dbInstance.Plan, &settings.Instance)
}

func (provider AWSClusteredProvider) GetReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	return provider.awsInstanceProvider.GetReadReplica(dbInstance)
}

func (provider AWSClusteredProvider) DeleteReadReplica(dbInstance *DbInstance) error {
	return provider.awsInstanceProvider.DeleteReadReplica(dbInstance)
}

func (provider AWSClusteredProvider) CreateReadOnlyUser(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	return provider.awsInstanceProvider.CreateReadOnlyUser(dbInstance)
}

func (provider AWSClusteredProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	return provider.awsInstanceProvider.DeleteReadOnlyUser(dbInstance, role)
}

func (provider AWSClusteredProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	return provider.awsInstanceProvider.RotatePasswordReadOnlyUser(dbInstance, role)
}

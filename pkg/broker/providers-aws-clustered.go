package broker

import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"os"
	"strings"
	"time"
	"fmt"
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

	settings.Instance.DBInstanceIdentifier = settings.Cluster.DatabaseName
	settings.Instance.Tags = []*rds.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	settings.Instance.DBClusterIdentifier = settings.Cluster.DatabaseName

	_, err := provider.awssvc.CreateDBCluster(&settings.Cluster)
	if err != nil {
		return nil, err
	}

	dbInstance, err := provider.awsInstanceProvider.ProvisionWithSettings(Id, plan, &settings.Instance)
	if err != nil {
		return nil, err
	}
	if settings.Cluster.MasterUserPassword == nil {
		return nil, errors.New("Unable to obtain the master password for this cluster, it was nil.")
	}
	dbInstance.Password = *settings.Cluster.MasterUserPassword
	return dbInstance, nil
}

func (provider AWSClusteredProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	resp, err := provider.awssvc.DescribeDBClusters(&rds.DescribeDBClustersInput{
		DBClusterIdentifier: 	aws.String(dbInstance.Name),
		MaxRecords:           	aws.Int64(20),
	})
	if err != nil {
		return  err
	}
	if len(resp.DBClusters) != 1 {
		return errors.New("Found none or multiples matching this cluster name.")
	}
	
	var dbPrimaryIdentifier *string = nil
	// delete all non-primary (read only replicas, etc)
	for _, member := range resp.DBClusters[0].DBClusterMembers {
		if member.IsClusterWriter != nil && *member.IsClusterWriter == true {
			dbPrimaryIdentifier = member.DBInstanceIdentifier
		} else {
			_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
				DBInstanceIdentifier:      member.DBInstanceIdentifier,
				SkipFinalSnapshot:         aws.Bool(!takeSnapshot),
			})
			if err != nil {
				return err
			}
		}
	}
	// delete primary database
	if dbPrimaryIdentifier == nil {
		return errors.New("Unable to find primary database identifier")
	}
	_, err = provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:      dbPrimaryIdentifier,
		SkipFinalSnapshot:         aws.Bool(!takeSnapshot),
	})
	if err != nil {
		return err
	}
	// delete cluster, you can't delete the cluster until all of the db instances
	// have been marked as removed.
	_, err = provider.awssvc.DeleteDBCluster(&rds.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(dbInstance.Name),
		SkipFinalSnapshot:   aws.Bool(!takeSnapshot),
		FinalDBSnapshotIdentifier: aws.String(dbInstance.Name + "-final"),
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
	snapshots, err := provider.awssvc.DescribeDBClusterSnapshots(&rds.DescribeDBClusterSnapshotsInput{
		DBClusterIdentifier: aws.String(dbInstance.Name),
		DBClusterSnapshotIdentifier: aws.String(Id),
	})
	if err != nil {
		return DatabaseBackupSpec{}, err
	}
	if len(snapshots.DBClusterSnapshots) != 1 {
		return DatabaseBackupSpec{}, errors.New("Not found")
	}

	created := time.Now().UTC().Format(time.RFC3339)
	if snapshots.DBClusterSnapshots[0].SnapshotCreateTime != nil {
		created = snapshots.DBClusterSnapshots[0].SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return DatabaseBackupSpec{
		Database: DatabaseSpec{
			Name: dbInstance.Name,
		},
		Id:       snapshots.DBClusterSnapshots[0].DBClusterSnapshotIdentifier,
		Progress: snapshots.DBClusterSnapshots[0].PercentProgress,
		Status:   snapshots.DBClusterSnapshots[0].Status,
		Created:  created,
	}, nil
}

func (provider AWSClusteredProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	snapshots, err := provider.awssvc.DescribeDBClusterSnapshots(&rds.DescribeDBClusterSnapshotsInput{DBClusterIdentifier: aws.String(dbInstance.Name)})
	if err != nil {
		return []DatabaseBackupSpec{}, err
	}
	out := make([]DatabaseBackupSpec, 0)
	for _, snapshot := range snapshots.DBClusterSnapshots {
		created := time.Now().UTC().Format(time.RFC3339)
		if snapshot.SnapshotCreateTime != nil {
			created = snapshot.SnapshotCreateTime.UTC().Format(time.RFC3339)
		}
		out = append(out, DatabaseBackupSpec{
			Database: DatabaseSpec{
				Name: dbInstance.Name,
			},
			Id:       snapshot.DBClusterSnapshotIdentifier,
			Progress: snapshot.PercentProgress,
			Status:   snapshot.Status,
			Created:  created,
		})
	}
	return out, nil
}

func (provider AWSClusteredProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	snapshot_name := (dbInstance.Name + "-manual-" + RandomString(10))
	snapshot, err := provider.awssvc.CreateDBClusterSnapshot(&rds.CreateDBClusterSnapshotInput{
		DBClusterIdentifier: aws.String(dbInstance.Name),
		DBClusterSnapshotIdentifier: aws.String(snapshot_name),
	})
	if err != nil {
		return DatabaseBackupSpec{}, err
	}
	created := time.Now().UTC().Format(time.RFC3339)
	if snapshot.DBClusterSnapshot.SnapshotCreateTime != nil {
		created = snapshot.DBClusterSnapshot.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return DatabaseBackupSpec{
		Database: DatabaseSpec{
			Name: dbInstance.Name,
		},
		Id:       snapshot.DBClusterSnapshot.DBClusterSnapshotIdentifier,
		Progress: snapshot.DBClusterSnapshot.PercentProgress,
		Status:   snapshot.DBClusterSnapshot.Status,
		Created:  created,
	}, nil
}

func (provider AWSClusteredProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	var settings AWSClusteredProviderPrivatePlanSettings
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}

	// For AWS, the best strategy for restoring (reliably) a database is to rename the existing db
	// then create from a snapshot the existing db, and then nuke the old one once finished.

	renamedSuffix := "-restore-" + RandomString(5)

	// 1. Rename all db instances in the cluster
	resp, err := provider.awssvc.DescribeDBClusters(&rds.DescribeDBClustersInput{
		DBClusterIdentifier: 	aws.String(dbInstance.Name),
		MaxRecords:           	aws.Int64(20),
	})
	if err != nil {
		return err
	}
	if len(resp.DBClusters) != 1 {
		return errors.New("Found none or multiples matching this cluster name.")
	}


	var vpcSecurityGroupIds []*string = make([]*string, 0)
	for _, group := range resp.DBClusters[0].VpcSecurityGroups {
		vpcSecurityGroupIds = append(vpcSecurityGroupIds, group.VpcSecurityGroupId)
	}

	for _, member := range resp.DBClusters[0].DBClusterMembers {
		_, err := provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
			ApplyImmediately: 			aws.Bool(true),
			DBInstanceIdentifier: 		member.DBInstanceIdentifier, 
			NewDBInstanceIdentifier: 	aws.String(*member.DBInstanceIdentifier + renamedSuffix),
		})
		if err != nil {
			glog.Errorf("Unable to rename db cluster member: %s because %s\n", *member.DBInstanceIdentifier, err.Error())
			return err
		}
	}

	// 2. Rename the db cluster
	_, err = provider.awssvc.ModifyDBCluster(&rds.ModifyDBClusterInput{
			ApplyImmediately: 			aws.Bool(true),
			DBClusterIdentifier: 		aws.String(dbInstance.Name), 
			NewDBClusterIdentifier: 	aws.String(dbInstance.Name + renamedSuffix),
	})
	if err != nil {
		glog.Errorf("Unable to rename db cluster because %s\n", err.Error())
		return err
	}

	// 3. Wait for the db instance to be available before requesting a retore.
	err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: 	aws.String(dbInstance.Name + renamedSuffix),
		MaxRecords:				aws.Int64(20),
	})
	if err != nil {
		glog.Errorf("Unable to wait for renamed db cluster: %s\n", err.Error())
		return err
	}
	t := time.NewTicker(time.Second * 15)
	<-t.C

	// 4. Restore the original db cluster
	_, err = provider.awssvc.RestoreDBClusterFromSnapshot(&rds.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier:			aws.String(dbInstance.Name),
		SnapshotIdentifier:				aws.String(Id),
		DBSubnetGroupName:				settings.Cluster.DBSubnetGroupName,
		Engine:							settings.Cluster.Engine,
		VpcSecurityGroupIds:			vpcSecurityGroupIds,
	})
	if err != nil {
		glog.Errorf("Unable to restore db cluster because %s\n", err.Error())
		return err
	}

	// 5. Recreate the cluster members
	for _, member := range resp.DBClusters[0].DBClusterMembers {
		settings.Instance.DBInstanceIdentifier 	= member.DBInstanceIdentifier
		settings.Instance.DBClusterIdentifier 	= aws.String(dbInstance.Name)
		_, err = provider.awsInstanceProvider.ProvisionWithSettings(*member.DBInstanceIdentifier, dbInstance.Plan, &settings.Instance)
		if err != nil {
			glog.Errorf("Unable to create db cluster instance because %s\n", err.Error())
			return err
		}
	}

	// 6. Delete the members of the renamed cluster
	var dbPrimaryIdentifier *string = nil
	for _, member := range resp.DBClusters[0].DBClusterMembers {
		if member.IsClusterWriter != nil && *member.IsClusterWriter == true {
			tmpstr := *member.DBInstanceIdentifier + renamedSuffix
			dbPrimaryIdentifier = &tmpstr
		} else {
			_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
				DBInstanceIdentifier:      aws.String(*member.DBInstanceIdentifier + renamedSuffix),
				SkipFinalSnapshot:         aws.Bool(true),
			})
			if err != nil {
				glog.Errorf("Unable to delete renamed db cluster member %s because %s\n", *member.DBInstanceIdentifier, err.Error())
				return err
			}
		}
	}
	if dbPrimaryIdentifier == nil {
		return errors.New("Unable to find primary database identifier")
	}
	_, err = provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:      dbPrimaryIdentifier,
		SkipFinalSnapshot:         aws.Bool(true),
	})
	if err != nil {
		glog.Errorf("Unable to delete db renamed primary member: %s because %s\n", *dbPrimaryIdentifier, err.Error())
		return err
	}
	_, err = provider.awssvc.DeleteDBCluster(&rds.DeleteDBClusterInput{
		DBClusterIdentifier:      	aws.String(dbInstance.Name + renamedSuffix),
		SkipFinalSnapshot:			aws.Bool(true),
	})
	if err != nil {
		fmt.Printf("Unable to clean up database cluster that should be removed after restoring (DeleteDBInstance): %s\n", dbInstance.Name + renamedSuffix)
	}
	return err
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

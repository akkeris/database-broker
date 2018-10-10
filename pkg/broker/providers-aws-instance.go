package broker

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"os"
	"strconv"
	"strings"
	"time"
	"fmt"
)

type AWSInstanceProvider struct {
	Provider
	awssvc              *rds.RDS
	namePrefix          string
	awsVpcSecurityGroup string
	instanceCache 		map[string]*DbInstance
}

func NewAWSInstanceProvider(namePrefix string) (*AWSInstanceProvider, error) {
	if os.Getenv("AWS_REGION") == "" {
		return nil, errors.New("Unable to find AWS_REGION environment variable.")
	}
	if os.Getenv("AWS_VPC_SECURITY_GROUPS") == "" {
		return nil, errors.New("Unable to find AWS_VPC_SECURITY_GROUPS environment variable.")
	}
	t := time.NewTicker(time.Second * 30)
	awsInstanceProvider := &AWSInstanceProvider{
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*DbInstance),
		awsVpcSecurityGroup: os.Getenv("AWS_VPC_SECURITY_GROUPS"),
		awssvc:              rds.New(session.New(&aws.Config{Region: aws.String(os.Getenv("AWS_REGION"))})),
	}
	go (func() {
		for {
			awsInstanceProvider.instanceCache = make(map[string]*DbInstance)
			<-t.C
		}
	})()
	return awsInstanceProvider, nil
}

func (provider AWSInstanceProvider) GetInstance(name string, plan *ProviderPlan) (*DbInstance, error) {
	if provider.instanceCache[name + plan.ID] != nil {
		return provider.instanceCache[name + plan.ID], nil
	}
	resp, err := provider.awssvc.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(name),
		MaxRecords:           aws.Int64(20),
	})
	if err != nil {
		return nil, err
	}
	var endpoint = ""
	if resp.DBInstances[0].Endpoint != nil && resp.DBInstances[0].Endpoint.Port != nil && resp.DBInstances[0].Endpoint.Address != nil {
		endpoint = *resp.DBInstances[0].Endpoint.Address + ":" + strconv.FormatInt(*resp.DBInstances[0].Endpoint.Port, 10) + "/" + name
	}
	provider.instanceCache[name + plan.ID] = &DbInstance{
		Id:            "", // providers should not store this.
		ProviderId:    *resp.DBInstances[0].DBInstanceArn,
		Name:          name,
		Plan:          plan,
		Username:      "", // providers should not store this.
		Password:      "", // providers should not store this.
		Endpoint:      endpoint,
		Status:        *resp.DBInstances[0].DBInstanceStatus,
		Ready:         IsReady(*resp.DBInstances[0].DBInstanceStatus),
		Engine:        *resp.DBInstances[0].Engine,
		EngineVersion: *resp.DBInstances[0].EngineVersion,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name + plan.ID], nil
}

func (provider AWSInstanceProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *rds.CreateDBInstanceInput) (*DbInstance, error) {
	resp, err := provider.awssvc.CreateDBInstance(settings)
	if err != nil {
		return nil, err
	}

	var endpoint = ""
	if resp.DBInstance.Endpoint != nil && resp.DBInstance.Endpoint.Port != nil && resp.DBInstance.Endpoint.Address != nil {
		endpoint = *resp.DBInstance.Endpoint.Address + ":" + strconv.FormatInt(*resp.DBInstance.Endpoint.Port, 10) + "/" + *settings.DBName
	}

	return &DbInstance{
		Id:            Id,
		Name:          *resp.DBInstance.DBName,
		ProviderId:    *resp.DBInstance.DBInstanceArn,
		Plan:          plan,
		Username:      *resp.DBInstance.MasterUsername,
		Password:      "",
		Endpoint:      endpoint,
		Status:        *resp.DBInstance.DBInstanceStatus,
		Ready:         IsReady(*resp.DBInstance.DBInstanceStatus),
		Engine:        *resp.DBInstance.Engine,
		EngineVersion: *resp.DBInstance.EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}


func (provider AWSInstanceProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	settings.DBName = aws.String(strings.ToLower(provider.namePrefix + RandomString(8)))
	settings.DBInstanceIdentifier = settings.DBName
	settings.MasterUsername = aws.String(strings.ToLower("u" + RandomString(8)))
	settings.MasterUserPassword = aws.String(RandomString(16))
	settings.Tags = []*rds.Tag{{Key: aws.String("BillingCode"), Value: aws.String(Owner)}}
	settings.VpcSecurityGroupIds = []*string{aws.String(provider.awsVpcSecurityGroup)}

	dbInstance, err := provider.ProvisionWithSettings(Id, plan, &settings)
	if err != nil {
		return nil, err
	}
	dbInstance.Password = *settings.MasterUserPassword
	return dbInstance, nil
}

func (provider AWSInstanceProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name + "-ro"),
		SkipFinalSnapshot:    aws.Bool(!takeSnapshot),
	})
	_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:      aws.String(dbInstance.Name),
		FinalDBSnapshotIdentifier: aws.String(dbInstance.Name + "-final"),
		SkipFinalSnapshot:         aws.Bool(!takeSnapshot),
	})
	return err
}


func (provider AWSInstanceProvider) ModifyWithSettings(dbInstance *DbInstance, plan *ProviderPlan, settings *rds.CreateDBInstanceInput) (*DbInstance, error) {
	resp, err := provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
		AllocatedStorage:        settings.AllocatedStorage,
		AutoMinorVersionUpgrade: settings.AutoMinorVersionUpgrade,
		ApplyImmediately:        aws.Bool(true),
		DBInstanceClass:         settings.DBInstanceClass,
		DBInstanceIdentifier:    aws.String(dbInstance.Name),
		EngineVersion:           settings.EngineVersion,
		MultiAZ:                 settings.MultiAZ,
		PubliclyAccessible:      settings.PubliclyAccessible,
		CopyTagsToSnapshot:      settings.CopyTagsToSnapshot,
		BackupRetentionPeriod:   settings.BackupRetentionPeriod,
		DBParameterGroupName:    settings.DBParameterGroupName,
		DBSubnetGroupName:       settings.DBSubnetGroupName,
		StorageType:             settings.StorageType,
		Iops:                    settings.Iops,
	})

	if err != nil {
		return nil, err
	}

	var endpoint = dbInstance.Endpoint
	if resp.DBInstance.Endpoint != nil && resp.DBInstance.Endpoint.Port != nil && resp.DBInstance.Endpoint.Address != nil {
		endpoint = *resp.DBInstance.Endpoint.Address + ":" + strconv.FormatInt(*resp.DBInstance.Endpoint.Port, 10) + "/" + dbInstance.Name
	}

	// TODO: What about replicas?

	return &DbInstance{
		Id:            dbInstance.Id,
		Name:          dbInstance.Name,
		ProviderId:    *resp.DBInstance.DBInstanceArn,
		Plan:          plan,
		Username:      *resp.DBInstance.MasterUsername,
		Password:      dbInstance.Password,
		Endpoint:      endpoint,
		Status:        *resp.DBInstance.DBInstanceStatus,
		Ready:         IsReady(*resp.DBInstance.DBInstanceStatus),
		Engine:        *resp.DBInstance.Engine,
		EngineVersion: *resp.DBInstance.EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider AWSInstanceProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	if dbInstance.Status != "available" {
		return nil, errors.New("Replicas cannot be created for databases being created, under maintenance or destroyed.")
	}

	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	return provider.ModifyWithSettings(dbInstance, plan, &settings)
}

func (provider AWSInstanceProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	// TODO: what abouut read replica?
	// TODO: Support multiple values of the same tag name, comma delimit them.
	_, err := provider.awssvc.AddTagsToResource(&rds.AddTagsToResourceInput{
		ResourceName: aws.String(dbInstance.ProviderId),
		Tags: []*rds.Tag{
			{
				Key:   aws.String(Name),
				Value: aws.String(Value),
			},
		},
	})
	return err
}

func (provider AWSInstanceProvider) Untag(dbInstance *DbInstance, Name string) error {
	// TODO: what abouut read replica?
	// TODO: Support multiple values of the same tag name, comma delimit them.
	_, err := provider.awssvc.RemoveTagsFromResource(&rds.RemoveTagsFromResourceInput{
		ResourceName: aws.String(dbInstance.ProviderId),
		TagKeys: []*string{
			aws.String(Name),
		},
	})
	return err
}

func (provider AWSInstanceProvider) GetBackup(dbInstance *DbInstance, Id string) (DatabaseBackupSpec, error) {
	snapshots, err := provider.awssvc.DescribeDBSnapshots(&rds.DescribeDBSnapshotsInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		DBSnapshotIdentifier: aws.String(Id),
	})
	if err != nil {
		return DatabaseBackupSpec{}, err
	}
	if len(snapshots.DBSnapshots) != 1 {
		return DatabaseBackupSpec{}, errors.New("Not found")
	}

	created := time.Now().UTC().Format(time.RFC3339)
	if snapshots.DBSnapshots[0].SnapshotCreateTime != nil {
		created = snapshots.DBSnapshots[0].SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return DatabaseBackupSpec{
		Database: DatabaseSpec{
			Name: dbInstance.Name,
		},
		Id:       snapshots.DBSnapshots[0].DBSnapshotIdentifier,
		Progress: snapshots.DBSnapshots[0].PercentProgress,
		Status:   snapshots.DBSnapshots[0].Status,
		Created:  created,
	}, nil
}

func (provider AWSInstanceProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	snapshots, err := provider.awssvc.DescribeDBSnapshots(&rds.DescribeDBSnapshotsInput{DBInstanceIdentifier: aws.String(dbInstance.Name)})
	if err != nil {
		return []DatabaseBackupSpec{}, err
	}
	out := make([]DatabaseBackupSpec, 0)
	for _, snapshot := range snapshots.DBSnapshots {
		created := time.Now().UTC().Format(time.RFC3339)
		if snapshot.SnapshotCreateTime != nil {
			created = snapshot.SnapshotCreateTime.UTC().Format(time.RFC3339)
		}
		out = append(out, DatabaseBackupSpec{
			Database: DatabaseSpec{
				Name: dbInstance.Name,
			},
			Id:       snapshot.DBSnapshotIdentifier,
			Progress: snapshot.PercentProgress,
			Status:   snapshot.Status,
			Created:  created,
		})
	}
	return out, nil
}

func (provider AWSInstanceProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	if !dbInstance.Ready {
		return DatabaseBackupSpec{}, errors.New("Cannot create read only user on database that is unavailable.")
	}
	snapshot_name := (dbInstance.Name + "-manual-" + RandomString(10))
	snapshot, err := provider.awssvc.CreateDBSnapshot(&rds.CreateDBSnapshotInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		DBSnapshotIdentifier: aws.String(snapshot_name),
	})
	if err != nil {
		return DatabaseBackupSpec{}, err
	}
	created := time.Now().UTC().Format(time.RFC3339)
	if snapshot.DBSnapshot.SnapshotCreateTime != nil {
		created = snapshot.DBSnapshot.SnapshotCreateTime.UTC().Format(time.RFC3339)
	}

	return DatabaseBackupSpec{
		Database: DatabaseSpec{
			Name: dbInstance.Name,
		},
		Id:       snapshot.DBSnapshot.DBSnapshotIdentifier,
		Progress: snapshot.DBSnapshot.PercentProgress,
		Status:   snapshot.DBSnapshot.Status,
		Created:  created,
	}, nil
}

func (provider AWSInstanceProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return err
	}

	if !dbInstance.Ready {
		return errors.New("Cannot restore backup on database that is unavailable.")
	}

	// For AWS, the best strategy for restoring (reliably) a database is to rename the existing db
	// then create from a snapshot the existing db, and then nuke the old one once finished.
	awsDbResp, err := provider.awssvc.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		MaxRecords:           aws.Int64(20),
	})
	if err != nil {
		return err
	}
	if len(awsDbResp.DBInstances) != 1 {
		return errors.New("Unable to find database to rebuild as none or multiple were returned")
	}
	var dbSecurityGroups []*string = make([]*string, 0)
	for _, group := range awsDbResp.DBInstances[0].VpcSecurityGroups {
		dbSecurityGroups = append(dbSecurityGroups, group.VpcSecurityGroupId)
	}

	renamedId := dbInstance.Name + "-restore-" + RandomString(5)

	_, err = provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
			ApplyImmediately: 			aws.Bool(true),
			DBInstanceIdentifier: 		aws.String(dbInstance.Name), 
			NewDBInstanceIdentifier: 	aws.String(renamedId),
	})
	if err != nil {
		return err
	}

	err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: 	aws.String(renamedId),
		MaxRecords:				aws.Int64(20),
	})
	if err != nil {
		return err
	}
	_, err = provider.awssvc.RestoreDBInstanceFromDBSnapshot(&rds.RestoreDBInstanceFromDBSnapshotInput{
		DBInstanceIdentifier:			aws.String(dbInstance.Name),
		DBSnapshotIdentifier:			aws.String(Id),
		DBSubnetGroupName:				settings.DBSubnetGroupName,
	})

	err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: 	aws.String(dbInstance.Name),
		MaxRecords:				aws.Int64(20),
	})
	if err != nil {
		return err
	}

	// The restored instance does not have the same security groups, nor is there a way 
	// of specifying the security groups when restoring the database on the previous call, 
	// so we have to modify the newly created restore.
	_, err = provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
		ApplyImmediately: 			aws.Bool(true),
		DBInstanceIdentifier: 		aws.String(dbInstance.Name), 
		VpcSecurityGroupIds:		dbSecurityGroups,
		DBParameterGroupName:		settings.DBParameterGroupName,
	})
	if err != nil {
		return err
	}

	go (func() {
		err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: 	aws.String(dbInstance.Name),
			MaxRecords:				aws.Int64(20),
		})
		if err != nil {
			fmt.Printf("Unable to clean up database that should be removed after restoring (WaitUntilDBInstanceAvailable): %s %s\n", renamedId, err.Error())
		}
		_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:      aws.String(renamedId),
			SkipFinalSnapshot:         aws.Bool(true),
		})
		if err != nil {
			fmt.Printf("Unable to clean up database that should be removed after restoring (DeleteDBInstance): %s %s\n", renamedId, err.Error())
		}
	})()
	return err
}

func (provider AWSInstanceProvider) Restart(dbInstance *DbInstance) error {
	// What about replica?
	if !dbInstance.Ready {
		return errors.New("Cannot restart a database that is unavailable.")
	}
	_, err := provider.awssvc.RebootDBInstance(&rds.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
	})
	return err
}

func (provider AWSInstanceProvider) ListLogs(dbInstance *DbInstance) ([]DatabaseLogs, error) {
	// What about replica?
	var fileLastWritten int64 = time.Now().AddDate(0, 0, -7).Unix()
	var maxRecords int64 = 100
	logs, err := provider.awssvc.DescribeDBLogFiles(&rds.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		FileLastWritten:      &fileLastWritten,
		MaxRecords:           &maxRecords,
	})
	if err != nil {
		return []DatabaseLogs{}, err
	}
	out := make([]DatabaseLogs, 0)
	for _, log := range logs.DescribeDBLogFiles {
		updated := time.Now().UTC().Format(time.RFC3339)
		if log.LastWritten != nil {
			updated = time.Unix(*log.LastWritten/1000, 0).UTC().Format(time.RFC3339)
		}
		out = append(out, DatabaseLogs{
			Name:    log.LogFileName,
			Size:    log.Size,
			Updated: updated,
		})
	}
	return out, nil
}

func (provider AWSInstanceProvider) GetLogs(dbInstance *DbInstance, path string) (string, error) {
	// What about replica?
	data, err := provider.awssvc.DownloadDBLogFilePortion(&rds.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: &dbInstance.Name,
		LogFileName:          &path,
	})
	if err != nil {
		return "", err
	}
	if data.LogFileData == nil {
		return "", nil
	} else {
		return *data.LogFileData, nil
	}
}

func (provider AWSInstanceProvider) CreateReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	// TODO: what about tags set?
	if dbInstance.Status != "available" {
		return nil, errors.New("Replicas cannot be created for databases being created, under maintenance or destroyed.")
	}
	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	rdsInstance := rds.CreateDBInstanceReadReplicaInput{
		DBInstanceClass:             settings.DBInstanceClass,
		SourceDBInstanceIdentifier:  aws.String(dbInstance.Name),
		DBInstanceIdentifier:        aws.String(dbInstance.Name + "-ro"),
		AutoMinorVersionUpgrade:     settings.AutoMinorVersionUpgrade,
		MultiAZ:                     settings.MultiAZ,
		PubliclyAccessible:          settings.PubliclyAccessible,
		Port:                        settings.Port,
		CopyTagsToSnapshot:          settings.CopyTagsToSnapshot,
		KmsKeyId:                    settings.KmsKeyId,
		DBSubnetGroupName:           settings.DBSubnetGroupName,
		EnablePerformanceInsights:   settings.EnablePerformanceInsights,
		PerformanceInsightsKMSKeyId: settings.KmsKeyId,
		StorageType:                 settings.StorageType,
		Iops:                        settings.Iops,
		Tags: []*rds.Tag{
			{
				Key:   aws.String("Name"),
				Value: aws.String(dbInstance.Name),
			},
		},
	}

	resp, err := provider.awssvc.CreateDBInstanceReadReplica(&rdsInstance)
	if err != nil {
		return nil, err
	}

	var endpoint = ""
	if resp.DBInstance.Endpoint != nil && resp.DBInstance.Endpoint.Port != nil && resp.DBInstance.Endpoint.Address != nil {
		endpoint = *resp.DBInstance.Endpoint.Address + ":" + strconv.FormatInt(*resp.DBInstance.Endpoint.Port, 10) + "/" + dbInstance.Name
	}

	return &DbInstance{
		Id:            dbInstance.Name + "-ro",
		Name:          dbInstance.Name,
		ProviderId:    *resp.DBInstance.DBInstanceArn,
		Plan:          dbInstance.Plan,
		Username:      *resp.DBInstance.MasterUsername,
		Password:      dbInstance.Password,
		Endpoint:      endpoint,
		Status:        *resp.DBInstance.DBInstanceStatus,
		Ready:         IsReady(*resp.DBInstance.DBInstanceStatus),
		Engine:        *resp.DBInstance.Engine,
		EngineVersion: *resp.DBInstance.EngineVersion,
		Scheme:        dbInstance.Scheme,
	}, nil
}

func (provider AWSInstanceProvider) GetReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	rrDbInstance, err := provider.GetInstance(dbInstance.Name+"-ro", dbInstance.Plan)
	if err != nil {
		return nil, err
	}
	rrDbInstance.Username = dbInstance.Username
	rrDbInstance.Password = dbInstance.Password
	return rrDbInstance, nil
}

func (provider AWSInstanceProvider) DeleteReadReplica(dbInstance *DbInstance) error {
	_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name+"-ro"),
		SkipFinalSnapshot:    aws.Bool(false),
	})
	return err
}

func (provider AWSInstanceProvider) CreateReadOnlyUser(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	if !dbInstance.Ready {
		return DatabaseUrlSpec{}, errors.New("Cannot create user on database that is unavailable.")
	}
	return CreatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint)
}

func (provider AWSInstanceProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	if !dbInstance.Ready {
		return errors.New("Cannot delete user on database that is unavailable.")
	}
	return DeletePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint, role)
}

func (provider AWSInstanceProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	if !dbInstance.Ready {
		return DatabaseUrlSpec{}, errors.New("Cannot rotate password on database that is unavailable.")
	}
	return RotatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint, role)
}
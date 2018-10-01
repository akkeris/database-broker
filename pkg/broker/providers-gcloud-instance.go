package broker

import (
	"encoding/json"
	"errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sqladmin/v1beta4"
	"os"
	"strings"
	"time"
)

type GCloudInstanceProvider struct {
	Provider
	ctx					context.Context
	svc					*sqladmin.Service
	projectId			string
	region				string
	namePrefix          string
	instanceCache 		map[string]*DbInstance
}

func NewGCloudInstanceProvider(namePrefix string) (*GCloudInstanceProvider, error) {
	if os.Getenv("PROJECT_ID") == "" {
		return nil, errors.New("Unable to find PROJECT_ID environment variable.")
	}
	if os.Getenv("REGION") == "" {
		return nil, errors.New("Unable to find REGION environment variable.")
	}
	
	ctx := context.Background() 
	c, err := google.DefaultClient(ctx, sqladmin.CloudPlatformScope)
	if err != nil {
		return nil, err
	}

	svc, err := sqladmin.New(c)

	t := time.NewTicker(time.Second * 30)
	GCloudInstanceProvider := &GCloudInstanceProvider{
		ctx:			 	 ctx,
		projectId:			 os.Getenv("PROJECT_ID"),
		region:			 	 os.Getenv("REGION"),
		namePrefix:          namePrefix,
		instanceCache:		 make(map[string]*DbInstance),
		svc:              	 svc,
	}
	go (func() {
		for {
			GCloudInstanceProvider.instanceCache = make(map[string]*DbInstance)
			<-t.C
		}
	})()
	return GCloudInstanceProvider, nil
}

func (provider GCloudInstanceProvider) GetInstance(name string, plan *ProviderPlan) (*DbInstance, error) {
	if provider.instanceCache[name + plan.ID] != nil {
		return provider.instanceCache[name + plan.ID], nil
	}
	
	svc := sqladmin.NewInstancesService(provider.svc)
	resp, err := svc.Get(provider.projectId, name).Do()

	
	if err != nil {
		return nil, err
	}

	var endpoint = ""
	// ConnectionName, IpAddresses[...].IpAddress ... resp.IpAddresses[0] + ":" + strconv.FormatInt(*resp.DBInstances[0].Endpoint.Port, 10) + "/" + name

	var dbEngine = "postgres"
	var dbEngineVersion = "9.6"
	if resp.DatabaseVersion == "MYSQL_5_7" || resp.DatabaseVersion == "MYSQL_5_6" {
		dbEngine = "mysql"
		dbEngineVersion = "5.6"
		if resp.DatabaseVersion == "MYSQL_5_7" {
			dbEngineVersion = "5.7"
		}
	}
	provider.instanceCache[name + plan.ID] = &DbInstance{
		Id:            "", // providers should not store this.
		ProviderId:    resp.Name,
		Name:          name,
		Plan:          plan,
		Username:      "", // providers should not store this.
		Password:      "", // providers should not store this.
		Endpoint:      endpoint,
		Status:        resp.State,
		Ready:         IsReady(resp.State),
		Engine:        dbEngine,
		EngineVersion: dbEngineVersion,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name + plan.ID], nil
}

func (provider GCloudInstanceProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *sqladmin.DatabaseInstance, user *sqladmin.User) (*DbInstance, error) {
	svc := sqladmin.NewInstancesService(provider.svc)
	_, err := svc.Insert(provider.projectId, settings).Do()
	if err != nil {
		return nil, err
	}
	resp, err := svc.Get(provider.projectId, settings.Name).Do()
	if err != nil {
		return nil, err
	}

	usersService := sqladmin.NewUsersService(provider.svc)
	_, err = usersService.Insert(provider.projectId, settings.Name, user).Do()
	if err != nil {
		return nil, err
	}

	var endpoint = "" // TODO

	var dbEngine = "postgres"
	var dbEngineVersion = "9.6"
	if resp.DatabaseVersion == "MYSQL_5_7" || resp.DatabaseVersion == "MYSQL_5_6" {
		dbEngine = "mysql"
		dbEngineVersion = "5.6"
		if resp.DatabaseVersion == "MYSQL_5_7" {
			dbEngineVersion = "5.7"
		}
	}

	return &DbInstance{
		Id:            Id,
		Name:          resp.Name,
		ProviderId:    resp.Name,
		Plan:          plan,
		Username:      user.Name,
		Password:      user.Password,
		Endpoint:      endpoint,
		Status:        resp.State,
		Ready:         IsReady(resp.State),
		Engine:        dbEngine,
		EngineVersion: dbEngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}


func (provider GCloudInstanceProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings sqladmin.DatabaseInstance 
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	settings.Name = strings.ToLower(provider.namePrefix + RandomString(8))
	settings.InstanceType = "CLOUD_SQL_INSTANCE"
	settings.Project = provider.projectId
	settings.Region = provider.region

	var user sqladmin.User
	user.Name = strings.ToLower("u" + RandomString(8))
	user.Password = RandomString(16)

	return provider.ProvisionWithSettings(Id, plan, &settings, &user)
}

func (provider GCloudInstanceProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	/*provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name + "-ro"),
		SkipFinalSnapshot:    aws.Bool(!takeSnapshot),
	})
	_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:      aws.String(dbInstance.Name),
		FinalDBSnapshotIdentifier: aws.String(dbInstance.Name + "-final"),
		SkipFinalSnapshot:         aws.Bool(!takeSnapshot),
	})
	return err*/
	return errors.New("unimplemented")
}


func (provider GCloudInstanceProvider) ModifyWithSettings(dbInstance *DbInstance, plan *ProviderPlan, settings *sqladmin.DatabaseInstance, user *sqladmin.User) (*DbInstance, error) {
	/*resp, err := provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
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
	}, nil*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	/*if dbInstance.Status != "available" {
		return nil, errors.New("Replicas cannot be created for databases being created, under maintenance or destroyed.")
	}

	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	return provider.ModifyWithSettings(dbInstance, plan, &settings)*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	/*	
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
	return err*/
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) Untag(dbInstance *DbInstance, Name string) error {
	/*
	// TODO: what abouut read replica?
	// TODO: Support multiple values of the same tag name, comma delimit them.
	_, err := provider.awssvc.RemoveTagsFromResource(&rds.RemoveTagsFromResourceInput{
		ResourceName: aws.String(dbInstance.ProviderId),
		TagKeys: []*string{
			aws.String(Name),
		},
	})
	return err*/
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) GetBackup(dbInstance *DbInstance, Id string) (DatabaseBackupSpec, error) {
	/*snapshots, err := provider.awssvc.DescribeDBSnapshots(&rds.DescribeDBSnapshotsInput{
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
	}, nil*/
	return DatabaseBackupSpec{}, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) ListBackups(dbInstance *DbInstance) ([]DatabaseBackupSpec, error) {
	/*snapshots, err := provider.awssvc.DescribeDBSnapshots(&rds.DescribeDBSnapshotsInput{DBInstanceIdentifier: aws.String(dbInstance.Name)})
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
	return out, nil*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) CreateBackup(dbInstance *DbInstance) (DatabaseBackupSpec, error) {
	/*snapshot_name := (dbInstance.Name + "-manual-" + RandomString(10))
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
	}, nil*/
	return DatabaseBackupSpec{}, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) RestoreBackup(dbInstance *DbInstance, Id string) error {
	/*_, err := provider.awssvc.RestoreDBInstanceFromDBSnapshot(&rds.RestoreDBInstanceFromDBSnapshotInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		DBSnapshotIdentifier: aws.String(Id),
	})
	return err*/
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) Restart(dbInstance *DbInstance) error {
	/*
	// What about replica?
	_, err := provider.awssvc.RebootDBInstance(&rds.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
	})
	return err*/
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) ListLogs(dbInstance *DbInstance) ([]DatabaseLogs, error) {
	/*
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
	return out, nil*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) GetLogs(dbInstance *DbInstance, path string) (string, error) {
	/*
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
	}*/
	return "", errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) CreateReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	/*
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
	}, nil*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) GetReadReplica(dbInstance *DbInstance) (*DbInstance, error) {
	/*
	rrDbInstance, err := provider.GetInstance(dbInstance.Name+"-ro", dbInstance.Plan)
	if err != nil {
		return nil, err
	}
	rrDbInstance.Username = dbInstance.Username
	rrDbInstance.Password = dbInstance.Password
	return rrDbInstance, nil*/
	return nil, errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) DeleteReadReplica(dbInstance *DbInstance) error {
	/*_, err := provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name+"-ro"),
		SkipFinalSnapshot:    aws.Bool(false),
	})
	return err*/
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) CreateReadOnlyUser(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	return CreatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name)
}

func (provider GCloudInstanceProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	return DeletePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name, role)
}

func (provider GCloudInstanceProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	return RotatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name, role)
}

package broker

import (
	"encoding/json"
	"errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sqladmin/v1beta4"
	"github.com/golang/glog"
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
	if os.Getenv("GCLOUD_PROJECT_ID") == "" {
		return nil, errors.New("Unable to find GCLOUD_PROJECT_ID environment variable.")
	}
	if os.Getenv("GCLOUD_REGION") == "" {
		return nil, errors.New("Unable to find GCLOUD_REGION environment variable.")
	}
	
	ctx := context.Background()

	// Although undocumented, I've found that if i don't explicitly try and find the default
	// credentials first (even if i don't use them), i'll get an unauthorized error. Maybe
	// using this incorrectly.
	_, err := google.FindDefaultCredentials(ctx, sqladmin.SqlserviceAdminScope)
	if err != nil {
		return nil, err
	}
	client, err := google.DefaultClient(ctx, sqladmin.SqlserviceAdminScope)
	if err != nil {
		return nil, err
	}
	svc, err := sqladmin.New(client)
	if err != nil {
		return nil, err
	}

	t := time.NewTicker(time.Second * 30)
	GCloudInstanceProvider := &GCloudInstanceProvider{
		ctx:			 	 ctx,
		projectId:			 os.Getenv("GCLOUD_PROJECT_ID"),
		region:			 	 os.Getenv("GCLOUD_REGION"),
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

	var ipAddress string = ""
	for _, ip := range resp.IpAddresses {
		if ip.Type == "PRIMARY" {
			ipAddress = ip.IpAddress
		}
	}

	if len(resp.IpAddresses) == 0 || ipAddress == "" {
		return nil, errors.New("Unable to get instance ip address.")
	}

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
		Endpoint:      ipAddress + "/" + resp.Name,
		Status:        resp.State,
		Ready:         IsReady(resp.State),
		Engine:        dbEngine,
		EngineVersion: dbEngineVersion,
		Scheme:        plan.Scheme,
	}

	return provider.instanceCache[name + plan.ID], nil
}

func (provider GCloudInstanceProvider) PerformPostProvision(db *DbInstance) (*DbInstance, error) {
	usersService := sqladmin.NewUsersService(provider.svc)
	var user sqladmin.User = sqladmin.User{
		Instance:	db.Name,
		Kind:		"sql#user",
		Name:		db.Username,
		Password:	db.Password,
		Project:	provider.projectId,
	}
	if _, err := usersService.Insert(provider.projectId, db.Name, &user).Do(); err != nil {
		glog.Infof("GCloudInstanceProvider: PerformPostProvision: Failure to insert new user: %s\n", err.Error())
		return nil, err
	}
	return db, nil
}

func (provider GCloudInstanceProvider) ProvisionWithSettings(Id string, plan *ProviderPlan, settings *sqladmin.DatabaseInstance, user *sqladmin.User) (*DbInstance, error) {
	svc := sqladmin.NewInstancesService(provider.svc)
	_, err := svc.Insert(provider.projectId, settings).Do()
	if err != nil {
		return nil, err
	}
	resp, err := svc.Get(provider.projectId, settings.Name).Do()
	if err != nil {
		glog.Infof("GCloudInstanceProvider: ProvisionWithSettings: Failure to get database: %s\n", err.Error())
		return nil, err
	}
	
	dbVersionInfo := strings.Split(resp.DatabaseVersion, "_")
	dbEngine := strings.ToLower(dbVersionInfo[0])
	dbEngineVersion := strings.Join(dbVersionInfo[1:], ".")

	return &DbInstance{
		Id:            Id,
		Name:          resp.Name,
		ProviderId:    resp.Name,
		Plan:          plan,
		Username:      user.Name,
		Password:      user.Password,
		Endpoint:      "", // This is not immediately available.
		Status:        resp.State,
		Ready:         IsReady(resp.State),
		Engine:        dbEngine,
		EngineVersion: dbEngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider GCloudInstanceProvider) Provision(Id string, plan *ProviderPlan, Owner string) (*DbInstance, error) {
	var settings sqladmin.Settings 
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	var dbInstanceGcloud sqladmin.DatabaseInstance
	dbInstanceGcloud.Settings = &settings
	dbInstanceGcloud.BackendType = "SECOND_GEN"
	if plan.basePlan.Metadata["engine"] == nil {
		return nil, errors.New("Cannot find the engine type and engine version.")
	}
	var engine map[string]string = plan.basePlan.Metadata["engine"].(map[string]string)
	// Assemble (or rather, infer) the engine/version from metadata on the plan.
	vArray := strings.Split(engine["version"], ".")
	maxLen := len(vArray)
	if maxLen > 2 {
		maxLen = 2
	}
	dbInstanceGcloud.DatabaseVersion = strings.ToUpper(engine["type"]) + "_" + strings.Join(vArray[0:maxLen], "_")
	dbInstanceGcloud.Name = strings.ToLower(provider.namePrefix + RandomString(8))
	dbInstanceGcloud.InstanceType = "CLOUD_SQL_INSTANCE"
	dbInstanceGcloud.Project = provider.projectId
	dbInstanceGcloud.Region = provider.region
	if dbInstanceGcloud.Settings.UserLabels == nil {
		dbInstanceGcloud.Settings.UserLabels = make(map[string]string)
	}
	if Owner != "" {
		dbInstanceGcloud.Settings.UserLabels["billing-code"] = strings.ToLower(Owner)
	} else {
		dbInstanceGcloud.Settings.UserLabels["billing-code"] = "unknown"
	}

	var user sqladmin.User
	user.Name = strings.ToLower("u" + RandomString(8))
	user.Password = RandomString(16)

	return provider.ProvisionWithSettings(Id, plan, &dbInstanceGcloud, &user)
}

func (provider GCloudInstanceProvider) Deprovision(dbInstance *DbInstance, takeSnapshot bool) error {
	// TODO: snapshot?
	svc := sqladmin.NewInstancesService(provider.svc)
	_, err := svc.Delete(provider.projectId, dbInstance.Name).Do()
	return err
}

func (provider GCloudInstanceProvider) ModifyWithSettings(dbInstance *DbInstance, plan *ProviderPlan, settings *sqladmin.Settings) (*DbInstance, error) {
	svc := sqladmin.NewInstancesService(provider.svc)

	resp, err := svc.Get(provider.projectId, dbInstance.Name).Do()
	if err != nil {
		return nil, err
	}
	resp.Settings = settings
	_, err = svc.Update(provider.projectId, dbInstance.Name, resp).Do()
	if err != nil {
		return nil, err
	}
	resp, err = svc.Get(provider.projectId, resp.Name).Do()
	if err != nil {
		return nil, err
	}
	if len(resp.IpAddresses) == 0 {
		return nil, errors.New("Unable to get instance ip address.")
	}

	var endpoint = resp.IpAddresses[0].IpAddress + "/" + resp.Name
	dbVersionInfo := strings.Split(resp.DatabaseVersion, "_")
	dbEngine := strings.ToLower(dbVersionInfo[0])
	dbEngineVersion := strings.Join(dbVersionInfo[1:], ".")

	return &DbInstance{
		Id:            dbInstance.Id,
		Name:          dbInstance.Name,
		ProviderId:    dbInstance.Name,
		Plan:          plan,
		Username:      dbInstance.Name,
		Password:      dbInstance.Password,
		Endpoint:      endpoint,
		Status:        resp.State,
		Ready:         IsReady(resp.State),
		Engine:        dbEngine,
		EngineVersion: dbEngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider GCloudInstanceProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	glog.Infof("Database: %s modifying settings...\n", dbInstance.Id)
	var settings sqladmin.Settings 
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}
	dbNew, err := provider.ModifyWithSettings(dbInstance, plan, &settings)
	glog.Infof("Database: %s modifications finished.\n", dbInstance.Id)
	return dbNew, err
}

func (provider GCloudInstanceProvider) Tag(dbInstance *DbInstance, Name string, Value string) error {
	
	return errors.New("unimplemented")
}

func (provider GCloudInstanceProvider) Untag(dbInstance *DbInstance, Name string) error {
	
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
	svc := sqladmin.NewInstancesService(provider.svc)
	_, err := svc.Restart(provider.projectId, dbInstance.Name).Do()
	return err
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
	if dbInstance.Status != "RUNNABLE" {
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
	if !dbInstance.Ready {
		return DatabaseUrlSpec{}, errors.New("Cannot rotate password on database that is unavailable.")
	}
	return CreatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name)
}

func (provider GCloudInstanceProvider) DeleteReadOnlyUser(dbInstance *DbInstance, role string) error {
	if !dbInstance.Ready {
		return errors.New("Cannot rotate password on database that is unavailable.")
	}
	return DeletePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name, role)
}

func (provider GCloudInstanceProvider) RotatePasswordReadOnlyUser(dbInstance *DbInstance, role string) (DatabaseUrlSpec, error) {
	if !dbInstance.Ready {
		return DatabaseUrlSpec{}, errors.New("Cannot rotate password on database that is unavailable.")
	}
	return RotatePostgresReadOnlyRole(dbInstance, dbInstance.Scheme + "://" + dbInstance.Username + ":" + dbInstance.Password + "@" + dbInstance.Endpoint + "/" + dbInstance.Name, role)
}

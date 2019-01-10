package broker

import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
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
	t := time.NewTicker(time.Second * 5)
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

func (provider AWSInstanceProvider) PerformPostProvision(db *DbInstance) (*DbInstance, error) {
	return db, nil
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
	var err error = nil
	if takeSnapshot {
		_, err = provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:      aws.String(dbInstance.Name),
			FinalDBSnapshotIdentifier: aws.String(dbInstance.Name + "-final"),
			SkipFinalSnapshot:         aws.Bool(false),
		})
	} else {
		_, err = provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
			DBInstanceIdentifier:      aws.String(dbInstance.Name),
			SkipFinalSnapshot:         aws.Bool(true),
		})
	}
	return err
}

func (provider AWSInstanceProvider) upgradePlan(dbInstance *DbInstance, proposed string) ([]string, error) {
	proposedVersion := strings.Split(proposed, ".")
	currentVersion  := strings.Split(dbInstance.EngineVersion, ".")
	
	if len(proposedVersion) == 0 {
		return nil, errors.New("No major version found.")
	}

	proposedMajor, err := strconv.Atoi(proposedVersion[0])
	if err != nil {
		return nil, err
	}
	proposedMinor := 1
	if len(proposedVersion) > 1 {
		proposedMinor, err = strconv.Atoi(proposedVersion[1])
		if err != nil {
			return nil, err
		}
	}
	proposedPatch := 1
	if len(proposedVersion) > 2 {
		proposedPatch, err = strconv.Atoi(proposedVersion[2])
		if err != nil {
			return nil, err
		}
	}

	var versionUpgradePlan []string
	for {
		currentMajor, err := strconv.Atoi(currentVersion[0])
		if err != nil {
			return nil, err
		}
		currentMinor := 1
		if len(currentVersion) > 1 {
			currentMinor, err = strconv.Atoi(currentVersion[1])
			if err != nil {
				return nil, err
			}
		}
		currentPatch := 1
		if len(currentVersion) > 2 {
			currentPatch, err = strconv.Atoi(currentVersion[2])
			if err != nil {
				return nil, err
			}
		}
		if  (currentMajor > proposedMajor) || 
			(currentMajor == proposedMajor && currentMinor > proposedMinor) || 
			(currentMajor == proposedMajor && currentMinor == proposedMinor && currentPatch >= proposedPatch) {
				break
		}

		devres, err := provider.awssvc.DescribeDBEngineVersions(&rds.DescribeDBEngineVersionsInput{
			MaxRecords:aws.Int64(100),
			Engine:aws.String(dbInstance.Engine),
			EngineVersion:aws.String(strings.Join(currentVersion, ".")),
		})
		if err != nil {
			return nil, err
		}

		// If there are no targets, c'est la vie. 
		if len(devres.DBEngineVersions) == 0 || len(devres.DBEngineVersions[0].ValidUpgradeTarget) == 0 {
			return nil, errors.New("Unable to find a valid upgrade path from " + dbInstance.EngineVersion + " to " + proposed)
		}
		
		// If the target does not have a version, c'est la vie
		if devres.DBEngineVersions[0].ValidUpgradeTarget[len(devres.DBEngineVersions[0].ValidUpgradeTarget) - 1].EngineVersion == nil {
			return nil, errors.New("Odd, Engine Version was null, unable to upgrade.")
		}

		// We may be stuck in a loop if we keep proposing ourself.
		nextMaxVersion := *devres.DBEngineVersions[0].ValidUpgradeTarget[len(devres.DBEngineVersions[0].ValidUpgradeTarget) - 1].EngineVersion
		if nextMaxVersion == strings.Join(currentVersion, ".") {
			return nil, errors.New("Odd, we keep getting ourselves as the next upgrade path.")
		}

		// See if an exact match to the proposed is in the target list, otherwise go for the maximum.
		currentVersion = strings.Split(nextMaxVersion, ".")
		for _, target := range devres.DBEngineVersions[0].ValidUpgradeTarget {
			if target.EngineVersion == nil {
				return nil, errors.New("Odd, Engine Version was null when searching available targets.")
			}
			if *target.EngineVersion == strings.Join(proposedVersion, ".") {
				currentVersion = strings.Split(*target.EngineVersion, ".")
			}
		}

		// See if the new proposed one already exists in the plan
		for _, v := range versionUpgradePlan {
			if v == strings.Join(currentVersion, ".") {
				return nil, errors.New("Odd, the proposed upgrade was already in the version upgrade plan.")
			}
		}

		// Append it as a next step
		versionUpgradePlan = append(versionUpgradePlan, strings.Join(currentVersion, "."))
	}

	if len(versionUpgradePlan) > 10 {
		return nil, errors.New("Unable to upgrade, too many steps were proposed in the upgrade plan.")
	}

	return versionUpgradePlan, nil
}

func (provider AWSInstanceProvider) UpgradeVersion(dbInstance *DbInstance, proposed string, settings *rds.CreateDBInstanceInput) (*DbInstance, error) {
	versions, err := provider.upgradePlan(dbInstance, proposed)
	if err != nil {
		return nil, err
	}

	// Begin the upgrades.
	for _, version := range versions {
		err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
			DBInstanceIdentifier: 	aws.String(dbInstance.Name),
			MaxRecords:				aws.Int64(20),
		})
		if err != nil {
			return nil, err
		}

		// We want to prefer the database parameter group that was specified by the 
		// configuration but during the upgrade process we may have to go through a
		// different parameter group (if non default) in order to to reach the target
		// parameter group of the plan. 

		devres, err := provider.awssvc.DescribeDBEngineVersions(&rds.DescribeDBEngineVersionsInput{
			MaxRecords:aws.Int64(100),
			Engine:aws.String(dbInstance.Engine),
			EngineVersion:aws.String(version),
		})
		if err != nil {
			return nil, err
		}
		if len(devres.DBEngineVersions) == 0 {
			return nil, errors.New("No valid db engine versions could be found for " + dbInstance.Engine + " " + version)
		}

		groups, err := provider.awssvc.DescribeDBParameterGroups(&rds.DescribeDBParameterGroupsInput{})
		if err != nil {
			return nil, err
		}

		var dbParameterGroup *string = nil
				
		// Use the preferred one if AWS says it's available, and if it was specified in the plan.
		if settings.DBParameterGroupName != nil && *settings.DBParameterGroupName != "" {
			for _, group := range groups.DBParameterGroups {
				if group.DBParameterGroupName != nil && *group.DBParameterGroupName == *settings.DBParameterGroupName {
					dbParameterGroup = settings.DBParameterGroupName
				} 
			}
		}

		// Next if we cant use the default specified one, pick the default parameter group based on
		// the versions parmaeter group family.
		if dbParameterGroup == nil {
			for _, group := range groups.DBParameterGroups {
				if group.DBParameterGroupName != nil && group.DBParameterGroupFamily != nil && devres.DBEngineVersions[0].DBParameterGroupFamily != nil && *group.DBParameterGroupFamily == *devres.DBEngineVersions[0].DBParameterGroupFamily && *group.DBParameterGroupName == ("default." + (*group.DBParameterGroupFamily)) {
					dbParameterGroup = group.DBParameterGroupName
				}
			}
		}

		// Finally, if nothing still matches, just pick one.
		if dbParameterGroup == nil {
			for _, group := range groups.DBParameterGroups {
				if group.DBParameterGroupFamily != nil && devres.DBEngineVersions[0].DBParameterGroupFamily != nil && *group.DBParameterGroupFamily == *devres.DBEngineVersions[0].DBParameterGroupFamily {
					dbParameterGroup = group.DBParameterGroupName
				}
			}
		}
		if dbParameterGroup != nil {
			glog.Infof("Database: %s upgrading to %s %s with %s\n", dbInstance.Id, dbInstance.Engine, dbInstance.EngineVersion, *dbParameterGroup)
		} else {
			glog.Infof("Database: %s upgrading to %s %s with no specified parameter group.\n", dbInstance.Id, dbInstance.Engine, dbInstance.EngineVersion)
		}
		_, err = provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
			AllowMajorVersionUpgrade:aws.Bool(true),
			EngineVersion:           aws.String(version),
			ApplyImmediately:        aws.Bool(true),
			DBInstanceIdentifier:    aws.String(dbInstance.Name),
			DBParameterGroupName:    dbParameterGroup,
		})
		if err != nil {
			return nil, err
		}
		dbInstance.EngineVersion = version
		glog.Infof("Database: %s upgraded to %s %s\n", dbInstance.Id, dbInstance.Engine, dbInstance.EngineVersion)
		tick := time.NewTicker(time.Second * 30)
		<-tick.C
	}
	err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: 	aws.String(dbInstance.Name),
		MaxRecords:				aws.Int64(20),
	})
	if err != nil {
		return nil, err
	}
	return dbInstance, nil
}

func (provider AWSInstanceProvider) ModifyWithSettings(dbInstance *DbInstance, plan *ProviderPlan, settings *rds.CreateDBInstanceInput) (*DbInstance, error) {
	dest, err := provider.awssvc.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(dbInstance.Name),
		MaxRecords:           aws.Int64(20),
	})
	if err != nil {
		return nil, err
	}
	if dest.DBInstances == nil || len(dest.DBInstances) != 1 {
		return nil, errors.New("Cannot find database to modify!")
	}
	glog.Infof("Database: %s modifying settings...\n", dbInstance.Id)
	resp, err := provider.awssvc.ModifyDBInstance(&rds.ModifyDBInstanceInput{
		AllocatedStorage:        settings.AllocatedStorage,
		AutoMinorVersionUpgrade: settings.AutoMinorVersionUpgrade,
		ApplyImmediately:        aws.Bool(true),
		DBInstanceClass:         settings.DBInstanceClass,
		DBInstanceIdentifier:    aws.String(dbInstance.Name),
		MultiAZ:                 settings.MultiAZ,
		PubliclyAccessible:      settings.PubliclyAccessible,
		CopyTagsToSnapshot:      settings.CopyTagsToSnapshot,
		BackupRetentionPeriod:   settings.BackupRetentionPeriod,
		StorageType:             settings.StorageType,
		Iops:                    settings.Iops,
	})
	if err != nil {
		return nil, err
	}

	tick := time.NewTicker(time.Second * 30)
	<-tick.C

	var endpoint = dbInstance.Endpoint
	if resp.DBInstance.Endpoint != nil && resp.DBInstance.Endpoint.Port != nil && resp.DBInstance.Endpoint.Address != nil {
		endpoint = *resp.DBInstance.Endpoint.Address + ":" + strconv.FormatInt(*resp.DBInstance.Endpoint.Port, 10) + "/" + dbInstance.Name
	}

	// TODO: What about replicas?

	// Upgrade the version seperately as this may be a lot of work.
	newDbInstance, err := provider.UpgradeVersion(dbInstance, *settings.EngineVersion, settings)
	if err != nil {
		return nil, err
	}

	dest, err = provider.awssvc.DescribeDBInstances(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(newDbInstance.Name),
		MaxRecords:           aws.Int64(20),
	})
	if err != nil {
		return nil, err
	}
	if dest.DBInstances == nil || len(dest.DBInstances) != 1 {
		return nil, errors.New("Cannot find database once modified!")
	}
	glog.Infof("Database: %s modifications finished.\n", dbInstance.Id)
	return &DbInstance{
		Id:            dbInstance.Id,
		Name:          dbInstance.Name,
		ProviderId:    *dest.DBInstances[0].DBInstanceArn,
		Plan:          plan,
		Username:      *dest.DBInstances[0].MasterUsername,
		Password:      dbInstance.Password,
		Endpoint:      endpoint,
		Status:        *dest.DBInstances[0].DBInstanceStatus,
		Ready:         IsReady(*dest.DBInstances[0].DBInstanceStatus),
		Engine:        *dest.DBInstances[0].Engine,
		EngineVersion: *dest.DBInstances[0].EngineVersion,
		Scheme:        plan.Scheme,
	}, nil
}

func (provider AWSInstanceProvider) Modify(dbInstance *DbInstance, plan *ProviderPlan) (*DbInstance, error) {
	if !CanBeModified(dbInstance.Status) {
		return nil, errors.New("Databases cannot be modifed during backups, upgrades or while maintenance is being performed.")
	}

	var settings rds.CreateDBInstanceInput
	if err := json.Unmarshal([]byte(plan.providerPrivateDetails), &settings); err != nil {
		return nil, err
	}

	var oldSettings rds.CreateDBInstanceInput 
	if err := json.Unmarshal([]byte(dbInstance.Plan.providerPrivateDetails), &oldSettings); err != nil {
		return nil, err
	}

	if oldSettings.AllocatedStorage != nil && settings.AllocatedStorage != nil {
		if *oldSettings.AllocatedStorage > *settings.AllocatedStorage {
			settings.AllocatedStorage = oldSettings.AllocatedStorage
		}
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

	err = provider.awssvc.WaitUntilDBInstanceAvailable(&rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: 	aws.String(dbInstance.Name),
		MaxRecords:				aws.Int64(20),
	})
	if err != nil {
		fmt.Printf("Unable to clean up database that should be removed after restoring (WaitUntilDBInstanceAvailable): %s %s\n", renamedId, err.Error())
	}
	_, err = provider.awssvc.DeleteDBInstance(&rds.DeleteDBInstanceInput{
		DBInstanceIdentifier:      aws.String(renamedId),
		SkipFinalSnapshot:         aws.Bool(true),
	})
	if err != nil {
		fmt.Printf("ERROR: Orphaned Database! Unable to clean up database that should be removed after restoring (DeleteDBInstance): %s %s\n", renamedId, err.Error())
	}
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

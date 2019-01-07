# Akkeris Database Broker

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9b3065a9f42d44618ed8e459032e5964)](https://www.codacy.com/app/Akkeris/database-broker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=akkeris/database-broker&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9b3065a9f42d44618ed8e459032e5964)](https://www.codacy.com/app/Akkeris/database-broker?utm_source=github.com&utm_medium=referral&utm_content=akkeris/database-broker&utm_campaign=Badge_Coverage)
[![CircleCI](https://circleci.com/gh/akkeris/database-broker.svg?style=svg)](https://circleci.com/gh/akkeris/database-broker)

A database broker for a variety of cloud providers and even on-prem db systems that implements the Open Service Broker 2.13 compliant database broker REST API.  Depending on provider it can provision postgres, mysql, aws aurora and hypothetically oracle and mssql (although not tested) databases. It can also be ran without Akkeris, but why would you? 

## Providers

The broker has support for the following providers

* AWS RDS Instances and Clusters
* Postgres Databases via Shared Tenant

## Features

* Create your own plans
* Upgrade plans
* Take backups, list and restore
* Database Read-Only Replicas
* Extra Database Accounts (read-only, read-write, create, remove, rotate password)
* Database Logs
* Restart
* Preprovisioning databases for speed
* Generate and issue KMS keys stored on fortanix

## Installing

First, set your settings, so to speak, although not required these installation instructions assume you're deploying to a dockerized environment.  You'll also need to provision (manually) a postgres database so the database broker can store plans, databases and other information for itself.  Once you have your settings, move on to the deploy step, then finally setup your plans.

### 1. Settings

Note almost all of these can be set via the command line as well.

**Required**

* `DATABASE_URL` - The postgres database to store its information on what databases its provisioned, this should be in the format of `postgres://user:password@host:port/database?sslmode=disable` or leave off sslmode=disable if ssl is supported.  This will auto create the schema if its unavailable.
* `NAME_PREFIX` - The prefix to use for all provisioned databases this should be short and help namespace databases created by the broker vs. other databases that may exist in the broker for other purposes. This is global to all of the providers configured.

**AWS Provider Specific**

* `AWS_REGION` - The AWS region to provision databases in, only one aws provider and region are supported by the database broker.
* `AWS_VPC_SECURITY_GROUPS` - The VPC security groups to automatically assign for all VPC instances, this overrides any plan settings and is recommended you set this in the environment.
* `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` to an IAM role that has full access to RDS in the `AWS_REGION` you specified above.

Note that you can get away with not setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and use EC2 IAM roles or hard coded credentials via the `~/.aws/credentials` file but these are not recommended!

**Shared Postgres Provider Specific**

There are no environment variables for shared postgres providers, although sensitive configuration can be set in the enivornment, see plans for more information. 

**Google Cloud Specific**

* `GOOGLE_JSON_TOKEN` - The JSON string that has the project id, token from oauth2 as the value.


**Optional**

* `PORT` - This defaults to 8443, setting this changes the default port number to listen to http (or https) traffic on
* `RETRY_WEBHOOKS` - (WORKER ONLY) whether outbound notifications about provisions or create bindings should be retried if they fail.  This by default is false, unless you trust or know the clients hitting this broker, leave this disabled.

### 2. Deployment

You can deploy the image `akkeris/database-broker:lastest` via docker with the environment or config var settings above. If you decide you're going to build this manually and run it you'll need see the Building section below. 

### 3. Plans

You can create new services and plans by provider by modifying the entries in its database. You can configure the plans and services how you wish.  By default the database broker will create 14 different postgres plans from AWS and a shared tenant system, feel free to update, delete and remove as needed. All the settings outside of provider private settings are for users benefit to help them make better choices, in Akkeris they also have additional meta data to know how or whether to certain capabilites can be performed on the addon (for example, attaching the addon to multiple apps) but are otherwise just presentation.

Open your favorite postgres client and connect to `$DATABASE_URL` above, ensure you've deployed the database broker before continuing. Each service and plan will be displayed to the user, the plans a provider specific and the column `provider_private_details` has different information depending on the provider for information on what should be in this column see the provider specific settings below. 

**AWS Instance Provider Specific Settings**

Setting the provider specific private settings in the plan is important to do carefully as these settings are whats ACTUALLY created. You can use `${ENV_VAR_NAME}` to fill in any portion of the provider settings with an environment variable. Setting a value to null will request to use the default value from AWS. The description, type and allowed values for each of these types can be found here: https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#CreateDBInstanceInput

Note: the following fields should not be set and will always be overridden (so do not set them in the settings) `Tags`, `DBInstanceIdentifier`, `DBName`, `MasterUserPassword`, `MasterUsername`, `Engine`, `EngineVersion` and `VpcSecurityGroupIds`.  The `VpcSecurityGroupIds` are tied to region and must be set via `AWS_VPC_SECURITY_GROUPS`. In addition `DBClusterIdentifier` should always be null.

***Sample AWS Instance Provider Specific Settings***

In this example 100 gb of storage and a `db.t2.medium` class server are provisioned. All other options use the default AWS settings provided.

```
{  
   "AllocatedStorage":100,
   "AutoMinorVersionUpgrade":null,
   "AvailabilityZone":null,
   "BackupRetentionPeriod":null,
   "CharacterSetName":null,
   "CopyTagsToSnapshot":null,
   "DBClusterIdentifier":null,
   "DBInstanceClass":"db.t2.medium",
   "DBInstanceIdentifier":null,
   "DBName":null,
   "DBParameterGroupName":null,
   "DBSecurityGroups":null,
   "DBSubnetGroupName":null,
   "Domain":null,
   "DomainIAMRoleName":null,
   "EnableCloudwatchLogsExports":null,
   "EnableIAMDatabaseAuthentication":null,
   "EnablePerformanceInsights":null,
   "Engine":null,
   "EngineVersion":null,
   "Iops":null,
   "KmsKeyId":null,
   "LicenseModel":null,
   "MasterUserPassword":null,
   "MasterUsername":null,
   "MonitoringInterval":null,
   "MonitoringRoleArn":null,
   "MultiAZ":null,
   "OptionGroupName":null,
   "PerformanceInsightsKMSKeyId":null,
   "PerformanceInsightsRetentionPeriod":null,
   "Port":null,
   "PreferredBackupWindow":null,
   "PreferredMaintenanceWindow":null,
   "ProcessorFeatures":null,
   "PromotionTier":null,
   "PubliclyAccessible":null,
   "StorageEncrypted":null,
   "StorageType":null,
   "Tags":null,
   "TdeCredentialArn":null,
   "TdeCredentialPassword":null,
   "Timezone":null,
   "VpcSecurityGroupIds":null
}
```

**AWS Cluster Specific Settings**

Similar to the AWS Instance specific settings these are the parameters used in calls to both CreateDBClusuter and CreateDBInstance subsequently.  See AWS Instance Specific Settings for more information on what fields are ignored or set automatically for the Instance portion.  For the cluster property (and portion) the fields `DBClusterIdentifier`, `DatabaseName`, `Engine`, `VpcSecurityGroupIds`, `MasterUserPassword`, `MasterUsername` and `Tags` are automatically overwritten, do not set these.  The VPC Security Group Ids are always set by the security groups defined in the environment. 

IMPORTANT: Becareful changing instance settings, most settings should be on the cluster property and are very rarely permitted on the instance settings.

***Sample AWS Cluster Specific Settings***

```
{  
   "Instance":{  
      "AllocatedStorage":null,
      "AutoMinorVersionUpgrade":null,
      "AvailabilityZone":null,
      "BackupRetentionPeriod":null,
      "CharacterSetName":null,
      "CopyTagsToSnapshot":null,
      "DBClusterIdentifier":null,
      "DBInstanceClass":null,
      "DBInstanceIdentifier":null,
      "DBName":null,
      "DBParameterGroupName":null,
      "DBSecurityGroups":null,
      "DBSubnetGroupName":null,
      "Domain":null,
      "DomainIAMRoleName":null,
      "EnableCloudwatchLogsExports":null,
      "EnableIAMDatabaseAuthentication":null,
      "EnablePerformanceInsights":null,
      "Engine":null,
      "EngineVersion":null,
      "Iops":null,
      "KmsKeyId":null,
      "LicenseModel":null,
      "MasterUserPassword":null,
      "MasterUsername":null,
      "MonitoringInterval":null,
      "MonitoringRoleArn":null,
      "MultiAZ":null,
      "OptionGroupName":null,
      "PerformanceInsightsKMSKeyId":null,
      "PerformanceInsightsRetentionPeriod":null,
      "Port":null,
      "PreferredBackupWindow":null,
      "PreferredMaintenanceWindow":null,
      "ProcessorFeatures":null,
      "PromotionTier":null,
      "PubliclyAccessible":null,
      "StorageEncrypted":null,
      "StorageType":null,
      "Tags":null,
      "TdeCredentialArn":null,
      "TdeCredentialPassword":null,
      "Timezone":null,
      "VpcSecurityGroupIds":null
   },
   "Cluster":{  
      "AvailabilityZones":null,
      "BacktrackWindow":null,
      "BackupRetentionPeriod":null,
      "CharacterSetName":null,
      "DBClusterIdentifier":null,
      "DBClusterParameterGroupName":null,
      "DBSubnetGroupName":null,
      "DatabaseName":null,
      "DestinationRegion":null,
      "EnableCloudwatchLogsExports":null,
      "EnableIAMDatabaseAuthentication":null,
      "Engine":null,
      "EngineMode":null,
      "EngineVersion":null,
      "KmsKeyId":null,
      "MasterUserPassword":null,
      "MasterUsername":null,
      "OptionGroupName":null,
      "Port":null,
      "PreSignedUrl":null,
      "PreferredBackupWindow":null,
      "PreferredMaintenanceWindow":null,
      "ReplicationSourceIdentifier":null,
      "ScalingConfiguration":null,
      "SourceRegion":null,
      "StorageEncrypted":null,
      "Tags":null,
      "VpcSecurityGroupIds":null
   }
}
```

**Postgres Shared Specific Settings**

Sort of self explanatory, the username and password MUST be the master account, otherwise provisioning will fail. 

***Sample Shared Specific Settings***

You can use `${ENV_VAR_NAME}` to fill in any portion of the provider settings with an environment variable.

```
{
   "master_username":"username",
   "master_password":"password",
   "host":"host",
   "port":"port",
   "engine":"postgres",
   "engine_version":"9.6.6"
}
```

For example in the above if you didn't want to store the master password in the database you can retrieve it from an enviornment variable:

```
{
   "master_username":"username",
   "master_password":"${MASTER_SHAREDPG_PASSWORD}",
   "host":"host",
   "port":"port",
   "engine":"postgres",
   "engine_version":"9.6.6"
}
```

### Gcloud Specific Settings

See https://godoc.org/google.golang.org/api/sqladmin/v1beta4#Settings for potential values.

```
{
   "activationPolicy":null,
   "authorizedGaeApplications":null,
   "availabilityType":null,
   "backupConfiguration":null,
   "crashSafeReplicationEnabled":null,
   "dataDiskSizeGb":"20",
   "dataDiskType":null,
   "databaseFlags":null,
   "databaseReplicationEnabled":null,
   "ipConfiguration":null,
   "kind":"sql#settings",
   "locationPreference":null,
   "maintenanceWindow":null,
   "pricingPlan":null,
   "replicationType":null,
   "settingsVersion":null,
   "storageAutoResize":null,
   "storageAutoResizeLimit":null,
   "tier":"db-g1-small",
   "userLabels":null
}
```

***Sample Gcloud Specific Settings***

**Important** 

1. `dataDiskSizeGb` must be set and is a string not an int64 so quote the gigabytes (go figure)
2. `tier` must be set and represents the servers memory and CPU size. This value may vary by account, run the gcloud CLI sdk command `gcloud sql tiers list` for a list of tier types you can provision and their id's, the id as a string is its value.
3. You cannot use first generation tier types (I believe Dx,D0...)

```
{
   "activationPolicy":"ALWAYS",
   "authorizedGaeApplications":null,
   "availabilityType":"REGIONAL",
   "backupConfiguration":null,
   "dataDiskSizeGb":"20",
   "dataDiskType":"PD_SSD",
   "databaseFlags":null,
   "databaseReplicationEnabled":null,
   "ipConfiguration":null,
   "kind":"sql#settings",
   "locationPreference":null,
   "maintenanceWindow":null,
   "pricingPlan":"PER_USE",
   "settingsVersion":null,
   "storageAutoResize":false,
   "storageAutoResizeLimit":null,
   "tier":"db-g1-small",
   "userLabels":null
}
```


### 4. Setup Task Worker

You'll need to deploy one or multiple (depending on your load) task workers with the same config or settings specified in Step 1. but with a different startup command, append the `-background-tasks` option to the service brokers startup command to put it into worker mode.  You MUST have at least 1 worker.

## Running

As described in the setup instructions you should have two deployments for your application, the first is the API that receives requests, the other is the tasks process.

**Deploying in Kubernetes**

See the `manifests/deployment.yml` for a kubernetes deployments description.

**Debugging**

You can optionally pass in the startup options `-logtostderr=1 -stderrthreshold 0` to enable debugging, in addition you can set `GLOG_logtostderr=1` to debug via the environment.  See glog for more information on enabling various levels. You can also set `STACKIMPACT` as an environment variable to have profiling information sent to stack impact. 

## Contributing and Building

1. `dep ensure`
2. `make`
3. `./servicebroker ...`

### Testing

`make test`

Note, to run the aws instance and cluster tests `TEST_AWS_CLUSTER` and `TEST_AWS_INSTANCE` must be set to true.
Note, to run the shared postgres tests set `TEST_SHARED_POSTGRES`



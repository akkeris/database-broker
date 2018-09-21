# Akkeris Database Broker

An open service broker 2.13 compliant database broker REST api.  Depending on provider it can provision postgres, mysql, aws aurora and hypothetically oracle and mssql (although not tested) databases. Can run without Akkeris, but why would you? 

## Providers

The broker has support for the following providers

* AWS RDS Instances and Clusters
* Postgres Databases via Shared Tenant

## Features

* Create your own plans
* Upgrade plans
* Take backups, list and restore
* Create and delete read only replicas
* Create and delete read only or read write accounts (roles)
* Get database logs
* Restart
* Preprovisioning databases for speed
* Generate and issue KMS keys stored on fortanix

## Open Service Broker Additions

These describe non-standard features above/beyond what the OSB spec provides.

1. Webhooks - During async operations for provisioning resources or requesting binding an additional `webhook` and `secret` can be used in the query parameters to be informed of when the asyncronous operation completes, the value `accepts_incomplete=true` MUST also be passed for this to work.  For more information on how this works see the proposal: https://github.com/openservicebrokerapi/servicebroker/issues/606. 
2. Extensions (Actions) - This implements expiremental extention api's that are not yet standard but being implemented, see: https://github.com/openservicebrokerapi/servicebroker/pull/431
3. Get Binding - a binding may be retrived using a `GET` operation in addition to created or deleted.  It returns the same payload as a PUT when syncronous, this is now part of the 2.14 standard.

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

**Optional**

* `PORT` - This defaults to 8443, setting this changes the default port number to listen to http (or https) traffic on
* `RETRY_WEBHOOKS` - (WORKER ONLY) whether outbound notifications about provisions or create bindings should be retried if they fail.  This by default is false, unless you trust or know the clients hitting this broker, leave this disabled.

### 2. Deployment

You can deploy the image `akkeris/database-broker:lastest` via docker with the environment or config var settings above. If you decide you're going to build this manually and run it you'll need see the Building section below.  This is a 12-factor app and supports 

### 3. Plans

You can create new services and plans by provider by modifying the entries in its database. You can configure the plans and services how you wish.  By default the database broker will create 14 different postgres plans from AWS and a shared tenant system, feel free to update, delete and remove as needed. All the settings outside of provider private settings are for users benefit to help them make better choices, in Akkeris they also have additional meta data to know how or whether to certain capabilites can be performed on the addon (for example, attaching the addon to multiple apps) but are otherwise just presentation.

Open your favorite postgres client and connect to `$DATABASE_URL` above, ensure you've deployed the database broker before continuing. Each service and plan will be displayed to the user, the plans a provider specific and the column `provider_private_details` has different information depending on the provider for information on what should be in this column see the provider specific settings below. 

**AWS Provider Specific Settings**

Setting the provider specific private settings in the plan is important to do carefully as these settings are whats ACTUALLY created. You can use `${ENV_VAR_NAME}` to fill in any portion of the provider settings with an environment variable. Setting a value to null will request to use the default value from AWS. The description, type and allowed values for each of these types can be found here: https://docs.aws.amazon.com/sdk-for-go/api/service/rds/#CreateDBInstanceInput

Note: the following fields should not be set and will always be overridden (so do not set them in the settings) `Tags`, `DBInstanceIdentifier`, `DBName`, `MasterUserPassword`, `MasterUsername`, `Engine`, `EngineVersion` and `VpcSecurityGroupIds`.  The `VpcSecurityGroupIds` are tied to region and must be set via `AWS_VPC_SECURITY_GROUPS`.

***Sample AWS Provider Specific Settings***

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


**Postgres Shared Specific Settings**

Sort of self explanatory, the username and password MUST be the master account, otherwise provisioning will fail. 

***Sample Shared Specific Settings***

You can use `${ENV_VAR_NAME}` to fill in any portion of the provider settings with an environment variable.

```
{"master_username":"username", "master_password":"password", "host":"host", "port":"port", "engine":"postgres", "engine_version":"9.6.6"}
```

For example in the above if you didn't want to store the master password in the database you can retrieve it from an enviornment variable:

```
{"master_username":"username", "master_password":"${MASTER_SHAREDPG_PASSWORD}", "host":"host", "port":"port", "engine":"postgres", "engine_version":"9.6.6"}
```





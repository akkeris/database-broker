## Plans

You can create new services and plans by provider by modifying the entries in its database. You can configure the plans and services how you wish.  By default the database broker will create 14 different postgres plans from AWS and a shared tenant system, feel free to update, delete and remove as needed. All the settings outside of provider private settings are for users benefit to help them make better choices, in Akkeris they also have additional meta data to know how or whether to certain capabilites can be performed on the addon (for example, attaching the addon to multiple apps) but are otherwise just presentation.

Open your favorite postgres client and connect to `$DATABASE_URL` above, ensure you've deployed the database broker before continuing. Each service and plan will be displayed to the user, the plans a provider specific and the column `provider_private_details` has different information depending on the provider for information on what should be in this column see the provider specific settings below. 

For examples of plans, and to see the default plans see [../pkg/broker/storage.go#L239](storage.go).

### AWS Instance Provider Specific Settings

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

### AWS Cluster Specific Settings

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

### Postgres Shared Specific Settings

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

### MySQL Shared Specific Settings

Sort of self explanatory, the username and password MUST be the master account, otherwise provisioning will fail. 

***Sample Shared Specific Settings***

You can use `${ENV_VAR_NAME}` to fill in any portion of the provider settings with an environment variable.

```
{
   "master_username":"username",
   "master_password":"password",
   "host":"host",
   "port":"port",
   "engine":"mysql",
   "engine_version":"5.7",
   "scheme_type":"dsn|uri"
}
```

The `schema_type` is used to specify which type of credentials to return, if set to `dsn` a DSN is returned in the format of `username:password@tcp(host:port)/db`.  If set to `uri` a URI is returned as `mysql://username:password@host:port/db`.  This is primarily to support mysql 8.0 and above which use uri's, vs mysql clients for 5.X which use a dsn type format to connect. If your database is 5.5, 5.6, or 5.7 its recommended to set this to `dsn`, if your database is 8.0+ you should use `uri`.

For example in the above if you didn't want to store the master password in the database you can retrieve it from an enviornment variable:

```
{
   "master_username":"username",
   "master_password":"${MASTER_SHAREDPG_PASSWORD}",
   "host":"host",
   "port":"port",
   "engine":"mysql",
   "engine_version":"5.7",
   "scheme_type":"dsn"
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


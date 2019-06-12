package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	_ "github.com/lib/pq"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const plansQuery string = `
select
    plans.plan,
    plans.service,
    services.name as service_name,
    plans.name,
    plans.human_name,
    plans.description,
    plans.version,
    plans.type,
    plans.scheme,
    plans.categories,
    plans.cost_cents,
    plans.cost_unit,
    plans.attributes::text,
    plans.installable_inside_private_network,
    plans.installable_outside_private_network,
    plans.supports_multiple_installations,
    plans.supports_sharing,
    plans.preprovision,
    plans.beta,
    plans.provider,
    plans.provider_private_details::text,
    plans.deprecated
from plans join services on services.service = plans.service
    where services.deleted = false and plans.deleted = false `

const servicesQuery string = `
select
    service,
    name,
    human_name,
    description,
    categories,
    image,
    beta,
    deprecated
from services where deleted = false `

var sqlCreateScript string = `
do $$
begin
    create extension if not exists "uuid-ossp";

    if not exists (select 1 from pg_type where typname = 'alpha_numeric') then
        create domain alpha_numeric as varchar(128) check (value ~ '^[A-z0-9\-]+$');
    end if;

    if not exists (select 1 from pg_type where typname = 'enginetype') then
        create type enginetype as enum('postgres', 'aurora-mysql', 'aurora-postgresql', 'mariadb', 'mysql', 'oracle-ee', 'oracle-se2', 'oracle-se1', 'oracle-se', 'sqlserver-ee', 'sqlserver-se', 'sqlserver-ex', 'sqlserver-web');
    end if;

    if not exists (select 1 from pg_type where typname = 'clientdbtype') then
        create type clientdbtype as enum('postgres', 'mysql', 'mysqlx', 'oracledb', 'mssql', 'dsn', '');
    end if;

    if not exists (select 1 from pg_type where typname = 'cents') then
        create domain cents as int check (value >= 0);
    end if;

    if not exists (select 1 from pg_type where typname = 'costunit') then
        create type costunit as enum('year', 'month', 'day', 'hour', 'minute', 'second', 'cycle', 'byte', 'megabyte', 'gigabyte', 'terabyte', 'petabyte', 'op', 'unit');
    end if;

    if not exists (select 1 from pg_type where typname = 'task_status') then
        create type task_status as enum('pending', 'started', 'finished', 'failed');
    end if;

    create or replace function mark_updated_column() returns trigger as $emp_stamp$
    begin
        NEW.updated = now();
        return NEW;
    end;
    $emp_stamp$ language plpgsql;

    create table if not exists services
    (
        service uuid not null primary key,
        name alpha_numeric not null,
        human_name text not null,
        description text not null,
        categories varchar(1024) not null default 'Data Stores',
        image varchar(1024) not null default '',

        beta boolean not null default false,
        deprecated boolean not null default false,
        deleted boolean not null default false,
        
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now()
    );
    drop trigger if exists services_updated on services;
    create trigger services_updated before update on services for each row execute procedure mark_updated_column();

    create table if not exists plans
    (
        plan uuid not null primary key,
        service uuid references services("service") not null,
        name alpha_numeric not null,
        human_name text not null,
        description text not null,
        version text not null,
        type enginetype not null,
        scheme clientdbtype not null,
        categories text not null default '',
        cost_cents cents not null default 0,
        cost_unit costunit not null default 'month',
        attributes json not null,

        provider varchar(1024) not null,
        provider_private_details json not null,

        installable_inside_private_network bool not null default true,
        installable_outside_private_network bool not null default true,
        supports_multiple_installations bool not null default true,
        supports_sharing bool not null default true,
        preprovision int not null default 0,

        beta boolean not null default false,
        deprecated boolean not null default false,
        deleted boolean not null default false,
        
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now()
    );
    drop trigger if exists plans_updated on plans;
    create trigger plans_updated before update on plans for each row execute procedure mark_updated_column();

    create table if not exists databases
    (
        id varchar(1024) not null primary key,
        name varchar(200) not null,
        plan uuid references plans("plan") not null,
        claimed boolean not null default false,
        status varchar(1024) not null default 'unknown',
        username varchar(128),
        password varchar(128),
        endpoint varchar(128),
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now(),
        deleted bool not null default false
    );
    drop trigger if exists databases_updated on databases;
    create trigger databases_updated before update on databases for each row execute procedure mark_updated_column();

    create table if not exists replicas
    (
        id varchar(1024) not null primary key,
        database varchar(1024) references databases("id") not null,
        name varchar(128) not null,
        status varchar(1024) not null default 'unknown',
        username varchar(128),
        password varchar(128),
        endpoint varchar(128),
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now(),
        deleted bool not null default false
    );
    drop trigger if exists replicas_updated on replicas;
    create trigger replicas_updated before update on replicas for each row execute procedure mark_updated_column();

    create table if not exists roles
    (
        database varchar(1024) references databases("id") not null,
        username varchar(128) not null,
        password varchar(128) not null,
        read_only boolean not null,
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now(),
        deleted bool not null default false,
        primary key(database, username)
    );
    drop trigger if exists roles_updated on roles;
    create trigger roles_updated before update on roles for each row execute procedure mark_updated_column();

    create table if not exists tasks
    (
        task uuid not null primary key,
        database varchar(1024) references databases("id") not null,
        action varchar(1024) not null,
        status task_status not null default 'pending',
        retries int not null default 0,
        metadata text not null default '',
        result text not null default '',
        created timestamp with time zone not null default now(),
        updated timestamp with time zone not null default now(),
        started timestamp with time zone,
        finished timestamp with time zone,
        deleted bool not null default false
    );
    
    if exists (SELECT NULL 
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE table_name = 'tasks'
              AND column_name = 'action'
              and udt_name = 'task_action'
              and table_schema = 'public') then
        alter table tasks alter column action TYPE varchar(1024) using action::varchar(1024);
    end if;

    if exists (SELECT NULL 
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE table_name = 'plans'
              AND column_name = 'provider'
              and udt_name = 'providertype'
              and table_schema = 'public') then
        alter table plans alter column provider TYPE varchar(1024) using provider::varchar(1024);
    end if;

    drop trigger if exists tasks_updated on tasks;
    create trigger tasks_updated before update on tasks for each row execute procedure mark_updated_column();

    -- populate some default services (aws postgres)
    if (select count(*) from services) = 0 then
        insert into services 
            (service, name, human_name, description, categories, image, beta, deprecated)
        values 
            ('01bb60d2-f2bb-64c0-4c8b-ead731a690bd','akkeris-postgresql',   'Akkeris PostgreSQL',   'Dedicated and scalable PostgreSQL relational SQL database.',       'Data Stores,postgres', 'https://upload.wikimedia.org/wikipedia/commons/thumb/2/29/Postgresql_elephant.svg/1200px-Postgresql_elephant.svg.png', false, false),
            ('11bb60d2-f2bb-64c0-4c8b-ead731a690be','akkeris-mysql',        'Akkeris MySQL',        'Dedicated and scalable MySQL (aurora) relational SQL database.',   'Data Stores,mysql',    'https://upload.wikimedia.org/wikipedia/en/thumb/6/62/MySQL.svg/1280px-MySQL.svg.png', false, false);
    end if;

    -- populate some default plans (aws postgres)
    if (select count(*) from plans) = 0 then
        -- 9 plans
        insert into plans 
            (plan, service, name, human_name, description, version, type, scheme, categories, cost_cents, preprovision, attributes, provider, provider_private_details)
        values 
            ('50660450-61d3-2c13-a3fd-d379997932fa', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'hobby-v9',      'Hobby (9.6)',        'Postgres 9.6.6 - 1xCPU 1GB Ram 512MB Storage', '9.6.6', 'postgres', 'postgres', 'Data Stores', 0,     1, '{"compliance":"", "supports_extensions":false, "ram":"1GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"512MB", "data_clips":false, "connection_limit":20,   "high_availability":false,  "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_9_URI}", "engine":"postgres", "engine_version":"9.6.6"}'),
            ('a329070d-8caa-2c1e-1897-2c6b1590784a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-0-v9', 'Standard-0 (9.6)',   'Postgres 9.6.6 - 1xCPU 2GB Ram 4GB Storage',   '9.6.6', 'postgres', 'postgres', 'Data Stores', 500,   0, '{"compliance":"", "supports_extensions":false, "ram":"2GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"4GB",   "data_clips":false, "connection_limit":50,   "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_9_URI}", "engine":"postgres", "engine_version":"9.6.6"}'),
            ('c4330196-7d00-b529-4217-edc9c4b7b2da', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-1-v9', 'Standard-1 (9.6)',   'Postgres 9.6.6 - 1xCPU 2GB Ram 16GB Storage',  '9.6.6', 'postgres', 'postgres', 'Data Stores', 1500,  0, '{"compliance":"", "supports_extensions":false, "ram":"2GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"16GB",  "data_clips":false, "connection_limit":100,  "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_9_URI}", "engine":"postgres", "engine_version":"9.6.6"}'),
            ('553eebe4-62f4-4b2d-e24e-39da5f31a71a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-2-v9', 'Standard-2 (9.6)',   'Postgres 9.6.6 - 1xCPU 4GB Ram 32GB Storage',  '9.6.6', 'postgres', 'postgres', 'Data Stores', 4500,  0, '{"compliance":"", "supports_extensions":false, "ram":"4GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"32GB",  "data_clips":false, "connection_limit":200,  "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_9_URI}", "engine":"postgres", "engine_version":"9.6.6"}'),
            ('d294d7f1-19b4-f72c-e3bd-96970f74f02a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-0-v9',  'Premium-0 (9.5)',    'Postgres 9.5.2 - 2xCPU 4GB Ram 20GB Storage',  '9.5.2', 'postgres', 'postgres', 'Data Stores', 6000,  0, '{"compliance":"", "supports_extensions":true,  "ram":"4GB",   "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"20GB",  "data_clips":true,  "connection_limit":null, "high_availability":false,  "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":20,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.t2.medium","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":false,"Engine":"postgres","EngineVersion":"9.6.9","Iops":null,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":null,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"gp2","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}'),
            ('682631dd-0374-e495-f1f6-f1640696287a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-1-v9',  'Premium-1 (9.5)',    'Postgres 9.5.2 - 2xCPU 8GB Ram 50GB Storage',  '9.5.2', 'postgres', 'postgres', 'Data Stores', 13500, 0, '{"compliance":"", "supports_extensions":true,  "ram":"8GB",   "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"50GB",  "data_clips":true,  "connection_limit":null, "high_availability":false,  "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":50,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.m4.large","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":true,"Engine":"postgres","EngineVersion":"9.6.9","Iops":null,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":7,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"gp2","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}'),
            ('53c42a36-e61d-6fa4-7d89-1c774aeec01a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-2-v9',  'Premium-2 (9.5)',    'Postgres 9.5.2 - 4xCPU 16GB Ram 100GB Storage','9.5.2', 'postgres', 'postgres', 'Data Stores', 72000, 0, '{"compliance":"", "supports_extensions":true,  "ram":"16GB",  "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"100GB", "data_clips":true,  "connection_limit":null, "high_availability":true,   "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":100,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.m4.large","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":true,"Engine":"postgres","EngineVersion":"9.6.9","Iops":1000,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":true,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":7,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"io1","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}');

        -- 10 plans
        insert into plans 
            (plan, service, name, human_name, description, version, type, scheme, categories, cost_cents, preprovision, attributes, provider, provider_private_details)
        values 
            ('a0660450-61d3-2c13-a3fd-d379997932fa', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'hobby',         'Hobby (10.4)',        'Postgres 10.4 - 1xCPU 1GB Ram 512MB Storage', '10.4', 'postgres', 'postgres', 'Data Stores', 0,     0, '{"compliance":"", "supports_extensions":false, "ram":"1GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"512MB", "data_clips":false, "connection_limit":20,   "high_availability":false,  "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_10_URI}", "engine":"postgres", "engine_version":"10.4"}'),
            ('b329070d-8caa-2c1e-1897-2c6b1590784a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-0',    'Standard-0 (10.4)',   'Postgres 10.4 - 1xCPU 2GB Ram 4GB Storage',   '10.4', 'postgres', 'postgres', 'Data Stores', 500,   0, '{"compliance":"", "supports_extensions":false, "ram":"2GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"4GB",   "data_clips":false, "connection_limit":50,   "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_10_URI}", "engine":"postgres", "engine_version":"10.4"}'),
            ('c4330196-7d00-b529-4217-edc9c4b7b2db', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-1',    'Standard-1 (10.4)',   'Postgres 10.4 - 1xCPU 2GB Ram 16GB Storage',  '10.4', 'postgres', 'postgres', 'Data Stores', 1500,  0, '{"compliance":"", "supports_extensions":false, "ram":"2GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"16GB",  "data_clips":false, "connection_limit":100,  "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":false, "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_10_URI}", "engine":"postgres", "engine_version":"10.4"}'),
            ('d53eebe4-62f4-4b2d-e24e-39da5f31a71a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'standard-2',    'Standard-2 (10.4)',   'Postgres 10.4 - 1xCPU 4GB Ram 32GB Storage',  '10.4', 'postgres', 'postgres', 'Data Stores', 4500,  0, '{"compliance":"", "supports_extensions":false, "ram":"4GB",   "database_replicas":false, "database_logs":false, "restartable":false, "row_limits":null, "storage_capacity":"32GB",  "data_clips":false, "connection_limit":200,  "high_availability":true,   "rollback":"7 days",  "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":true,  "dedicated":false }', 'postgres-shared', '{"master_uri":"${PG_HOBBY_10_URI}", "engine":"postgres", "engine_version":"10.4"}'),
            ('e294d7f1-19b4-f72c-e3bd-96970f74f02a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-0',     'Premium-0 (10.4)',    'Postgres 10.4 - 2xCPU 4GB Ram 20GB Storage',  '10.4', 'postgres', 'postgres', 'Data Stores', 6000,  0, '{"compliance":"", "supports_extensions":true,  "ram":"4GB",   "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"20GB",  "data_clips":true,  "connection_limit":null, "high_availability":false,  "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":20,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.t2.medium","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":false,"Engine":"postgres","EngineVersion":"10.4","Iops":null,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":null,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"gp2","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}' ),
            ('f82631dd-0374-e495-f1f6-f1640696287a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-1',     'Premium-1 (10.4)',    'Postgres 10.4 - 2xCPU 8GB Ram 50GB Storage',  '10.4', 'postgres', 'postgres', 'Data Stores', 13500, 0, '{"compliance":"", "supports_extensions":true,  "ram":"8GB",   "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"50GB",  "data_clips":true,  "connection_limit":null, "high_availability":false,  "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":50,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.m4.large","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":true,"Engine":"postgres","EngineVersion":"10.4","Iops":null,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":7,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"gp2","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}' ),
            ('13c42a36-e61d-6fa4-7d89-1c774aeec01a', '01bb60d2-f2bb-64c0-4c8b-ead731a690bd', 'premium-2',     'Premium-2 (10.4)',    'Postgres 10.4 - 4xCPU 16GB Ram 100GB Storage','10.4', 'postgres', 'postgres', 'Data Stores', 72000, 0, '{"compliance":"", "supports_extensions":true,  "ram":"16GB",  "database_replicas":true,  "database_logs":true,  "restartable":true,  "row_limits":null, "storage_capacity":"100GB", "data_clips":true,  "connection_limit":null, "high_availability":true,   "rollback":"14 days", "encryption_at_rest":true, "high_speed_ssd":true,  "burstable_performance":false, "dedicated":true  }', 'aws-instance',    '{"AllocatedStorage":100,"AutoMinorVersionUpgrade":true,"AvailabilityZone":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"CopyTagsToSnapshot":true,"DBClusterIdentifier":null,"DBInstanceClass":"db.m4.large","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-postgres-subnet-group","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":true,"Engine":"postgres","EngineVersion":"10.4","Iops":1000,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":true,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":7,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":"io1","Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null}');
    
        -- aurora plans
        insert into plans 
            (plan, service, name, human_name, description, version, type, scheme, categories, cost_cents, preprovision, attributes, provider, provider_private_details)
        values 
            ('bb660450-61d3-1c13-a3fd-d3799979322a', '11bb60d2-f2bb-64c0-4c8b-ead731a690be', 'premium-0',     'Premium-0 MySQL (5.7)', 'MySQL 5.7 - 2xCPU 4GB Ram 20GB Storage',    '5.7',  'mysql',    'mysql',    'Data Stores', 6000,  0, '{"compliance":"", "supports_extensions":true,  "ram":"2GB",   "database_replicas":false, "database_logs":true, "restartable":true,  "row_limits":null, "storage_capacity":"20GB", "data_clips":false, "connection_limit":null,   "high_availability":false,  "rollback":"14 days",  "encryption_at_rest":true, "high_speed_ssd":true, "burstable_performance":false,  "dedicated":true }', 'aws-cluster',   '{"Instance":{"AllocatedStorage":null,"AutoMinorVersionUpgrade":null,"AvailabilityZone":null,"BackupRetentionPeriod":null,"CharacterSetName":null,"CopyTagsToSnapshot":null,"DBClusterIdentifier":null,"DBInstanceClass":"db.r3.large","DBInstanceIdentifier":null,"DBName":null,"DBParameterGroupName":null,"DBSecurityGroups":null,"DBSubnetGroupName":"rds-auroramysql-subnet-group-ds1","Domain":null,"DomainIAMRoleName":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"EnablePerformanceInsights":null,"Engine":"aurora-mysql","EngineVersion":null,"Iops":null,"KmsKeyId":null,"LicenseModel":null,"MasterUserPassword":null,"MasterUsername":null,"MonitoringInterval":null,"MonitoringRoleArn":null,"MultiAZ":false,"OptionGroupName":null,"PerformanceInsightsKMSKeyId":null,"PerformanceInsightsRetentionPeriod":null,"Port":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ProcessorFeatures":null,"PromotionTier":null,"PubliclyAccessible":false,"StorageEncrypted":true,"StorageType":null,"Tags":null,"TdeCredentialArn":null,"TdeCredentialPassword":null,"Timezone":null,"VpcSecurityGroupIds":null},"Cluster":{"AvailabilityZones":null,"BacktrackWindow":null,"BackupRetentionPeriod":14,"CharacterSetName":null,"DBClusterIdentifier":null,"DBClusterParameterGroupName":null,"DBSubnetGroupName":"rds-auroramysql-subnet-group-ds1","DatabaseName":null,"DestinationRegion":null,"EnableCloudwatchLogsExports":null,"EnableIAMDatabaseAuthentication":null,"Engine":"aurora-mysql","EngineMode":null,"EngineVersion":null,"KmsKeyId":null,"MasterUserPassword":null,"MasterUsername":null,"OptionGroupName":null,"Port":null,"PreSignedUrl":null,"PreferredBackupWindow":null,"PreferredMaintenanceWindow":null,"ReplicationSourceIdentifier":null,"ScalingConfiguration":null,"SourceRegion":null,"StorageEncrypted":true,"Tags":null,"VpcSecurityGroupIds":null}}');
            
    end if;
end
$$
`

func cancelOnInterrupt(ctx context.Context, db *sql.DB) {
	term := make(chan os.Signal)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-term:
			db.Close()
		case <-ctx.Done():
			db.Close()
		}
	}
}

type Storage interface {
	GetPlans(string) ([]ProviderPlan, error)
	GetPlanByID(string) (*ProviderPlan, error)
	GetReplicas(*DbInstance) (DatabaseUrlSpec, error)
	HasReplicas(*DbInstance) (int64, error)
	AddReplica(*DbInstance) error
	DeleteReplica(*DbInstance) error
	ListRoles(*DbInstance) ([]DatabaseUrlSpec, error)
	GetRole(*DbInstance, string) (DatabaseUrlSpec, error)
	AddRole(*DbInstance, string, string) (DatabaseUrlSpec, error)
	UpdateRole(*DbInstance, string, string) (DatabaseUrlSpec, error)
	HasRole(*DbInstance, string) (int64, error)
	DeleteRole(*DbInstance, string) error
	GetInstance(string) (*DbEntry, error)
	AddInstance(*DbInstance) error
	DeleteInstance(*DbInstance) error
	UpdateInstance(*DbInstance, string) error
	AddTask(string, TaskAction, string) (string, error)
	GetServices() ([]osb.Service, error)
	UpdateTask(string, *string, *int64, *string, *string, *time.Time, *time.Time) error
	PopPendingTask() (*Task, error)
	GetUnclaimedInstance(string, string) (*DbEntry, error)
	ReturnClaimedInstance(string) error
	StartProvisioningTasks() ([]DbEntry, error)
	NukeInstance(string) error
	WarnOnUnfinishedTasks()
    IsRestoring(string) (bool, error)
    IsUpgrading(string) (bool, error)
    ValidateInstanceID(id string) error
}

type PostgresStorage struct {
	Storage
	db *sql.DB
}

func (b *PostgresStorage) getPlans(subquery string, arg string) ([]ProviderPlan, error) {
	// arg could be a service ID or Plan Id
	rows, err := b.db.Query(plansQuery+subquery, arg)
	if err != nil {
		glog.Errorf("GetPlans query failed: %s\n", err.Error())
		return nil, err
	}
	defer rows.Close()
	plans := make([]ProviderPlan, 0)
	for rows.Next() {
		var planId, serviceId, serviceName, name, humanName, description, engineVersion, engineType, scheme, categories, costUnits, provider, attributes, providerPrivateDetails string
		var costInCents, preprovision int
		var beta, deprecated, installInsidePrivateNetwork, installOutsidePrivateNetwork, supportsMultipleInstallations, supportsSharing bool
		var created, updated time.Time

		err := rows.Scan(&planId, &serviceId, &serviceName, &name, &humanName, &description, &engineVersion, &engineType, &scheme, &categories, &costInCents, &costUnits, &attributes, &installInsidePrivateNetwork, &installOutsidePrivateNetwork, &supportsMultipleInstallations, &supportsSharing, &preprovision, &beta, &provider, &providerPrivateDetails, &deprecated)
		if err != nil {
			glog.Errorf("Scan from query failed: %s\n", err.Error())
			return nil, err
		}
		var free = falsePtr()
		if costInCents == 0 {
			free = truePtr()
		}

		var attributesJson map[string]interface{}
		if err = json.Unmarshal([]byte(attributes), &attributesJson); err != nil {
			glog.Errorf("Unable to unmarshal attributes in plans query: %s\n", err.Error())
			return nil, err
		}
		var state = "ga"
		if beta == true {
			state = "beta"
		}
		if deprecated == true {
			state = "deprecated"
		}
		plans = append(plans, ProviderPlan{
			basePlan: osb.Plan{
				ID:          planId,
				Name:        name,
				Description: description,
				Free:        free,
				Schemas: &osb.Schemas{
					ServiceInstance: &osb.ServiceInstanceSchema{
						Create: &osb.InputParametersSchema{},
					},
				},
				Metadata: map[string]interface{}{
					"addon_service": map[string]interface{}{
						"id":   serviceId,
						"name": serviceName,
					},
					"created_at":                          created,
					"description":                         description,
					"human_name":                          humanName,
					"id":                                  planId,
					"installable_inside_private_network":  installInsidePrivateNetwork,
					"installable_outside_private_network": installOutsidePrivateNetwork,
					"name":                                name,
					"key":                                 serviceName + ":" + name,
					"price": map[string]interface{}{
						"cents": costInCents,
						"unit":  costUnits,
					},
					"compliance":    []interface{}{},
					"space_default": false,
					"state":         state,
					"attributes":    attributesJson,
					"updated_at":    updated,
                    "engine": map[string]string{
                        "type": engineType,
                        "version": engineVersion,
                    },
				},
			},
			Provider:               GetProvidersFromString(provider),
			Scheme:                 scheme,
			providerPrivateDetails: os.ExpandEnv(providerPrivateDetails),
			ID:                     planId,
		})
	}
	return plans, nil
}

func (b *PostgresStorage) GetServices() ([]osb.Service, error) {
	services := make([]osb.Service, 0)

	rows, err := b.db.Query(servicesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var service_id, service_name, plan_human_name, plan_description, plan_categories, plan_image string
		var beta, deprecated bool
		err = rows.Scan(&service_id, &service_name, &plan_human_name, &plan_description, &plan_categories, &plan_image, &beta, &deprecated)
		if err != nil {
			return nil, err
		}

		plans, err := b.GetPlans(service_id)
		if err != nil {
			glog.Errorf("Unable to get RDS plans: %s\n", err.Error())
			return nil, InternalServerError()
		}

		osbPlans := make([]osb.Plan, 0)
		for _, plan := range plans {
			osbPlans = append(osbPlans, plan.basePlan)
		}
		services = append(services, osb.Service{
			Name:                service_name,
			ID:                  service_id,
			Description:         plan_description,
			Bindable:            true,
			BindingsRetrievable: true,
			PlanUpdatable:       truePtr(),
			Tags:                strings.Split(plan_categories, ","),
			Metadata: map[string]interface{}{
				"name":  plan_human_name,
				"image": plan_image,
			},
			Plans: osbPlans,
		})
	}
	return services, nil
}

func (b *PostgresStorage) GetPlanByID(planId string) (*ProviderPlan, error) {
	plans, err := b.getPlans(" and plans.plan::varchar(1024) = $1::varchar(1024)", planId)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("Not found")
	}
	return &plans[0], nil
}

func (b *PostgresStorage) GetPlans(serviceId string) ([]ProviderPlan, error) {
	return b.getPlans(" and services.service::varchar(1024) = $1::varchar(1024) order by plans.name", serviceId)
}

func (b *PostgresStorage) GetReplicas(dbInstance *DbInstance) (DatabaseUrlSpec, error) {
	var username, password, endpoint string
	err := b.db.QueryRow("select username, password, endpoint from replicas where database = $1 and deleted = false", dbInstance.Id).Scan(&username, &password, &endpoint)
	return DatabaseUrlSpec{
		Username: username,
		Password: password,
		Endpoint: endpoint,
		Plan:     dbInstance.Plan.ID,
	}, err
}

func (b *PostgresStorage) HasReplicas(dbInstance *DbInstance) (int64, error) {
	var amount int64
	err := b.db.QueryRow("select count(*) from replicas where database = $1 and deleted = false", dbInstance.Id).Scan(&amount)
	return amount, err
}

func (b *PostgresStorage) AddReplica(dbInstance *DbInstance) error {
	_, err := b.db.Exec("insert into replicas (id, database, name, status, username, password, endpoint) values (uuid_generate_v4(), $1, $2, $3, true, $4, false, '', now(), $5, $6, $7)", dbInstance.Id, dbInstance.Name, dbInstance.Status, dbInstance.Username, dbInstance.Password, dbInstance.Endpoint)
	return err
}

func (b *PostgresStorage) DeleteReplica(dbInstance *DbInstance) error {
	_, err := b.db.Exec("update replicas set deleted = true where id = $1", dbInstance.Id)
	return err
}

func (b *PostgresStorage) ListRoles(dbInstance *DbInstance) ([]DatabaseUrlSpec, error) {
	rows, err := b.db.Query("SELECT username, password FROM roles where database = $1 and deleted = false", dbInstance.Id)
	if err != nil {
		return []DatabaseUrlSpec{}, err
	}
	defer rows.Close()
	var roles []DatabaseUrlSpec
	for rows.Next() {
		var role DatabaseUrlSpec
		role.Endpoint = dbInstance.Endpoint
		if err := rows.Scan(&role.Username, &role.Password); err != nil {
			return []DatabaseUrlSpec{}, err
		}
		roles = append(roles, role)
	}
	return roles, nil
}

func (b *PostgresStorage) GetRole(dbInstance *DbInstance, r string) (DatabaseUrlSpec, error) {
	var role DatabaseUrlSpec
	role.Endpoint = dbInstance.Endpoint
	err := b.db.QueryRow("SELECT username, password FROM roles where database = $1 and username = $2 and deleted = false", dbInstance.Id, r).Scan(&role.Username, &role.Password)
	return role, err
}

func (b *PostgresStorage) AddRole(dbInstance *DbInstance, username string, password string) (DatabaseUrlSpec, error) {
	_, err := b.db.Exec("insert into roles (database, username, password, read_only) values ($1, $2, $3, $4)", dbInstance.Id, username, password, true)
    if err != nil {
        return DatabaseUrlSpec{}, err
    }
    var role DatabaseUrlSpec
    role.Endpoint = dbInstance.Endpoint
    role.Username = username
    role.Password = password
	return role, err
}

func (b *PostgresStorage) UpdateRole(dbInstance *DbInstance, username string, password string) (DatabaseUrlSpec, error) {
	_, err := b.db.Exec("update roles set password=$3 where database = $1 and username = $2", dbInstance.Id, username, password)
	if err != nil {
        return DatabaseUrlSpec{}, err
    }
    var role DatabaseUrlSpec
    role.Endpoint = dbInstance.Endpoint
    role.Username = username
    role.Password = password
    return role, err
}

func (b *PostgresStorage) IsUpgrading(dbId string) (bool, error) {
    var count int64
    err := b.db.QueryRow("select count(*) from tasks where ( status = 'started' or status = 'pending' ) and (action = 'change-providers' OR action = 'change-plans') and deleted = false and database = $1", dbId).Scan(&count)
    return count > 0, err
}

func (b *PostgresStorage) IsRestoring(dbId string) (bool, error) {
    var count int64
    err := b.db.QueryRow("select count(*) from tasks where ( status = 'started' or status = 'pending' ) and action = 'restore-database' and deleted = false and database = $1", dbId).Scan(&count)
    return count > 0, err
}

func (b *PostgresStorage) HasRole(dbInstance *DbInstance, username string) (int64, error) {
	var count int64
	err := b.db.QueryRow("select count(*) from roles where database = $1 and username = $2 and deleted = false", dbInstance.Id, username).Scan(&count)
	return count, err
}

func (b *PostgresStorage) DeleteRole(dbInstance *DbInstance, username string) error {
	_, err := b.db.Exec("update roles set deleted = true where database = $1 and username = $2", dbInstance.Id, username)
	return err
}

func (b *PostgresStorage) GetUnclaimedInstance(PlanId string, InstanceId string) (*DbEntry, error) {
	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	var entry DbEntry
	err = tx.QueryRow("select id, name, plan, claimed, status, username, password, endpoint from databases where claimed = false and status = 'available' and deleted = false and id != $1 and plan = $2 limit 1", InstanceId, PlanId).Scan(&entry.Id, &entry.Name, &entry.PlanId, &entry.Claimed, &entry.Status, &entry.Username, &entry.Password, &entry.Endpoint)
	if err != nil && err.Error() == "sql: no rows in result set" {
		tx.Rollback()
		return nil, errors.New("Cannot find database instance")
	} else if err != nil {
		tx.Rollback()
		return nil, err
	}

	if _, err = tx.Exec("insert into databases (id, name, plan, claimed, status, username, password, endpoint) values ($1, $2, $3, true, $4, $5, $6, $7)", InstanceId, entry.Name, entry.PlanId, entry.Status, entry.Username, entry.Password, entry.Endpoint); err != nil {
		tx.Rollback()
		return nil, err
	}

	if _, err = tx.Exec("update tasks set database = $2 where database = $1 and deleted = false", entry.Id, InstanceId); err != nil {
		tx.Rollback()
		return nil, err
	}

	if _, err = tx.Exec("update roles set database = $2 where database = $1 and deleted = false", entry.Id, InstanceId); err != nil {
		tx.Rollback()
		return nil, err
	}

	if _, err = tx.Exec("update replicas set database = $2 where database = $1 and deleted = false", entry.Id, InstanceId); err != nil {
		tx.Rollback()
		return nil, err
	}

	if _, err = tx.Exec("delete from databases where id = $1 and deleted = false and claimed = false", entry.Id); err != nil {
		tx.Rollback()
		return nil, err
	}

    entry.Claimed = true
	entry.Id = InstanceId

	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &entry, err
}

func (b *PostgresStorage) ReturnClaimedInstance(Id string) error {
	rows, err := b.db.Exec("update databases set claimed = false, id = uuid_generate_v4()::varchar(1024) where id = $1 and status = 'available' and deleted = false and claimed = true", Id)
	if err != nil {
		return err
	}
	count, err := rows.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("invalid count returned after trying to return unclaimed db " + Id)
	}
	return nil
}

func (b *PostgresStorage) AddInstance(dbInstance *DbInstance) error {
	_, err := b.db.Exec("insert into databases (id, name, plan, claimed, status, username, password, endpoint) values ($1, $2, $3, true, $4, $5, $6, $7)", dbInstance.Id, dbInstance.Name, dbInstance.Plan.ID, dbInstance.Status, dbInstance.Username, dbInstance.Password, dbInstance.Endpoint)
	return err
}

func (b *PostgresStorage) NukeInstance(Id string) error {
	_, err := b.db.Exec("delete from databases where id = $1", Id)
	return err
}

func (b *PostgresStorage) DeleteInstance(dbInstance *DbInstance) error {
	b.db.Exec("update roles set deleted = true where database = $1", dbInstance.Name)
	// TODO: do we need to ensure it does not have a replica that is going ot be orphaned?
	b.db.Exec("update replicas set deleted = true where database = $1", dbInstance.Id)
	b.db.Exec("update tasks set deleted = true where database = $1", dbInstance.Id)
	_, err := b.db.Exec("update databases set deleted = true where id = $1", dbInstance.Id)
	return err
}

func (b *PostgresStorage) UpdateInstance(dbInstance *DbInstance, PlanId string) error {
	_, err := b.db.Exec("update databases set plan = $1, endpoint = $2, status = $3, username = $4, password = $5, name = $6 where id = $7", PlanId, dbInstance.Endpoint, dbInstance.Status, dbInstance.Username, dbInstance.Password, dbInstance.Name, dbInstance.Id)
	return err
}

func (b *PostgresStorage) ValidateInstanceID(id string) error {
    var count int64
    err := b.db.QueryRow("select count(*) from databases where id = $1", id).Scan(&count)
    if err != nil {
        return err
    }
    if count != 0 {
        return errors.New("The instance id is already in use (even if deleted)")
    }
    return nil
}

func (b *PostgresStorage) StartProvisioningTasks() ([]DbEntry, error) {
	var sqlSelectToProvisionQuery = `
        select 
            plans.plan,
            plans.preprovision - ( select count(*) from databases where databases.claimed = false and (databases.status = 'available' or databases.status = 'creating' or databases.status = 'provisioning' or databases.status = 'backing-up' or databases.status = 'starting') and databases.deleted = false and plan = plans.plan ) as needed
        from 
            plans join services on plans.service = services.service 
        where 
            plans.deprecated = false and 
            plans.deleted = false and 
            services.deleted = false and 
            services.deprecated = false
    `

	rows, err := b.db.Query(sqlSelectToProvisionQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]DbEntry, 0)

	for rows.Next() {
		var planId string
		var needed int
		if err := rows.Scan(&planId, &needed); err != nil {
			return nil, err
		}
		for i := 0; i < needed; i++ {
			var entry DbEntry
			if err := b.db.QueryRow("insert into databases (id, name, plan, claimed, status, username, password, endpoint) values (uuid_generate_v4(), '', $1, false, 'provisioning', '', '', '') returning id", planId).Scan(&entry.Id); err != nil {
				glog.Infof("Unable to insert database entry for preprovisioning: %s\n", err.Error())
			} else {
				entry.PlanId = planId
				entries = append(entries, entry)
			}
		}
	}
	return entries, nil
}

func (b *PostgresStorage) GetInstance(Id string) (*DbEntry, error) {
	var entry DbEntry
	err := b.db.QueryRow("select id, name, plan, claimed, status, username, password, endpoint, (select count(*) from tasks where tasks.database=databases.id and tasks.status = 'started' and tasks.deleted = false) as tasks from databases where id = $1 and deleted = false", Id).Scan(&entry.Id, &entry.Name, &entry.PlanId, &entry.Claimed, &entry.Status, &entry.Username, &entry.Password, &entry.Endpoint, &entry.Tasks)

	if err != nil && err.Error() == "sql: no rows in result set" {
		return nil, errors.New("Cannot find database instance")
	} else if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (b *PostgresStorage) AddTask(Id string, action TaskAction, metadata string) (string, error) {
	var task_id string
	return task_id, b.db.QueryRow("insert into tasks (task, database, action, metadata) values (uuid_generate_v4(), $1, $2, $3) returning task", Id, action, metadata).Scan(&task_id)
}

func (b *PostgresStorage) UpdateTask(Id string, status *string, retries *int64, metadata *string, result *string, started *time.Time, finsihed *time.Time) error {
	_, err := b.db.Exec("update tasks set status = coalesce($2, status), retries = coalesce($3, retries), metadata = coalesce($4, metadata), result = coalesce($5, result), started = coalesce($6, started), finished = coalesce($7, finished) where task = $1", Id, status, retries, metadata, result, started, finsihed)
	return err
}

func (b *PostgresStorage) WarnOnUnfinishedTasks() {
	var amount int
	err := b.db.QueryRow("select count(*) from tasks where status = 'started' and extract(hours from now() - started) > 24 and deleted = false").Scan(&amount)
	if err != nil {
		glog.Errorf("Unable to select stale tasks: %s\n", err.Error())
		return
	}
	if amount < 0 {
		glog.Errorf("WARNING: There are %d started tasks that are now over 24 hours old and have not yet finished, they may be stale.\n", amount)
	}
}

func (b *PostgresStorage) PopPendingTask() (*Task, error) {
	var task Task
	err := b.db.QueryRow(`
        update tasks set 
            status = 'started', 
            started = now() 
        where 
            task in ( select task from tasks where status = 'pending' and deleted = false order by updated asc limit 1)
        returning task, action, database, status, retries, metadata, result, started, finished
    `).Scan(&task.Id, &task.Action, &task.DatabaseId, &task.Status, &task.Retries, &task.Metadata, &task.Result, &task.Started, &task.Finished)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func InitStorage(ctx context.Context, o Options) (*PostgresStorage, error) {
	// Sanity checks
	if o.DatabaseUrl == "" && os.Getenv("DATABASE_URL") != "" {
		o.DatabaseUrl = os.Getenv("DATABASE_URL")
	}
	if o.DatabaseUrl == "" {
		return nil, errors.New("Unable to connect to database, none was specified in the environment via DATABASE_URL or through the -database cli option.")
	}
	db, err := sql.Open("postgres", o.DatabaseUrl)
	if err != nil {
		glog.Errorf("Unable to create database schema: %s\n", err.Error())
		return nil, errors.New("Unable to create database schema: " + err.Error())
	}

	_, err = db.Exec(sqlCreateScript)
	if err != nil {
		return nil, err
	}

	go cancelOnInterrupt(ctx, db)

	return &PostgresStorage{
		db: db,
	}, nil
}

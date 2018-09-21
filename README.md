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
* `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to an IAM role that has full access to RDS in the `AWS_REGION` you specified above.

Note that you can get away with not setting `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and use EC2 IAM roles or hard coded credentials via the `~/.aws/credentials` file but these are not recommended!

**Shared Postgres Provider Specific**

There are no environment variables for postgres, see plans

**Optional**

* `PORT` - This defaults to 8443, setting this changes the default port number to listen to http (or https) traffic on
* `RETRY_WEBHOOKS` - (WORKER ONLY) whether outbound notifications about provisions or create bindings should be retried if they fail.  This by default is false, unless you trust or know the clients hitting this broker, leave this disabled.

### 2. Deployment

You can deploy the image `akkeris/database-broker:lastest` via docker with the environment or config var settings above. If you decide you're going to build this manually and run it you'll need see the Building section below.  This is a 12-factor app and supports 
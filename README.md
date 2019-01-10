# Akkeris Database Broker

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/9b3065a9f42d44618ed8e459032e5964)](https://www.codacy.com/app/Akkeris/database-broker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=akkeris/database-broker&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://api.codacy.com/project/badge/Coverage/9b3065a9f42d44618ed8e459032e5964)](https://www.codacy.com/app/Akkeris/database-broker?utm_source=github.com&utm_medium=referral&utm_content=akkeris/database-broker&utm_campaign=Badge_Coverage)
[![CircleCI](https://circleci.com/gh/akkeris/database-broker.svg?style=svg)](https://circleci.com/gh/akkeris/database-broker)

A database broker for a variety of cloud providers and even on-prem db systems that implements the Open Service Broker 2.13 compliant database broker REST API.  Depending on provider it can provision postgres, mysql, aws aurora and hypothetically oracle and mssql (although not tested) databases. It can also be ran without Akkeris, but why would you? 

## Providers

The broker has support for the following providers

* AWS RDS Instances and Clusters
* Gcloud SQL Instances and Clusters
* Postgres Databases via Shared Tenant
* MySQL 5.5, 5.7, 8 Databases via Shared Tenant

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

First, set your settings, so to speak, although not required these installation instructions assume you're deploying to a dockerized environment.  You'll also need to provision (manually) a postgres database so the database broker can store plans, databases and other information for itself.  Once you have your settings, move on to the deploy step, then finally setup your [docs/PLANS.md](plans).

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

**Google Cloud Specific**

* Gcloud credentials are automatically inferred by the SDK through the standard environment variables, installed credentials on the host, or via server roles.  See https://cloud.google.com/docs/authentication/production for more information on injecting credentials in the app (normally set `GOOGLE_APPLICATION_CREDENTIALS` to the path of your credentials (in json format). Ensure the credentials used have access to SQL administration.
* `GCLOUD_PROJECT_ID` - The google project id to use.
* `GCLOUD_REGION` - The google region used for this broker.

**Shared Postgres Provider Specific**

There are no environment variables for shared postgres providers, although sensitive configuration can be set in the enivornment, see [docs/PLANS.md](plans) for more information. 

**Shared Mysql Provider Specific**

There are no environment variables for shared postgres providers, although sensitive configuration can be set in the enivornment, see [docs/PLANS.md](plans) for more information. 

**Optional**

* `PORT` - This defaults to 8443, setting this changes the default port number to listen to http (or https) traffic on
* `RETRY_WEBHOOKS` - (WORKER ONLY) whether outbound notifications about provisions or create bindings should be retried if they fail.  This by default is false, unless you trust or know the clients hitting this broker, leave this disabled.

### 2. Deployment

You can deploy the image `akkeris/database-broker:lastest` via docker with the environment or config var settings above. If you decide you're going to build this manually and run it you'll need see the Building section below. 

### 3. Plans

Plans can be created by modifying the database table called "plans". They provide a great way of limiting the scope, capability and offerings to whomever is using the broker. See [docs/PLANS.md](plans) for more information. By default the database-broker will initially load with plans for aws and shared postgres. 

### 4. Setup Task Worker

You'll need to deploy one or multiple (depending on your load) task workers with the same config or settings specified in Step 1. but with a different startup command, append the `-background-tasks` option to the service brokers startup command to put it into worker mode.  You MUST have at least 1 worker.

## Running

As described in the setup instructions you should have two deployments for your application, the first is the API that receives requests, the other is the tasks process.  See `start.sh` for the API startup command, see `start-background.sh` for the tasks process startup command. Both of these need the above environment variables in order to run correctly.

**Debugging**

You can optionally pass in the startup options `-logtostderr=1 -stderrthreshold 0` to enable debugging, in addition you can set `GLOG_logtostderr=1` to debug via the environment.  See glog for more information on enabling various levels. You can also set `STACKIMPACT` as an environment variable to have profiling information sent to stack impact. 

## Contributing and Building

1. `dep ensure`
2. `make`
3. `./servicebroker ...`

### Testing

`make test`

1. To run the aws instance and cluster tests `TEST_AWS_CLUSTER` and `TEST_AWS_INSTANCE` must be set to true.
2. To run the shared postgres tests set `TEST_SHARED_POSTGRES` to true (the `DATABASE_URL` will be used as the shared tenant!)
3. To run the mysql postgres test set `MYSQL_URL` and `TEST_SHARED_MYSQL` to true.



<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Catalogs

PyIceberg currently has native support for REST, Hive and Glue.

There are three ways to pass in configuration:

- Using the `~/.pyiceberg.yaml` configuration file
- Through environment variables
- By passing in credentials through the CLI or the Python API

The configuration file is recommended since that's the most transparent way. If you prefer environment configuration:

```sh
export PYICEBERG_CATALOG__DEFAULT__URI=thrift://localhost:9083
```

The environment variable picked up by Iceberg starts with `PYICEBERG_` and then follows the yaml structure below, where a double underscore `__` represents a nested field.

For the FileIO there are several configuration options available:

| Key                      | Example             | Description                                                                                                                                                                                                                                               |
|--------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| s3.endpoint              | https://10.0.19.25/ | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. |
| s3.access-key-id         | admin               | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                         |
| s3.secret-access-key     | password            | Configure the static session token used to access the FileIO.                                                                                                                                                                                             |
| s3.signer                | bearer              | Configure the signature version of the FileIO.                                                                                                                                                                                                            |
| adlfs.endpoint           | http://127.0.0.1/   | Configure an alternative endpoint of the ADLFS service for the FileIO to access. This could be used to use FileIO with any adlfs-compatible object storage service that has a different endpoint (like [azurite](https://github.com/azure/azurite)).      |
| adlfs.account-name       | devstoreaccount1    | Configure the static storage account name used to access the FileIO.                                                                                                                                                                                      |
| adlfs.account-key        | Eby8vdM02xNOcqF...  | Configure the static storage account key used to access the FileIO.                                                                                                                                                                                       |

## REST Catalog

```yaml
catalog:
  default:
    uri: http://rest-catalog/ws/
    credential: t-1234:secret

  default-mtls-secured-catalog:
    uri: https://rest-catalog/ws/
    ssl:
      client:
        cert: /absolute/path/to/client.crt
        key: /absolute/path/to/client.key
      cabundle: /absolute/path/to/cabundle.pem
```

## Hive Catalog

```yaml
catalog:
  default:
    uri: thrift://localhost:9083
    s3.endpoint: http://localhost:9000
    s3.access-key-id: admin
    s3.secret-access-key: password
```

## Glue Catalog

If you want to use AWS Glue as the catalog, you can use the last two ways to configure the pyiceberg and refer
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) to set your AWS account credentials locally.

```yaml
catalog:
  default:
    type: glue
```

## DynamoDB Catalog

If you want to use AWS DynamoDB as the catalog, you can use the last two ways to configure the pyiceberg and refer
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
to set your AWS account credentials locally.

```yaml
catalog:
  default:
    type: dynamodb
    dynamodb_table_name: iceberg
```

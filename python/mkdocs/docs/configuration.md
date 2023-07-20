---
hide:
  - navigation
---

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

PyIceberg currently has native support for REST, SQL, Hive, Glue and DynamoDB.

There are three ways to pass in configuration:

- Using the `~/.pyiceberg.yaml` configuration file
- Through environment variables
- By passing in credentials through the CLI or the Python API

The configuration file is recommended since that's the most transparent way. If you prefer environment configuration:

```sh
export PYICEBERG_CATALOG__DEFAULT__URI=thrift://localhost:9083
```

The environment variable picked up by Iceberg starts with `PYICEBERG_` and then follows the yaml structure below, where a double underscore `__` represents a nested field.

## FileIO

Iceberg works with the concept of a FileIO which is a pluggable module for reading, writing, and deleting files. By default, PyIceberg will try to initialize the FileIO that's suitable for the scheme (`s3://`, `gs://`, etc.) and will use the first one that's installed.

- **s3**, **s3a**, **s3n**: `PyArrowFileIO`, `FsspecFileIO`
- **gs**: `PyArrowFileIO`
- **file**: `PyArrowFileIO`
- **hdfs**: `PyArrowFileIO`
- **abfs**, **abfss**: `FsspecFileIO`

You can also set the FileIO explicitly:

| Key        | Example                          | Description                                                                                     |
| ---------- | -------------------------------- | ----------------------------------------------------------------------------------------------- |
| py-io-impl | pyiceberg.io.fsspec.FsspecFileIO | Sets the FileIO explicitly to an implementation, and will fail explicitly if it can't be loaded |

For the FileIO there are several configuration options available:

### S3

| Key                  | Example                  | Description                                                                                                                                                                                                                                               |
| -------------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| s3.endpoint          | https://10.0.19.25/      | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3FileIO with any s3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud. |
| s3.access-key-id     | admin                    | Configure the static secret access key used to access the FileIO.                                                                                                                                                                                         |
| s3.secret-access-key | password                 | Configure the static session token used to access the FileIO.                                                                                                                                                                                             |
| s3.signer            | bearer                   | Configure the signature version of the FileIO.                                                                                                                                                                                                            |
| s3.region            | us-west-2                | Sets the region of the bucket                                                                                                                                                                                                                             |
| s3.proxy-uri         | http://my.proxy.com:8080 | Configure the proxy server to be used by the FileIO.                                                                                                                                                                                                      |

### Azure Data lake

| Key                     | Example                                                                                   | Description                                                                                                                                                                                                                                                                            |
| ----------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| adlfs.connection-string | AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqF...;BlobEndpoint=http://localhost/ | A [connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string). This could be used to use FileIO with any adlfs-compatible object storage service that has a different endpoint (like [azurite](https://github.com/azure/azurite)). |
| adlfs.account-name      | devstoreaccount1                                                                          | The account that you want to connect to                                                                                                                                                                                                                                                |
| adlfs.account-key       | Eby8vdM02xNOcqF...                                                                        | The key to authentication against the account.                                                                                                                                                                                                                                         |
| adlfs.sas-token         | NuHOuuzdQN7VRM%2FOpOeqBlawRCA845IY05h9eu1Yte4%3D                                          | The shared access signature                                                                                                                                                                                                                                                            |
| adlfs.tenant-id         | ad667be4-b811-11ed-afa1-0242ac120002                                                      | The tenant-id                                                                                                                                                                                                                                                                          |
| adlfs.client-id         | ad667be4-b811-11ed-afa1-0242ac120002                                                      | The client-id                                                                                                                                                                                                                                                                          |
| adlfs.client-secret     | oCA3R6P\*ka#oa1Sms2J74z...                                                                | The client-secret                                                                                                                                                                                                                                                                      |

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

| Key                 | Example                 | Description                                                                |
| ------------------- | ----------------------- | -------------------------------------------------------------------------- |
| uri                 | https://rest-catalog/ws | URI identifying the REST Server                                            |
| credential          | t-1234:secret           | Credential to use for OAuth2 credential flow when initializing the catalog |
| token               | FEW23.DFSDF.FSDF        | Bearer token value to use for `Authorization` header                       |
| rest.sigv4-enabled  | true                    | Sign requests to the REST Server using AWS SigV4 protocol                  |
| rest.signing-region | us-east-1               | The region to use when SigV4 signing a request                             |
| rest.signing-name   | execute-api             | The service signing name to use when SigV4 signing a request               |

## SQL Catalog

The SQL catalog requires a database for its backend. As of now, pyiceberg only supports PostgreSQL through psycopg2.
The database connection has to be configured using the `uri` property (see SQLAlchemy's [documentation for URL format](https://docs.sqlalchemy.org/en/20/core/engines.html#backend-specific-urls)):

```yaml
catalog:
  default:
    type: sql
    uri: postgresql+psycopg2://username:password@localhost/mydatabase
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

Your AWS credentials can be passed directly through the Python API.
Otherwise, please refer to
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) to set your AWS account credentials locally.
If you did not set up a default AWS profile, you can configure the `profile_name`.

```yaml
catalog:
  default:
    type: glue
    aws_access_key_id: <ACCESS_KEY_ID>
    aws_secret_access_key: <SECRET_ACCESS_KEY>
    aws_session_token: <SESSION_TOKEN>
    region_name: <REGION_NAME>
```

```yaml
catalog:
  default:
    type: glue
    profile_name: <PROFILE_NAME>
    region_name: <REGION_NAME>
```

## DynamoDB Catalog

If you want to use AWS DynamoDB as the catalog, you can use the last two ways to configure the pyiceberg and refer
[How to configure AWS credentials](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
to set your AWS account credentials locally.

```yaml
catalog:
  default:
    type: dynamodb
    table-name: iceberg
```

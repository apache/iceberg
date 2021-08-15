<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->
 
# Iceberg AWS Integrations

Iceberg provides integration with different AWS services through the `iceberg-aws` module. 
This section describes how to use Iceberg with AWS.

## Enabling AWS Integration

The `iceberg-aws` module is bundled with Spark and Flink engine runtimes for all versions from `0.11.0` onwards.
However, the AWS clients are not bundled so that you can use the same client version as your application.
You will need to provide the AWS v2 SDK because that is what Iceberg depends on.
You can choose to use the [AWS SDK bundle](https://mvnrepository.com/artifact/software.amazon.awssdk/bundle), 
or individual AWS client packages (Glue, S3, DynamoDB, KMS, STS) if you would like to have a minimal dependency footprint.

All the default AWS clients use the [URL Connection HTTP Client](https://mvnrepository.com/artifact/software.amazon.awssdk/url-connection-client)
for HTTP connection management.
This dependency is not part of the AWS SDK bundle and needs to be added separately.
To choose a different HTTP client library such as [Apache HTTP Client](https://mvnrepository.com/artifact/software.amazon.awssdk/apache-client),
see the section [client customization](#aws-client-customization) for more details.

All the AWS module features can be loaded through custom catalog properties,
you can go to the documentations of each engine to see how to load a custom catalog.
Here are some examples.

### Spark

For example, to use AWS features with Spark 3 and AWS clients version 2.15.40, you can start the Spark SQL shell with:

```sh
# add Iceberg dependency
ICEBERG_VERSION={{ versions.iceberg }}
DEPENDENCIES="org.apache.iceberg:iceberg-spark3-runtime:$ICEBERG_VERSION"

# add AWS dependnecy
AWS_SDK_VERSION=2.15.40
AWS_MAVEN_GROUP=software.amazon.awssdk
AWS_PACKAGES=(
    "bundle"
    "url-connection-client"
)
for pkg in "${AWS_PACKAGES[@]}"; do
    DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

# start Spark SQL client shell
spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager \
    --conf spark.sql.catalog.my_catalog.lock.table=myGlueLockTable
```

As you can see, In the shell command, we use `--packages` to specify the additional AWS bundle and HTTP client dependencies with their version as `2.15.40`.

### Flink

To use AWS module with Flink, you can download the necessary dependencies and specify them when starting the Flink SQL client:

```sh
# download Iceberg dependency
ICEBERG_VERSION={{ versions.iceberg }}
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_MAVEN_URL=$MAVEN_URL/org/apache/iceberg
wget $ICEBERG_MAVEN_URL/iceberg-flink-runtime/$ICEBERG_VERSION/iceberg-flink-runtime-$ICEBERG_VERSION.jar

# download AWS dependnecy
AWS_SDK_VERSION=2.15.40
AWS_MAVEN_URL=$MAVEN_URL/software/amazon/awssdk
AWS_PACKAGES=(
    "bundle"
    "url-connection-client"
)
for pkg in "${AWS_PACKAGES[@]}"; do
    wget $AWS_MAVEN_URL/$pkg/$AWS_SDK_VERSION/$pkg-$AWS_SDK_VERSION.jar
done

# start Flink SQL client shell
/path/to/bin/sql-client.sh embedded \
    -j iceberg-flink-runtime-$ICEBERG_VERSION.jar \
    -j bundle-$AWS_SDK_VERSION.jar \
    -j url-connection-client-$AWS_SDK_VERSION.jar \
    shell
```

With those dependencies, you can create a Flink catalog like the following:

```sql
CREATE CATALOG my_catalog WITH (
  'type'='iceberg',
  'warehouse'='s3://my-bucket/my/key/prefix',
  'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  'lock-impl'='org.apache.iceberg.aws.glue.DynamoLockManager',
  'lock.table'='myGlueLockTable'
);
```

You can also specify the catalog configurations in `sql-client-defaults.yaml` to preload it:

```yaml
catalogs: 
  - name: my_catalog
    type: iceberg
    warehouse: s3://my-bucket/my/key/prefix
    catalog-impl: org.apache.iceberg.aws.glue.GlueCatalog
    io-impl: org.apache.iceberg.aws.s3.S3FileIO
    lock-impl: org.apache.iceberg.aws.glue.DynamoLockManager
    lock.table: myGlueLockTable
```

### Hive

To use AWS module with Hive, you can download the necessary dependencies similar to the Flink example,
and then add them to the Hive classpath or add the jars at runtime in CLI:

```
add jar /my/path/to/iceberg-hive-runtime.jar;
add jar /my/path/to/aws/bundle.jar;
add jar /my/path/to/aws/url-connection-client.jar;
```

With those dependencies, you can register a Glue catalog and create external tables in Hive at runtime in CLI by:

```sql
SET iceberg.engine.hive.enabled=true;
SET hive.vectorized.execution.enabled=false;
SET iceberg.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog;
SET iceberg.catalog.glue.warehouse=s3://my-bucket/my/key/prefix;
SET iceberg.catalog.glue.lock-impl=org.apache.iceberg.aws.glue.DynamoLockManager;
SET iceberg.catalog.glue.lock.table=myGlueLockTable;

-- suppose you have an Iceberg table database_a.table_a created by GlueCatalog
CREATE EXTERNAL TABLE database_a.table_a
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
TBLPROPERTIES ('iceberg.catalog'='glue');
```

You can also preload the catalog by setting the configurations above in `hive-site.xml`.

## Catalogs

There are multiple different options that users can choose to build an Iceberg catalog with AWS.

### Glue Catalog

Iceberg enables the use of [AWS Glue](https://aws.amazon.com/glue) as the `Catalog` implementation.
When used, an Iceberg namespace is stored as a [Glue Database](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html), 
an Iceberg table is stored as a [Glue Table](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html),
and every Iceberg table version is stored as a [Glue TableVersion](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-TableVersion). 
You can start using Glue catalog by specifying the `catalog-impl` as `org.apache.iceberg.aws.glue.GlueCatalog`,
just like what is shown in the [enabling AWS integration](#enabling-aws-integration) section above. 
More details about loading the catalog can be found in individual engine pages, such as [Spark](../spark-configuration/#loading-a-custom-catalog) and [Flink](../flink/#creating-catalogs-and-using-catalogs).

#### Glue Catalog ID
There is a unique Glue metastore in each AWS account and each AWS region.
By default, `GlueCatalog` chooses the Glue metastore to use based on the user's default AWS client credential and region setup.
You can specify the Glue catalog ID through `glue.id` catalog property to point to a Glue catalog in a different AWS account.
The Glue catalog ID is your numeric AWS account ID.
If the Glue catalog is in a different region, you should configure you AWS client to point to the correct region, 
see more details in [AWS client customization](#aws-client-customization).

#### Skip Archive

By default, Glue stores all the table versions created and user can rollback a table to any historical version if needed.
However, if you are streaming data to Iceberg, this will easily create a lot of Glue table versions.
Therefore, it is recommended to turn off the archive feature in Glue by setting `glue.skip-archive` to `true`.
For more details, please read [Glue Quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html) and the [UpdateTable API](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html).

#### DynamoDB for Commit Locking

Glue does not have a strong guarantee over concurrent updates to a table. 
Although it throws `ConcurrentModificationException` when detecting two processes updating a table at the same time,
there is no guarantee that one update would not clobber the other update.
Therefore, [DynamoDB](https://aws.amazon.com/dynamodb) can be used for Glue, so that for every commit, 
`GlueCatalog` first obtains a lock using a helper DynamoDB table and then try to safely modify the Glue table.

This feature requires the following lock related catalog properties:

1. Set `lock-impl` as `org.apache.iceberg.aws.glue.DynamoLockManager`.
2. Set `lock.table` as the DynamoDB table name you would like to use. If the lock table with the given name does not exist in DynamoDB, a new table is created with billing mode set as [pay-per-request](https://aws.amazon.com/blogs/aws/amazon-dynamodb-on-demand-no-capacity-planning-and-pay-per-request-pricing).

Other lock related catalog properties can also be used to adjust locking behaviors such as heartbeat interval.
For more details, please refer to [Lock catalog properties](../configuration/#lock-catalog-properties). 

#### Warehouse Location

Similar to all other catalog implementations, `warehouse` is a required catalog property to determine the root path of the data warehouse in storage.
By default, Glue only allows a warehouse location in S3 because of the use of `S3FileIO`.
To store data in a different local or cloud store, Glue catalog can switch to use `HadoopFileIO` or any custom FileIO by setting the `io-impl` catalog property.
Details about this feature can be found in the [custom FileIO](../custom-catalog/#custom-file-io-implementation) section.

#### Table Location

By default, the root location for a table `my_table` of namespace `my_ns` is at `my-warehouse-location/my-ns.db/my-table`.
This default root location can be changed at both namespace and table level.

To use a different path prefix for all tables under a namespace, use AWS console or any AWS Glue client SDK you like to update the `locationUri` attribute of the corresponding Glue database.
For example, you can update the `locationUri` of `my_ns` to `s3://my-ns-bucket`, 
then any newly created table will have a default root location under the new prefix.
For instance, a new table `my_table_2` will have its root location at `s3://my-ns-bucket/my_table_2`.

To use a completely different root path for a specific table, set the `location` table property to the desired root path value you want.
For example, in Spark SQL you can do:

```sql
CREATE TABLE my_catalog.my_ns.my_table (
    id bigint,
    data string,
    category string)
USING iceberg
OPTIONS ('location'='s3://my-special-table-bucket')
PARTITIONED BY (category);
```

For engines like Spark that supports the `LOCATION` keyword, the above SQL statement is equivalent to:

```sql
CREATE TABLE my_catalog.my_ns.my_table (
    id bigint,
    data string,
    category string)
USING iceberg
LOCATION 's3://my-special-table-bucket'
PARTITIONED BY (category);
```

### DynamoDB Catalog

Iceberg supports using a [DynamoDB](https://aws.amazon.com/dynamodb) table to record and manage database and table information.

#### Configurations

The DynamoDB catalog supports the following configurations:

| Property                          | Default                                            | Description                                            |
| --------------------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| dynamodb.table-name               | iceberg                                            | name of the DynamoDB table used by DynamoDbCatalog     |


#### Internal Table Design

The DynamoDB table is designed with the following columns:

| Column            | Key             | Type        | Description                                                          |
| ----------------- | --------------- | ----------- |--------------------------------------------------------------------- |
| identifier        | partition key   | string      | table identifier such as `db1.table1`, or string `NAMESPACE` for namespaces |
| namespace         | sort key        | string      | namespace name. A [global secondary index (GSI)](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html) is created with namespace as partition key, identifier as sort key, no other projected columns |
| v                 |                 | string      | row version, used for optimistic locking |
| updated_at        |                 | number      | timestamp (millis) of the last update | 
| created_at        |                 | number      | timestamp (millis) of the table creation |
| p.<property_key\> |                 | string      | Iceberg-defined table properties including `table_type`, `metadata_location` and `previous_metadata_location` or namespace properties

This design has the following benefits:

1. it avoids potential [hot partition issue](https://aws.amazon.com/premiumsupport/knowledge-center/dynamodb-table-throttled/) if there are heavy write traffic to the tables within the same namespace, because the partition key is at the table level
2. namespace operations are clustered in a single partition to avoid affecting table commit operations
3. a sort key to partition key reverse GSI is used for list table operation, and all other operations are single row ops or single partition query. No full table scan is needed for any operation in the catalog.
4. a string UUID version field `v` is used instead of `updated_at` to avoid 2 processes committing at the same millisecond
5. multi-row transaction is used for `catalog.renameTable` to ensure idempotency
6. properties are flattened as top level columns so that user can add custom GSI on any property field to customize the catalog. For example, users can store owner information as table property `owner`, and search tables by owner by adding a GSI on the `p.owner` column.

### RDS JDBC Catalog

Iceberg also supports JDBC catalog which uses a table in a relational database to manage Iceberg tables.
You can configure to use JDBC catalog with relational database services like [AWS RDS](https://aws.amazon.com/rds).
Read [the JDBC integration page](../jdbc/#jdbc-catalog) for guides and examples about using the JDBC catalog.
Read [this AWS documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Java.html) for more details about configuring JDBC catalog with IAM authentication. 

### Which catalog to choose?

With all the available options, we offer the following guidance when choosing the right catalog to use for your application:

1. if your organization has an existing Glue metastore or plans to use the AWS analytics ecosystem including Glue, [Athena](https://aws.amazon.com/athena), [EMR](https://aws.amazon.com/emr), [Redshift](https://aws.amazon.com/redshift) and [LakeFormation](https://aws.amazon.com/lake-formation), Glue catalog provides the easiest integration.
2. if your application requires frequent updates to table or high read and write throughput (e.g. streaming write), DynamoDB catalog provides the best performance through optimistic locking.
3. if you would like to enforce access control for tables in a catalog, Glue tables can be managed as an [IAM resource](https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsglue.html), whereas DynamoDB catalog tables can only be managed through [item-level permission](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/specifying-conditions.html) which is much more complicated.
4. if you would like to query tables based on table property information without the need to scan the entire catalog, DynamoDB catalog allows you to build secondary indexes for any arbitrary property field and provide efficient query performance.
5. if you would like to have the benefit of DynamoDB catalog while also connect to Glue, you can enable [DynamoDB stream with Lambda trigger](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.Lambda.Tutorial.html) to asynchronously update your Glue metastore with table information in the DynamoDB catalog. 
6. if your organization already maintains an existing relational database in RDS or uses [serverless Aurora](https://aws.amazon.com/rds/aurora/serverless/) to manage tables, JDBC catalog provides the easiest integration.

## S3 FileIO

Iceberg allows users to write data to S3 through `S3FileIO`.
`GlueCatalog` by default uses this `FileIO`, and other catalogs can load this `FileIO` using the `io-impl` catalog property.

### Progressive Multipart Upload

`S3FileIO` implements a customized progressive multipart upload algorithm to upload data.
Data files are uploaded by parts in parallel as soon as each part is ready,
and each file part is deleted as soon as its upload process completes.
This provides maximized upload speed and minimized local disk usage during uploads.
Here are the configurations that users can tune related to this feature:

| Property                          | Default                                            | Description                                            |
| --------------------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| s3.multipart.num-threads          | the available number of processors in the system   | number of threads to use for uploading parts to S3 (shared across all output streams)  |
| s3.multipart.part-size-bytes      | 32MB                                               | the size of a single part for multipart upload requests  |
| s3.multipart.threshold            | 1.5                                                | the threshold expressed as a factor times the multipart size at which to switch from uploading using a single put object request to uploading using multipart upload  |
| s3.staging-dir                    | `java.io.tmpdir` property value                    | the directory to hold temporary files  |

### S3 Server Side Encryption

`S3FileIO` supports all 3 S3 server side encryption modes:

* [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html): When you use Server-Side Encryption with Amazon S3-Managed Keys (SSE-S3), each object is encrypted with a unique key. As an additional safeguard, it encrypts the key itself with a master key that it regularly rotates. Amazon S3 server-side encryption uses one of the strongest block ciphers available, 256-bit Advanced Encryption Standard (AES-256), to encrypt your data.
* [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html): Server-Side Encryption with Customer Master Keys (CMKs) Stored in AWS Key Management Service (SSE-KMS) is similar to SSE-S3, but with some additional benefits and charges for using this service. There are separate permissions for the use of a CMK that provides added protection against unauthorized access of your objects in Amazon S3. SSE-KMS also provides you with an audit trail that shows when your CMK was used and by whom. Additionally, you can create and manage customer managed CMKs or use AWS managed CMKs that are unique to you, your service, and your Region.
* [SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html): With Server-Side Encryption with Customer-Provided Keys (SSE-C), you manage the encryption keys and Amazon S3 manages the encryption, as it writes to disks, and decryption, when you access your objects.

To enable server side encryption, use the following configuration properties:

| Property                          | Default                                  | Description                                            |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| s3.sse.type                       | `none`                                   | `none`, `s3`, `kms` or `custom`                        |
| s3.sse.key                        | `aws/s3` for `kms` type, null otherwise  | A KMS Key ID or ARN for `kms` type, or a custom base-64 AES256 symmetric key for `custom` type.  |
| s3.sse.md5                        | null                                     | If SSE type is `custom`, this value must be set as the base-64 MD5 digest of the symmetric key to ensure integrity. |

### S3 Access Control List

`S3FileIO` supports S3 access control list (ACL) for detailed access control. 
User can choose the ACL level by setting the `s3.acl` property.
For more details, please read [S3 ACL Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html).

### Object Store File Layout

S3 and many other cloud storage services [throttle requests based on object prefix](https://aws.amazon.com/premiumsupport/knowledge-center/s3-request-limit-avoid-throttling/).
Data stored in S3 with a traditional Hive storage layout can face S3 request throttling as objects are stored under the same filepath prefix.

Iceberg by default uses the Hive storage layout, but can be switched to use the `ObjectStoreLocationProvider`. 
With `ObjectStoreLocationProvider`, a determenistic hash is generated for each stored file, with the hash appended 
directly after the `write.object-storage.path`. This ensures files written to s3 are equally distributed across multiple [prefixes](https://aws.amazon.com/premiumsupport/knowledge-center/s3-object-key-naming-pattern/) in the S3 bucket. Resulting in minimized throttling and maximized throughput for S3-related IO operations. When using `ObjectStoreLocationProvider` having a shared and short `write.object-storage.path` across your Iceberg tables will improve performance.

For more information on how S3 scales API QPS, checkout the 2018 re:Invent session on [Best Practices for Amazon S3 and Amazon S3 Glacier]( https://youtu.be/rHeTn9pHNKo?t=3219). At [53:39](https://youtu.be/rHeTn9pHNKo?t=3219) it covers how S3 scales/partitions & at [54:50](https://youtu.be/rHeTn9pHNKo?t=3290) it discusses the 30-60 minute wait time before new partitions are created.

To use the `ObjectStorageLocationProvider` add `'write.object-storage.enabled'=true` in the table's properties. 
Below is an example Spark SQL command to create a table using the `ObjectStorageLocationProvider`:
```sql
CREATE TABLE my_catalog.my_ns.my_table (
    id bigint,
    data string,
    category string)
USING iceberg
OPTIONS (
    'write.object-storage.enabled'=true, 
    'write.object-storage.path'='s3://my-table-data-bucket')
PARTITIONED BY (category);
```

We can then insert a single row into this new table
```SQL
INSERT INTO my_catalog.my_ns.my_table VALUES (1, "Pizza", "orders");
```

Which will write the data to S3 with a hash (`2d3905f8`) appended directly after the `write.object-storage.path`, ensuring reads to the table are spread evenly  across [S3 bucket prefixes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html), and improving performance.
```
s3://my-table-data-bucket/2d3905f8/my_ns.db/my_table/category=orders/00000-0-5affc076-96a4-48f2-9cd2-d5efbc9f0c94-00001.parquet
```

Note, the path resolution logic for `ObjectStoreLocationProvider` is as follows:
- if `write.object-storage.path` is set, use it
- if not found, fallback to `write.folder-storage.path`
- if not found, use `<tableLocation>/data`

For more details, please refer to the [LocationProvider Configuration](../custom-catalog/#custom-location-provider-implementation) section.  

### S3 Strong Consistency

In November 2020, S3 announced [strong consistency](https://aws.amazon.com/s3/consistency/) for all read operations, and Iceberg is updated to fully leverage this feature.
There is no redundant consistency wait and check which might negatively impact performance during IO operations.

### Hadoop S3A FileSystem

Before `S3FileIO` was introduced, many Iceberg users choose to use `HadoopFileIO` to write data to S3 through the [S3A FileSystem](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java).
As introduced in the previous sections, `S3FileIO` adopts latest AWS clients and S3 features for optimized security and performance,
 and is thus recommend for S3 use cases rather than the S3A FileSystem.

`S3FileIO` writes data with `s3://` URI scheme, but it is also compatible with schemes written by the S3A FileSystem.
This means for any table manifests containing `s3a://` or `s3n://` file paths, `S3FileIO` is still able to read them.
This feature allows people to easily switch from S3A to `S3FileIO`.

If for any reason you have to use S3A, here are the instructions:

1. To store data using S3A, specify the `warehouse` catalog property to be an S3A path, e.g. `s3a://my-bucket/my-warehouse` 
2. For `HiveCatalog`, to also store metadata using S3A, specify the Hadoop config property `hive.metastore.warehouse.dir` to be an S3A path.
3. Add [hadoop-aws](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws) as a runtime dependency of your compute engine.
4. Configure AWS settings based on [hadoop-aws documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html) (make sure you check the version, S3A configuration varies a lot based on the version you use).   

## AWS Client Customization

Many organizations have customized their way of configuring AWS clients with their own credential provider, access proxy, retry strategy, etc.
Iceberg allows users to plug in their own implementation of `org.apache.iceberg.aws.AwsClientFactory` by setting the `client.factory` catalog property.

### Cross-Account and Cross-Region Access

It is a common use case for organizations to have a centralized AWS account for Glue metastore and S3 buckets, and use different AWS accounts and regions for different teams to access those resources.
In this case, a [cross-account IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html) is needed to access those centralized resources.
Iceberg provides an AWS client factory `AssumeRoleAwsClientFactory` to support this common use case.
This also serves as an example for users who would like to implement their own AWS client factory.

This client factory has the following configurable catalog properties:

| Property                          | Default                                  | Description                                            |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| client.assume-role.arn            | null, requires user input                | ARN of the role to assume, e.g. arn:aws:iam::123456789:role/myRoleToAssume  |
| client.assume-role.region         | null, requires user input                | All AWS clients except the STS client will use the given region instead of the default region chain  |
| client.assume-role.external-id    | null                                     | An optional [external ID](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html)  |
| client.assume-role.timeout-sec    | 1 hour                                   | Timeout of each assume role session. At the end of the timeout, a new set of role session credentials will be fetched through a STS client.  |

By using this client factory, an STS client is initialized with the default credential and region to assume the specified role.
The Glue, S3 and DynamoDB clients are then initialized with the assume-role credential and region to access resources.
Here is an example to start Spark shell with this client factory:

```shell
spark-sql --packages org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }},software.amazon.awssdk:bundle:2.15.40 \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/my/key/prefix \    
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.client.factory=org.apache.iceberg.aws.AssumeRoleAwsClientFactory \
    --conf spark.sql.catalog.my_catalog.client.assume-role.arn=arn:aws:iam::123456789:role/myRoleToAssume \
    --conf spark.sql.catalog.my_catalog.client.assume-role.region=ap-northeast-1
```

## Run Iceberg on AWS

### Amazon EMR

[Amazon EMR](https://aws.amazon.com/emr/) can provision clusters with [Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html) (EMR 6 for Spark 3, EMR 5 for Spark 2),
[Hive](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hive.html), [Flink](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-flink.html),
[Trino](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-presto.html) that can run Iceberg.

You can use a [bootstrap action](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-bootstrap.html) similar to the following to pre-install all necessary dependencies:

```sh
#!/bin/bash

AWS_SDK_VERSION=2.15.40
ICEBERG_VERSION={{ versions.iceberg }}
MAVEN_URL=https://repo1.maven.org/maven2
ICEBERG_MAVEN_URL=$MAVEN_URL/org/apache/iceberg
AWS_MAVEN_URL=$MAVEN_URL/software/amazon/awssdk
# NOTE: this is just an example shared class path between Spark and Flink,
#  please choose a proper class path for production.
LIB_PATH=/usr/share/aws/aws-java-sdk/

AWS_PACKAGES=(
  "bundle"
  "url-connection-client"
)

ICEBERG_PACKAGES=(
  "iceberg-spark3-runtime"
  "iceberg-flink-runtime"
)

install_dependencies () {
  install_path=$1
  download_url=$2
  version=$3
  shift
  pkgs=("$@")
  for pkg in "${pkgs[@]}"; do
    sudo wget -P $install_path $download_url/$pkg/$version/$pkg-$version.jar
  done
}

install_dependencies $LIB_PATH $ICEBERG_MAVEN_URL $ICEBERG_VERSION "${ICEBERG_PACKAGES[@]}"
install_dependencies $LIB_PATH $AWS_MAVEN_URL $AWS_SDK_VERSION "${AWS_PACKAGES[@]}"
```


### Amazon Kinesis

[Amazon Kinesis Data Analytics](https://aws.amazon.com/about-aws/whats-new/2019/11/you-can-now-run-fully-managed-apache-flink-applications-with-apache-kafka/) provides a platform 
to run fully managed Apache Flink applications. You can include Iceberg in your application Jar and run it in the platform.

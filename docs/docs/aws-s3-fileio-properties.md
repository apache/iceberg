### Language Implementations

| S3 Property                                                                               | Go | Java | Python | Rust |
|-------------------------------------------------------------------------------------------|---|---|---|---|
| [Progressive Multipart Upload](aws-s3-fileio-properties#progressive-multipart-upload)     |   | v |   |   |
| [S3 Server Side Encryption](aws-s3-fileio-properties#s3-server-side-encryption)           |   | v |   | v |
| [S3 Access Control List](aws-s3-fileio-properties#s3-access-control-list)                 |   | v |   |   |
| [S3 Endpoint](aws-s3-fileio-properties#s3-endpoint)                                       | v | v | v | v |
| [S3 Path Style Access](aws-s3-fileio-properties#s3-path-style-access)                     |   | v |   | v |
| [S3 Static Credentials](aws-s3-fileio-properties#s3-static-credentials)                   | v | v | v | v |
| [S3 Remote Signing](aws-s3-fileio-properties#s3-remote-signing)                           |   | v |   |   |
| [S3 Delete Batch Size](aws-s3-fileio-properties#s3-delete-batch-size)                     |   | v |   |   |
| [S3 Write Storage Class](aws-s3-fileio-properties#s3-write-storage-class)                 |   | v |   |   |
| [S3 Preload Client](aws-s3-fileio-properties#s3-preload-client)                           |   | v |   |   |
| [S3 Retries](aws-s3-fileio-properties#s3-retries)                                         |   | v |   |   |
| [S3 Write Checksum Verification](aws-s3-fileio-properties#s3-write-checksum-verification) |   | v |   |   |
| [S3 Tags](aws-s3-fileio-properties#s3-tags)                                               |   | v |   |   |
| [S3 Access Points](aws-s3-fileio-properties#s3-access-points)                             |   | v |   |   |
| [S3 Access Grants](aws-s3-fileio-properties#s3-access-grants)                             |   | v |   |   |
| [S3 Acceleration](aws-s3-fileio-properties#s3-acceleration)                               |   | v |   |   |
| [S3 Dual-stack](aws-s3-fileio-properties#s3-dual-stack)                                   |   | v |   |   |
| [S3 Signer](aws-s3-fileio-properties#s3-signer)                                           |   |   | v |   |
| [S3 Proxy](aws-s3-fileio-properties#s3-proxy)                                             | v |   | v |   |
| [S3 Connection Timeout](aws-s3-fileio-properties#s3-connection-timeout)                   |   |   | v |   |
| [S3 Region](aws-s3-fileio-properties#s3-region)                                           | v |   | v | v |
| [S3 Cross Region](aws-s3-fileio-properties#s3-cross-region-access)                        |   | v |   |   |

#### Implementations
- [Go](https://github.com/apache/iceberg-go/blob/main/io/s3.go)
- [Java](https://github.com/apache/iceberg/blob/main/aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIOProperties.java)
- [Python](https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/__init__.py)
- [Rust](https://github.com/apache/iceberg-rust/blob/main/crates/iceberg/src/io/storage_s3.rs)

---

### Progressive Multipart Upload

`S3FileIO` implements a customized progressive multipart upload algorithm to upload data.
Data files are uploaded by parts in parallel as soon as each part is ready,
and each file part is deleted as soon as its upload process completes.
This provides maximized upload speed and minimized local disk usage during uploads.
Here are the configurations that users can tune related to this feature:

| Language | Property                          | Default                                            | Description                                            |
|----------| --------------------------------- | -------------------------------------------------- | ------------------------------------------------------ |
| Java     | s3.multipart.num-threads          | the available number of processors in the system   | number of threads to use for uploading parts to S3 (shared across all output streams)  |
| Java     | s3.multipart.part-size-bytes      | 32MB                                               | the size of a single part for multipart upload requests  |
| Java     | s3.multipart.threshold            | 1.5                                                | the threshold expressed as a factor times the multipart size at which to switch from uploading using a single put object request to uploading using multipart upload  |
| Java     | s3.staging-dir                    | `java.io.tmpdir` property value                    | the directory to hold temporary files  |

### S3 Server Side Encryption

`S3FileIO` supports all 3 S3 server side encryption modes:

* [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html): When you use Server-Side Encryption with Amazon S3-Managed Keys (SSE-S3), each object is encrypted with a unique key. As an additional safeguard, it encrypts the key itself with a master key that it regularly rotates. Amazon S3 server-side encryption uses one of the strongest block ciphers available, 256-bit Advanced Encryption Standard (AES-256), to encrypt your data.
* [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html): Server-Side Encryption with Customer Master Keys (CMKs) Stored in AWS Key Management Service (SSE-KMS) is similar to SSE-S3, but with some additional benefits and charges for using this service. There are separate permissions for the use of a CMK that provides added protection against unauthorized access of your objects in Amazon S3. SSE-KMS also provides you with an audit trail that shows when your CMK was used and by whom. Additionally, you can create and manage customer managed CMKs or use AWS managed CMKs that are unique to you, your service, and your Region.
* [DSSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingDSSEncryption.html): Dual-layer Server-Side Encryption with AWS Key Management Service keys (DSSE-KMS) is similar to SSE-KMS, but applies two layers of encryption to objects when they are uploaded to Amazon S3. DSSE-KMS can be used to fulfill compliance standards that require you to apply multilayer encryption to your data and have full control of your encryption keys.
* [SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html): With Server-Side Encryption with Customer-Provided Keys (SSE-C), you manage the encryption keys and Amazon S3 manages the encryption, as it writes to disks, and decryption when you access your objects.

To enable server side encryption, use the following configuration properties:

| Language   | Property                          | Default                                  | Description                                            |
|------------| --------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| Java, Rust | s3.sse.type                       | `none`                                   | `none`, `s3`, `kms`, `dsse-kms` or `custom`            |
| Java, Rust | s3.sse.key                        | `aws/s3` for `kms` and `dsse-kms` types, null otherwise | A KMS Key ID or ARN for `kms` and `dsse-kms` types, or a custom base-64 AES256 symmetric key for `custom` type.  |
| Java, Rust | s3.sse.md5                        | null                                     | If SSE type is `custom`, this value must be set as the base-64 MD5 digest of the symmetric key to ensure integrity. |

### S3 Access Control List
`S3FileIO` supports S3 access control list (ACL) for detailed access control.

| Language | Property             | Default | Description                                   |
|----------|----------------------| ------- |-----------------------------------------------|
| Java     | s3.acl |  | |

For more details, please read [S3 ACL Documentation](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html).

### S3 Endpoint
`S3FileIO` can be configured to access an alternative endpoint of the S3 service.

| Language      | Property             | Default | Description                                   |
|---------------|----------------------| ------- |-----------------------------------------------|
| All Languages | s3.endpoint |  | |

### S3 Path Style Access
`S3FileIO` supports [Path-style requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#path-style-access).

| Language   | Property             | Default | Description                                   |
|------------|----------------------|---------|-----------------------------------------------|
| Java, Rust | s3.path-style-access | false   | |


For more details, please read [S3 Virtual Hosting Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)

### S3 Static Credentials
Configure the static access key ID / secret access key / session token used to access S3 service.

| Language      | Property             | Default | Description             |
|---------------|----------------------| ------- |-------------------------|
| All Languages | s3.access-key-id |  | For basic / session  credentials |
| All Languages | s3.secret-access-key |  | For basic / session  credentials |
| All Languages | s3.session-token |  | For session credentials |

### S3 Remote Signing
| Language | Property             | Default | Description                                   |
|----------|----------------------|---------|-----------------------------------------------|
| Java     | s3.remote-signing-enabled | false   | |

### S3 Delete Batch Size
Configure the number of keys to delete in one S3 delete request.

| Language | Property | Default | Description |
|----------| -------- | -------- | -------- |
| Java     | s3.delete.batch-size | 250 | The batch size when deleting files from a S3 bucket. Max is 1000 keys in one batch. |

For more details, please read [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)

### S3 Write Storage Class
Used by `S3FileIO` to tag objects' [storage class](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/StorageClass.html) when writing.

| Language | Property | Default  | Description |
|----------| -------- |----------| -------- |
| Java     | s3.write.storage-class | STANDARD | For more details, please read [S3 Storage Class](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html) |

### S3 Preload Client
User can initialize the S3 client during the `S3FileIO` initialization instead of default lazy initialization upon use.

| Language | Property | Default | Description |
|----------| -------- |---------| ------- |
| Java     | s3.preload-client-enabled | false   | |


### S3 Retries
Workloads which encounter S3 throttling should persistently retry, with exponential backoff, to make progress while S3
automatically scales. We provide the configurations below to adjust S3 retries for this purpose. For workloads that encounter
throttling and fail due to retry exhaustion, we recommend retry count to set 32 in order allow S3 to auto-scale. Note that
workloads with exceptionally high throughput against tables that S3 has not yet scaled, it may be necessary to increase the retry count further.


| Language | Property             | Default | Description                                                                           |
|----------|----------------------|---------|---------------------------------------------------------------------------------------|
| Java     | s3.retry.num-retries | 5       | Number of times to retry S3 operations. Recommended 32 for high-throughput workloads. |
| Java     | s3.retry.min-wait-ms | 2s      | Minimum wait time to retry a S3 operation.                                            |
| Java     | s3.retry.max-wait-ms | 20s     | Maximum wait time to retry a S3 read operation.                                       |

### S3 Write Checksum Verification

To ensure integrity of uploaded objects, checksum validations for S3 writes can be turned on by setting catalog property `s3.checksum-enabled` to `true`.

| Language | Property | Default | Description |
|----------| -------- |---------| ------- |
| Java     | s3.checksum-enabled | false   | |

### S3 Tags

Custom [tags](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html) can be added to S3 objects while writing and deleting.
For example, to write S3 tags with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.write.tags.my_key1=my_val1 \
    --conf spark.sql.catalog.my_catalog.s3.write.tags.my_key2=my_val2
```
For the above example, the objects in S3 will be saved with tags: `my_key1=my_val1` and `my_key2=my_val2`. Do note that the specified write tags will be saved only while object creation.

When the catalog property `s3.delete-enabled` is set to `false`, the objects are not hard-deleted from S3.
This is expected to be used in combination with S3 delete tagging, so objects are tagged and removed using [S3 lifecycle policy](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html).
The property is set to `true` by default.

With the `s3.delete.tags` config, objects are tagged with the configured key-value pairs before deletion.
Users can configure tag-based object lifecycle policy at bucket level to transition objects to different tiers.
For example, to add S3 delete tags with Spark 3.3, you can start the Spark SQL shell with:

```
sh spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://iceberg-warehouse/s3-tagging \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.delete.tags.my_key3=my_val3 \
    --conf spark.sql.catalog.my_catalog.s3.delete-enabled=false
```

For the above example, the objects in S3 will be saved with tags: `my_key3=my_val3` before deletion.
Users can also use the catalog property `s3.delete.num-threads` to mention the number of threads to be used for adding delete tags to the S3 objects.

When the catalog property `s3.write.table-tag-enabled` and `s3.write.namespace-tag-enabled` is set to `true` then the objects in S3 will be saved with tags: `iceberg.table=<table-name>` and `iceberg.namespace=<namespace-name>`.
Users can define access and data retention policy per namespace or table based on these tags.
For example, to write table and namespace name as S3 tags with Spark 3.3, you can start the Spark SQL shell with:
```
sh spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://iceberg-warehouse/s3-tagging \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.write.table-tag-enabled=true \
    --conf spark.sql.catalog.my_catalog.s3.write.namespace-tag-enabled=true
```
For more details on tag restrictions, please refer [User-Defined Tag Restrictions](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html).

### S3 Access Points

[Access Points](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html) can be used to perform
S3 operations by specifying a mapping of bucket to access points. This is useful for multi-region access, cross-region access,
disaster recovery, etc.

For using cross-region access points, we need to additionally set `use-arn-region-enabled` catalog property to
`true` to enable `S3FileIO` to make cross-region calls, it's not required for same / multi-region access points.

For example, to use S3 access-point with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket2/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.use-arn-region-enabled=false \
    --conf spark.sql.catalog.my_catalog.s3.access-points.my-bucket1=arn:aws:s3::<ACCOUNT_ID>:accesspoint/<MRAP_ALIAS> \
    --conf spark.sql.catalog.my_catalog.s3.access-points.my-bucket2=arn:aws:s3::<ACCOUNT_ID>:accesspoint/<MRAP_ALIAS>
```
For the above example, the objects in S3 on `my-bucket1` and `my-bucket2` buckets will use `arn:aws:s3::<ACCOUNT_ID>:accesspoint/<MRAP_ALIAS>`
access-point for all S3 operations.

For more details on using access-points, please refer [Using access points with compatible Amazon S3 operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points-usage-examples.html), [Sample notebook](https://github.com/aws-samples/quant-research/tree/main) .

### S3 Access Grants

[S3 Access Grants](https://aws.amazon.com/s3/features/access-grants/) can be used to grant accesses to S3 data using IAM Principals.
In order to enable S3 Access Grants to work in Iceberg, you can set the `s3.access-grants.enabled` catalog property to `true` after
you add the [S3 Access Grants Plugin jar](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2) to your classpath. A link
to the Maven listing for this plugin can be found [here](https://mvnrepository.com/artifact/software.amazon.s3.accessgrants/aws-s3-accessgrants-java-plugin).

In addition, we allow the [fallback-to-IAM configuration](https://github.com/aws/aws-s3-accessgrants-plugin-java-v2) which allows
you to fallback to using your IAM role (and its permission sets directly) to access your S3 data in the case the S3 Access Grants
is unable to authorize your S3 call. This can be done using the `s3.access-grants.fallback-to-iam` boolean catalog property. By default,
this property is set to `false`.

For example, to add the S3 Access Grants Integration with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket2/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.access-grants.enabled=true \
    --conf spark.sql.catalog.my_catalog.s3.access-grants.fallback-to-iam=true
```

For more details on using S3 Access Grants, please refer to [Managing access with S3 Access Grants](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants.html).

### S3 Acceleration

[S3 Acceleration](https://aws.amazon.com/s3/transfer-acceleration/) can be used to speed up transfers to and from Amazon S3 by as much as 50-500% for long-distance transfer of larger objects.

To use S3 Acceleration, we need to set `s3.acceleration-enabled` catalog property to `true` to enable `S3FileIO` to make accelerated S3 calls.

For example, to use S3 Acceleration with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket2/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.acceleration-enabled=true
```

For more details on using S3 Acceleration, please refer to [Configuring fast, secure file transfers using Amazon S3 Transfer Acceleration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/transfer-acceleration.html).

### S3 Dual-stack

[S3 Dual-stack](https://docs.aws.amazon.com/AmazonS3/latest/userguide/dual-stack-endpoints.html) allows a client to access an S3 bucket through a dual-stack endpoint.
When clients request a dual-stack endpoint, the bucket URL resolves to an IPv6 address if possible, otherwise fallback to IPv4.

To use S3 Dual-stack, we need to set `s3.dualstack-enabled` catalog property to `true` to enable `S3FileIO` to make dual-stack S3 calls.

For example, to use S3 Dual-stack with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket2/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.dualstack-enabled=true
```

For more details on using S3 Dual-stack, please refer [Using dual-stack endpoints from the AWS CLI and the AWS SDKs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/dual-stack-endpoints.html#dual-stack-endpoints-cli)

### S3 Signer
| Language                    | Property             | Default | Description                                    |
|-----------------------------|----------------------| ------- |------------------------------------------------|
| Python | s3.signer |  | Name of the signer in the `SIGNERS` dictionary |
| Python | s3.signer.uri |  |                                                |
| Python | s3.signer.endpoint | v1/aws/s3/sign |                                                |

For more details on authenticating S3 request, please refer [Authenticating Requests (AWS Signature Version 4)](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)

### S3 Proxy
| Language | Property             | Default | Description                                   |
|----------|----------------------| ------- |-----------------------------------------------|
| Go, Python     | s3.proxy-uri |  | |

### S3 Connection Timeout
| Language                    | Property             | Default | Description                                   |
|-----------------------------|----------------------| ------- |-----------------------------------------------|
| Python | s3.connect-timeout |  | |

### S3 Region
| Language         | Property             | Default | Description                                   |
|------------------|----------------------| ------- |-----------------------------------------------|
| Go, Python, Rust | s3.region |  | |

### S3 Cross-Region Access

S3 Cross-Region bucket access can be turned on by setting catalog property `s3.cross-region-access-enabled` to `true`.
This is turned off by default to avoid first S3 API call increased latency.

For example, to enable S3 Cross-Region bucket access with Spark 3.3, you can start the Spark SQL shell with:
```
spark-sql --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://my-bucket2/my/key/prefix \
    --conf spark.sql.catalog.my_catalog.type=glue \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.s3.cross-region-access-enabled=true
```

For more details, please refer to [Cross-Region access for Amazon S3](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-cross-region.html).

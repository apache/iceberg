/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Tag;

public class AwsProperties implements Serializable {

  /**
   * Type of S3 Server side encryption used, default to {@link
   * AwsProperties#S3FILEIO_SSE_TYPE_NONE}.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
   */
  public static final String S3FILEIO_SSE_TYPE = "s3.sse.type";

  /** No server side encryption. */
  public static final String S3FILEIO_SSE_TYPE_NONE = "none";

  /**
   * S3 SSE-KMS encryption.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_KMS = "kms";

  /**
   * S3 SSE-S3 encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_S3 = "s3";

  /**
   * S3 SSE-C encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   */
  public static final String S3FILEIO_SSE_TYPE_CUSTOM = "custom";

  /**
   * If S3 encryption type is SSE-KMS, input is a KMS Key ID or ARN. In case this property is not
   * set, default key "aws/s3" is used. If encryption type is SSE-C, input is a custom base-64
   * AES256 symmetric key.
   */
  public static final String S3FILEIO_SSE_KEY = "s3.sse.key";

  /**
   * If S3 encryption type is SSE-C, input is the base-64 MD5 digest of the secret key. This MD5
   * must be explicitly passed in by the caller to ensure key integrity.
   */
  public static final String S3FILEIO_SSE_MD5 = "s3.sse.md5";

  /**
   * The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
   * automatically uses the caller's AWS account ID by default.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
   */
  public static final String GLUE_CATALOG_ID = "glue.id";

  /**
   * The account ID used in a Glue resource ARN, e.g.
   * arn:aws:glue:us-east-1:1000000000000:table/db1/table1
   */
  public static final String GLUE_ACCOUNT_ID = "glue.account-id";

  /**
   * If Glue should skip archiving an old table version when creating a new version in a commit. By
   * default Glue archives all old table versions after an UpdateTable call, but Glue has a default
   * max number of archived table versions (can be increased). So for streaming use case with lots
   * of commits, it is recommended to set this value to true.
   */
  public static final String GLUE_CATALOG_SKIP_ARCHIVE = "glue.skip-archive";

  public static final boolean GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = false;

  /**
   * If Glue should skip name validations It is recommended to stick to Glue best practice in
   * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html to make sure operations
   * are Hive compatible. This is only added for users that have existing conventions using
   * non-standard characters. When database name and table name validation are skipped, there is no
   * guarantee that downstream systems would all support the names.
   */
  public static final String GLUE_CATALOG_SKIP_NAME_VALIDATION = "glue.skip-name-validation";

  public static final boolean GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT = false;

  /**
   * If set, GlueCatalog will use Lake Formation for access control. For more credential vending
   * details, see: https://docs.aws.amazon.com/lake-formation/latest/dg/api-overview.html. If
   * enabled, the {@link AwsClientFactory} implementation must be {@link
   * LakeFormationAwsClientFactory} or any class that extends it.
   */
  public static final String GLUE_LAKEFORMATION_ENABLED = "glue.lakeformation-enabled";

  public static final boolean GLUE_LAKEFORMATION_ENABLED_DEFAULT = false;

  /**
   * Number of threads to use for uploading parts to S3 (shared pool across all output streams),
   * default to {@link Runtime#availableProcessors()}
   */
  public static final String S3FILEIO_MULTIPART_UPLOAD_THREADS = "s3.multipart.num-threads";

  /**
   * The size of a single part for multipart upload requests in bytes (default: 32MB). based on S3
   * requirement, the part size must be at least 5MB. Too ensure performance of the reader and
   * writer, the part size must be less than 2GB.
   *
   * <p>For more details, see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  public static final String S3FILEIO_MULTIPART_SIZE = "s3.multipart.part-size-bytes";

  public static final int S3FILEIO_MULTIPART_SIZE_DEFAULT = 32 * 1024 * 1024;
  public static final int S3FILEIO_MULTIPART_SIZE_MIN = 5 * 1024 * 1024;

  /**
   * The threshold expressed as a factor times the multipart size at which to switch from uploading
   * using a single put object request to uploading using multipart upload (default: 1.5).
   */
  public static final String S3FILEIO_MULTIPART_THRESHOLD_FACTOR = "s3.multipart.threshold";

  public static final double S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT = 1.5;

  /**
   * Location to put staging files for upload to S3, default to temp directory set in
   * java.io.tmpdir.
   */
  public static final String S3FILEIO_STAGING_DIRECTORY = "s3.staging-dir";

  /**
   * Used to configure canned access control list (ACL) for S3 client to use during write. If not
   * set, ACL will not be set for requests.
   *
   * <p>The input must be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL},
   * such as 'public-read-write' For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   */
  public static final String S3FILEIO_ACL = "s3.acl";

  /**
   * Configure an alternative endpoint of the S3 service for S3FileIO to access.
   *
   * <p>This could be used to use S3FileIO with any s3-compatible object storage service that has a
   * different endpoint, or access a private S3 endpoint in a virtual private cloud.
   */
  public static final String S3FILEIO_ENDPOINT = "s3.endpoint";

  /**
   * If set {@code true}, requests to S3FileIO will use Path-Style, otherwise, Virtual Hosted-Style
   * will be used.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
   */
  public static final String S3FILEIO_PATH_STYLE_ACCESS = "s3.path-style-access";

  public static final boolean S3FILEIO_PATH_STYLE_ACCESS_DEFAULT = false;

  /**
   * Configure the static access key ID used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #S3FILEIO_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is
   * used.
   */
  public static final String S3FILEIO_ACCESS_KEY_ID = "s3.access-key-id";

  /**
   * Configure the static secret access key used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #S3FILEIO_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is
   * used.
   */
  public static final String S3FILEIO_SECRET_ACCESS_KEY = "s3.secret-access-key";

  /**
   * Configure the static session token used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the session credentials provided instead of
   * reading the default credential chain to create S3 access credentials.
   */
  public static final String S3FILEIO_SESSION_TOKEN = "s3.session-token";

  /**
   * Enable to make S3FileIO, to make cross-region call to the region specified in the ARN of an
   * access point.
   *
   * <p>By default, attempting to use an access point in a different region will throw an exception.
   * When enabled, this property allows using access points in other regions.
   *
   * <p>For more details see:
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Configuration.html#useArnRegionEnabled--
   */
  public static final String S3_USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";

  public static final boolean S3_USE_ARN_REGION_ENABLED_DEFAULT = false;

  /** Enables eTag checks for S3 PUT and MULTIPART upload requests. */
  public static final String S3_CHECKSUM_ENABLED = "s3.checksum-enabled";

  public static final boolean S3_CHECKSUM_ENABLED_DEFAULT = false;

  /** Configure the batch size used when deleting multiple files from a given S3 bucket */
  public static final String S3FILEIO_DELETE_BATCH_SIZE = "s3.delete.batch-size";

  /**
   * Default batch size used when deleting files.
   *
   * <p>Refer to https://github.com/apache/hadoop/commit/56dee667707926f3796c7757be1a133a362f05c9
   * for more details on why this value was chosen.
   */
  public static final int S3FILEIO_DELETE_BATCH_SIZE_DEFAULT = 250;

  /**
   * Max possible batch size for deletion. Currently, a max of 1000 keys can be deleted in one
   * batch. https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
   */
  public static final int S3FILEIO_DELETE_BATCH_SIZE_MAX = 1000;

  /** Configure an alternative endpoint of the DynamoDB service to access. */
  public static final String DYNAMODB_ENDPOINT = "dynamodb.endpoint";

  /** DynamoDB table name for {@link DynamoDbCatalog} */
  public static final String DYNAMODB_TABLE_NAME = "dynamodb.table-name";

  public static final String DYNAMODB_TABLE_NAME_DEFAULT = "iceberg";

  /**
   * The implementation class of {@link AwsClientFactory} to customize AWS client configurations. If
   * set, all AWS clients will be initialized by the specified factory. If not set, {@link
   * AwsClientFactories#defaultFactory()} is used as default factory.
   */
  public static final String CLIENT_FACTORY = "client.factory";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. If set, all AWS clients will assume a role of the
   * given ARN, instead of using the default credential chain.
   */
  public static final String CLIENT_ASSUME_ROLE_ARN = "client.assume-role.arn";

  /**
   * Used by {@link AssumeRoleAwsClientFactory} to pass a list of sessions. Each session tag
   * consists of a key name and an associated value.
   */
  public static final String CLIENT_ASSUME_ROLE_TAGS_PREFIX = "client.assume-role.tags.";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. The timeout of the assume role session in seconds,
   * default to 1 hour. At the end of the timeout, a new set of role session credentials will be
   * fetched through a STS client.
   */
  public static final String CLIENT_ASSUME_ROLE_TIMEOUT_SEC = "client.assume-role.timeout-sec";

  public static final int CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT = 3600;

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. Optional external ID used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
   */
  public static final String CLIENT_ASSUME_ROLE_EXTERNAL_ID = "client.assume-role.external-id";

  /**
   * Used by {@link AssumeRoleAwsClientFactory}. If set, all AWS clients except STS client will use
   * the given region instead of the default region chain.
   *
   * <p>The value must be one of {@link software.amazon.awssdk.regions.Region}, such as 'us-east-1'.
   * For more details, see https://docs.aws.amazon.com/general/latest/gr/rande.html
   */
  public static final String CLIENT_ASSUME_ROLE_REGION = "client.assume-role.region";

  /**
   * The type of {@link software.amazon.awssdk.http.SdkHttpClient} implementation used by {@link
   * AwsClientFactory} If set, all AWS clients will use this specified HTTP client. If not set,
   * {@link #HTTP_CLIENT_TYPE_DEFAULT} will be used. For specific types supported, see
   * HTTP_CLIENT_TYPE_* defined below.
   */
  public static final String HTTP_CLIENT_TYPE = "http-client.type";

  /**
   * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} will be used as the HTTP
   * Client in {@link AwsClientFactory}
   */
  public static final String HTTP_CLIENT_TYPE_URLCONNECTION = "urlconnection";

  /**
   * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient} will be used as the HTTP Client in {@link
   * AwsClientFactory}
   */
  public static final String HTTP_CLIENT_TYPE_APACHE = "apache";

  public static final String HTTP_CLIENT_TYPE_DEFAULT = HTTP_CLIENT_TYPE_URLCONNECTION;

  /**
   * Used by {@link S3FileIO} to tag objects when writing. To set, we can pass a catalog property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.tags.my_key=my_val
   */
  public static final String S3_WRITE_TAGS_PREFIX = "s3.write.tags.";

  /**
   * Used by {@link S3FileIO} to tag objects when deleting. When this config is set, objects are
   * tagged with the configured key-value pairs before deletion. This is considered a soft-delete,
   * because users are able to configure tag-based object lifecycle policy at bucket level to
   * transition objects to different tiers.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
   *
   * <p>Example: s3.delete.tags.my_key=my_val
   */
  public static final String S3_DELETE_TAGS_PREFIX = "s3.delete.tags.";

  /**
   * Number of threads to use for adding delete tags to S3 objects, default to {@link
   * Runtime#availableProcessors()}
   */
  public static final String S3FILEIO_DELETE_THREADS = "s3.delete.num-threads";

  /**
   * Determines if {@link S3FileIO} deletes the object when io.delete() is called, default to true.
   * Once disabled, users are expected to set tags through {@link #S3_DELETE_TAGS_PREFIX} and manage
   * deleted files through S3 lifecycle policy.
   */
  public static final String S3_DELETE_ENABLED = "s3.delete-enabled";

  public static final boolean S3_DELETE_ENABLED_DEFAULT = true;

  /**
   * Used by {@link S3FileIO}, prefix used for bucket access point configuration. To set, we can
   * pass a catalog property.
   *
   * <p>For more details, see https://aws.amazon.com/s3/features/access-points/
   *
   * <p>Example: s3.access-points.my-bucket=access-point
   */
  public static final String S3_ACCESS_POINTS_PREFIX = "s3.access-points.";

  /**
   * @deprecated will be removed at 0.15.0, please use {@link #S3_CHECKSUM_ENABLED_DEFAULT} instead
   */
  @Deprecated public static final boolean CLIENT_ENABLE_ETAG_CHECK_DEFAULT = false;

  /**
   * Used by {@link LakeFormationAwsClientFactory}. The table name used as part of lake formation
   * credentials request.
   */
  public static final String LAKE_FORMATION_TABLE_NAME = "lakeformation.table-name";

  /**
   * Used by {@link LakeFormationAwsClientFactory}. The database name used as part of lake formation
   * credentials request.
   */
  public static final String LAKE_FORMATION_DB_NAME = "lakeformation.db-name";

  private String s3FileIoSseType;
  private String s3FileIoSseKey;
  private String s3FileIoSseMd5;
  private int s3FileIoMultipartUploadThreads;
  private int s3FileIoMultiPartSize;
  private int s3FileIoDeleteBatchSize;
  private double s3FileIoMultipartThresholdFactor;
  private String s3fileIoStagingDirectory;
  private ObjectCannedACL s3FileIoAcl;
  private boolean isS3ChecksumEnabled;
  private final Set<Tag> s3WriteTags;
  private final Set<Tag> s3DeleteTags;
  private int s3FileIoDeleteThreads;
  private boolean isS3DeleteEnabled;
  private final Map<String, String> s3BucketToAccessPointMapping;

  private String glueCatalogId;
  private boolean glueCatalogSkipArchive;
  private boolean glueCatalogSkipNameValidation;
  private boolean glueLakeFormationEnabled;

  private String dynamoDbTableName;

  public AwsProperties() {
    this.s3FileIoSseType = S3FILEIO_SSE_TYPE_NONE;
    this.s3FileIoSseKey = null;
    this.s3FileIoSseMd5 = null;
    this.s3FileIoAcl = null;

    this.s3FileIoMultipartUploadThreads = Runtime.getRuntime().availableProcessors();
    this.s3FileIoMultiPartSize = S3FILEIO_MULTIPART_SIZE_DEFAULT;
    this.s3FileIoMultipartThresholdFactor = S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT;
    this.s3FileIoDeleteBatchSize = S3FILEIO_DELETE_BATCH_SIZE_DEFAULT;
    this.s3fileIoStagingDirectory = System.getProperty("java.io.tmpdir");
    this.isS3ChecksumEnabled = S3_CHECKSUM_ENABLED_DEFAULT;
    this.s3WriteTags = Sets.newHashSet();
    this.s3DeleteTags = Sets.newHashSet();
    this.s3FileIoDeleteThreads = Runtime.getRuntime().availableProcessors();
    this.isS3DeleteEnabled = S3_DELETE_ENABLED_DEFAULT;
    this.s3BucketToAccessPointMapping = ImmutableMap.of();

    this.glueCatalogId = null;
    this.glueCatalogSkipArchive = GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT;
    this.glueCatalogSkipNameValidation = GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT;
    this.glueLakeFormationEnabled = GLUE_LAKEFORMATION_ENABLED_DEFAULT;

    this.dynamoDbTableName = DYNAMODB_TABLE_NAME_DEFAULT;
  }

  public AwsProperties(Map<String, String> properties) {
    this.s3FileIoSseType =
        properties.getOrDefault(
            AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_NONE);
    this.s3FileIoSseKey = properties.get(AwsProperties.S3FILEIO_SSE_KEY);
    this.s3FileIoSseMd5 = properties.get(AwsProperties.S3FILEIO_SSE_MD5);
    if (AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM.equals(s3FileIoSseType)) {
      Preconditions.checkNotNull(
          s3FileIoSseKey, "Cannot initialize SSE-C S3FileIO with null encryption key");
      Preconditions.checkNotNull(
          s3FileIoSseMd5, "Cannot initialize SSE-C S3FileIO with null encryption key MD5");
    }

    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueCatalogSkipArchive =
        PropertyUtil.propertyAsBoolean(
            properties,
            AwsProperties.GLUE_CATALOG_SKIP_ARCHIVE,
            AwsProperties.GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);
    this.glueCatalogSkipNameValidation =
        PropertyUtil.propertyAsBoolean(
            properties,
            AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION,
            AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT);
    this.glueLakeFormationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_LAKEFORMATION_ENABLED, GLUE_LAKEFORMATION_ENABLED_DEFAULT);

    this.s3FileIoMultipartUploadThreads =
        PropertyUtil.propertyAsInt(
            properties,
            S3FILEIO_MULTIPART_UPLOAD_THREADS,
            Runtime.getRuntime().availableProcessors());

    try {
      this.s3FileIoMultiPartSize =
          PropertyUtil.propertyAsInt(
              properties, S3FILEIO_MULTIPART_SIZE, S3FILEIO_MULTIPART_SIZE_DEFAULT);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Input malformed or exceeded maximum multipart upload size 5GB: %s"
              + properties.get(S3FILEIO_MULTIPART_SIZE));
    }

    this.s3FileIoMultipartThresholdFactor =
        PropertyUtil.propertyAsDouble(
            properties,
            S3FILEIO_MULTIPART_THRESHOLD_FACTOR,
            S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT);

    Preconditions.checkArgument(
        s3FileIoMultipartThresholdFactor >= 1.0, "Multipart threshold factor must be >= to 1.0");

    Preconditions.checkArgument(
        s3FileIoMultiPartSize >= S3FILEIO_MULTIPART_SIZE_MIN,
        "Minimum multipart upload object size must be larger than 5 MB.");

    this.s3fileIoStagingDirectory =
        PropertyUtil.propertyAsString(
            properties, S3FILEIO_STAGING_DIRECTORY, System.getProperty("java.io.tmpdir"));

    String aclType = properties.get(S3FILEIO_ACL);
    this.s3FileIoAcl = ObjectCannedACL.fromValue(aclType);
    Preconditions.checkArgument(
        s3FileIoAcl == null || !s3FileIoAcl.equals(ObjectCannedACL.UNKNOWN_TO_SDK_VERSION),
        "Cannot support S3 CannedACL " + aclType);

    this.isS3ChecksumEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_CHECKSUM_ENABLED, S3_CHECKSUM_ENABLED_DEFAULT);

    this.s3FileIoDeleteBatchSize =
        PropertyUtil.propertyAsInt(
            properties, S3FILEIO_DELETE_BATCH_SIZE, S3FILEIO_DELETE_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(
        s3FileIoDeleteBatchSize > 0 && s3FileIoDeleteBatchSize <= S3FILEIO_DELETE_BATCH_SIZE_MAX,
        String.format(
            "Deletion batch size must be between 1 and %s", S3FILEIO_DELETE_BATCH_SIZE_MAX));

    this.s3WriteTags = toTags(properties, S3_WRITE_TAGS_PREFIX);
    this.s3DeleteTags = toTags(properties, S3_DELETE_TAGS_PREFIX);
    this.s3FileIoDeleteThreads =
        PropertyUtil.propertyAsInt(
            properties, S3FILEIO_DELETE_THREADS, Runtime.getRuntime().availableProcessors());
    this.isS3DeleteEnabled =
        PropertyUtil.propertyAsBoolean(properties, S3_DELETE_ENABLED, S3_DELETE_ENABLED_DEFAULT);
    this.s3BucketToAccessPointMapping =
        PropertyUtil.propertiesWithPrefix(properties, S3_ACCESS_POINTS_PREFIX);

    this.dynamoDbTableName =
        PropertyUtil.propertyAsString(properties, DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME_DEFAULT);
  }

  public String s3FileIoSseType() {
    return s3FileIoSseType;
  }

  public void setS3FileIoSseType(String sseType) {
    this.s3FileIoSseType = sseType;
  }

  public String s3FileIoSseKey() {
    return s3FileIoSseKey;
  }

  public int s3FileIoDeleteBatchSize() {
    return s3FileIoDeleteBatchSize;
  }

  public void setS3FileIoDeleteBatchSize(int deleteBatchSize) {
    this.s3FileIoDeleteBatchSize = deleteBatchSize;
  }

  public void setS3FileIoSseKey(String sseKey) {
    this.s3FileIoSseKey = sseKey;
  }

  public String s3FileIoSseMd5() {
    return s3FileIoSseMd5;
  }

  public void setS3FileIoSseMd5(String sseMd5) {
    this.s3FileIoSseMd5 = sseMd5;
  }

  public String glueCatalogId() {
    return glueCatalogId;
  }

  public void setGlueCatalogId(String id) {
    this.glueCatalogId = id;
  }

  public boolean glueCatalogSkipArchive() {
    return glueCatalogSkipArchive;
  }

  public void setGlueCatalogSkipArchive(boolean skipArchive) {
    this.glueCatalogSkipArchive = skipArchive;
  }

  public boolean glueCatalogSkipNameValidation() {
    return glueCatalogSkipNameValidation;
  }

  public void setGlueCatalogSkipNameValidation(boolean glueCatalogSkipNameValidation) {
    this.glueCatalogSkipNameValidation = glueCatalogSkipNameValidation;
  }

  public boolean glueLakeFormationEnabled() {
    return glueLakeFormationEnabled;
  }

  public void setGlueLakeFormationEnabled(boolean glueLakeFormationEnabled) {
    this.glueLakeFormationEnabled = glueLakeFormationEnabled;
  }

  public int s3FileIoMultipartUploadThreads() {
    return s3FileIoMultipartUploadThreads;
  }

  public void setS3FileIoMultipartUploadThreads(int threads) {
    this.s3FileIoMultipartUploadThreads = threads;
  }

  public int s3FileIoMultiPartSize() {
    return s3FileIoMultiPartSize;
  }

  public void setS3FileIoMultiPartSize(int size) {
    this.s3FileIoMultiPartSize = size;
  }

  public double s3FileIOMultipartThresholdFactor() {
    return s3FileIoMultipartThresholdFactor;
  }

  public void setS3FileIoMultipartThresholdFactor(double factor) {
    this.s3FileIoMultipartThresholdFactor = factor;
  }

  public String s3fileIoStagingDirectory() {
    return s3fileIoStagingDirectory;
  }

  public void setS3fileIoStagingDirectory(String directory) {
    this.s3fileIoStagingDirectory = directory;
  }

  public ObjectCannedACL s3FileIoAcl() {
    return this.s3FileIoAcl;
  }

  public void setS3FileIoAcl(ObjectCannedACL acl) {
    this.s3FileIoAcl = acl;
  }

  public String dynamoDbTableName() {
    return dynamoDbTableName;
  }

  public void setDynamoDbTableName(String name) {
    this.dynamoDbTableName = name;
  }

  public boolean isS3ChecksumEnabled() {
    return this.isS3ChecksumEnabled;
  }

  public void setS3ChecksumEnabled(boolean eTagCheckEnabled) {
    this.isS3ChecksumEnabled = eTagCheckEnabled;
  }

  public Set<Tag> s3WriteTags() {
    return s3WriteTags;
  }

  public Set<Tag> s3DeleteTags() {
    return s3DeleteTags;
  }

  public int s3FileIoDeleteThreads() {
    return s3FileIoDeleteThreads;
  }

  public void setS3FileIoDeleteThreads(int threads) {
    this.s3FileIoDeleteThreads = threads;
  }

  public boolean isS3DeleteEnabled() {
    return isS3DeleteEnabled;
  }

  public void setS3DeleteEnabled(boolean s3DeleteEnabled) {
    this.isS3DeleteEnabled = s3DeleteEnabled;
  }

  private Set<Tag> toTags(Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
        .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toSet());
  }

  public Map<String, String> s3BucketToAccessPointMapping() {
    return s3BucketToAccessPointMapping;
  }
}

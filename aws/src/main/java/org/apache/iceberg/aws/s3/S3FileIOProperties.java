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
package org.apache.iceberg.aws.s3;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Tag;

public class S3FileIOProperties implements Serializable {
  /**
   * This property is used to pass in the aws client factory implementation class for S3 FileIO. The
   * class should implement {@link S3FileIOAwsClientFactory}. For example, {@link
   * DefaultS3FileIOAwsClientFactory} implements {@link S3FileIOAwsClientFactory}. If this property
   * wasn't set, will load one of {@link org.apache.iceberg.aws.AwsClientFactory} factory classes to
   * provide backward compatibility.
   */
  public static final String CLIENT_FACTORY = "s3.client-factory-impl";

  /**
   * Type of S3 Server side encryption used, default to {@link S3FileIOProperties#SSE_TYPE_NONE}.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
   */
  public static final String SSE_TYPE = "s3.sse.type";

  /** No server side encryption. */
  public static final String SSE_TYPE_NONE = "none";

  /**
   * S3 SSE-KMS encryption.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   */
  public static final String SSE_TYPE_KMS = "kms";

  /**
   * S3 SSE-S3 encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
   */
  public static final String SSE_TYPE_S3 = "s3";

  /**
   * S3 SSE-C encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   */
  public static final String SSE_TYPE_CUSTOM = "custom";

  /**
   * If S3 encryption type is SSE-KMS, input is a KMS Key ID or ARN. In case this property is not
   * set, default key "aws/s3" is used. If encryption type is SSE-C, input is a custom base-64
   * AES256 symmetric key.
   */
  public static final String SSE_KEY = "s3.sse.key";

  /**
   * If S3 encryption type is SSE-C, input is the base-64 MD5 digest of the secret key. This MD5
   * must be explicitly passed in by the caller to ensure key integrity.
   */
  public static final String SSE_MD5 = "s3.sse.md5";

  /**
   * Number of threads to use for uploading parts to S3 (shared pool across all output streams),
   * default to {@link Runtime#availableProcessors()}
   */
  public static final String MULTIPART_UPLOAD_THREADS = "s3.multipart.num-threads";

  /**
   * The size of a single part for multipart upload requests in bytes (default: 32MB). based on S3
   * requirement, the part size must be at least 5MB. To ensure performance of the reader and
   * writer, the part size must be less than 2GB.
   *
   * <p>For more details, see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   */
  public static final String MULTIPART_SIZE = "s3.multipart.part-size-bytes";

  public static final int MULTIPART_SIZE_DEFAULT = 32 * 1024 * 1024;
  public static final int MULTIPART_SIZE_MIN = 5 * 1024 * 1024;

  /**
   * The threshold expressed as a factor times the multipart size at which to switch from uploading
   * using a single put object request to uploading using multipart upload (default: 1.5).
   */
  public static final String MULTIPART_THRESHOLD_FACTOR = "s3.multipart.threshold";

  public static final double MULTIPART_THRESHOLD_FACTOR_DEFAULT = 1.5;

  /**
   * Location to put staging files for upload to S3, default to temp directory set in
   * java.io.tmpdir.
   */
  public static final String STAGING_DIRECTORY = "s3.staging-dir";

  /**
   * Used to configure canned access control list (ACL) for S3 client to use during write. If not
   * set, ACL will not be set for requests.
   *
   * <p>The input must be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL},
   * such as 'public-read-write' For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   */
  public static final String ACL = "s3.acl";

  /**
   * Configure an alternative endpoint of the S3 service for S3FileIO to access.
   *
   * <p>This could be used to use S3FileIO with any s3-compatible object storage service that has a
   * different endpoint, or access a private S3 endpoint in a virtual private cloud.
   */
  public static final String ENDPOINT = "s3.endpoint";

  /**
   * If set {@code true}, requests to S3FileIO will use Path-Style, otherwise, Virtual Hosted-Style
   * will be used.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
   */
  public static final String PATH_STYLE_ACCESS = "s3.path-style-access";

  public static final boolean PATH_STYLE_ACCESS_DEFAULT = false;

  /**
   * Configure the static access key ID used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String ACCESS_KEY_ID = "s3.access-key-id";

  /**
   * Configure the static secret access key used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String SECRET_ACCESS_KEY = "s3.secret-access-key";

  /**
   * Configure the static session token used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the session credentials provided instead of
   * reading the default credential chain to create S3 access credentials.
   */
  public static final String SESSION_TOKEN = "s3.session-token";

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
  public static final String USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";

  public static final boolean USE_ARN_REGION_ENABLED_DEFAULT = false;

  /** Enables eTag checks for S3 PUT and MULTIPART upload requests. */
  public static final String CHECKSUM_ENABLED = "s3.checksum-enabled";

  public static final boolean CHECKSUM_ENABLED_DEFAULT = false;

  public static final String REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";

  public static final boolean REMOTE_SIGNING_ENABLED_DEFAULT = false;

  /** Configure the batch size used when deleting multiple files from a given S3 bucket */
  public static final String DELETE_BATCH_SIZE = "s3.delete.batch-size";

  /**
   * Default batch size used when deleting files.
   *
   * <p>Refer to https://github.com/apache/hadoop/commit/56dee667707926f3796c7757be1a133a362f05c9
   * for more details on why this value was chosen.
   */
  public static final int DELETE_BATCH_SIZE_DEFAULT = 250;

  /**
   * Max possible batch size for deletion. Currently, a max of 1000 keys can be deleted in one
   * batch. https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
   */
  public static final int DELETE_BATCH_SIZE_MAX = 1000;

  /**
   * Used by {@link S3FileIO} to tag objects when writing. To set, we can pass a catalog property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.tags.my_key=my_val
   */
  public static final String WRITE_TAGS_PREFIX = "s3.write.tags.";

  /**
   * Used by {@link GlueCatalog} to tag objects when writing. To set, we can pass a catalog
   * property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.table-tag-enabled=true
   */
  public static final String WRITE_TABLE_TAG_ENABLED = "s3.write.table-tag-enabled";

  public static final boolean WRITE_TABLE_TAG_ENABLED_DEFAULT = false;

  /**
   * Used by {@link GlueCatalog} to tag objects when writing. To set, we can pass a catalog
   * property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.namespace-tag-enabled=true
   */
  public static final String WRITE_NAMESPACE_TAG_ENABLED = "s3.write.namespace-tag-enabled";

  public static final boolean WRITE_NAMESPACE_TAG_ENABLED_DEFAULT = false;

  /**
   * Tag name that will be used by {@link #WRITE_TAGS_PREFIX} when {@link #WRITE_TABLE_TAG_ENABLED}
   * is enabled
   *
   * <p>Example: iceberg.table=tableName
   */
  public static final String S3_TAG_ICEBERG_TABLE = "iceberg.table";

  /**
   * Tag name that will be used by {@link #WRITE_TAGS_PREFIX} when {@link
   * #WRITE_NAMESPACE_TAG_ENABLED} is enabled
   *
   * <p>Example: iceberg.namespace=namespaceName
   */
  public static final String S3_TAG_ICEBERG_NAMESPACE = "iceberg.namespace";

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
  public static final String DELETE_TAGS_PREFIX = "s3.delete.tags.";

  /**
   * Number of threads to use for adding delete tags to S3 objects, default to {@link
   * Runtime#availableProcessors()}
   */
  public static final String DELETE_THREADS = "s3.delete.num-threads";

  /**
   * Determines if {@link S3FileIO} deletes the object when io.delete() is called, default to true.
   * Once disabled, users are expected to set tags through {@link #DELETE_TAGS_PREFIX} and manage
   * deleted files through S3 lifecycle policy.
   */
  public static final String DELETE_ENABLED = "s3.delete-enabled";

  public static final boolean DELETE_ENABLED_DEFAULT = true;

  /**
   * Determines if S3 client will use the Acceleration Mode, default to false.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/transfer-acceleration.html
   */
  public static final String ACCELERATION_ENABLED = "s3.acceleration-enabled";

  public static final boolean ACCELERATION_ENABLED_DEFAULT = false;

  /**
   * Determines if S3 client will use the Dualstack Mode, default to false.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/dual-stack-endpoints.html
   */
  public static final String DUALSTACK_ENABLED = "s3.dualstack-enabled";

  public static final boolean DUALSTACK_ENABLED_DEFAULT = false;

  /**
   * Used by {@link S3FileIO}, prefix used for bucket access point configuration. To set, we can
   * pass a catalog property.
   *
   * <p>For more details, see https://aws.amazon.com/s3/features/access-points/
   *
   * <p>Example: s3.access-points.my-bucket=access-point
   */
  public static final String ACCESS_POINTS_PREFIX = "s3.access-points.";

  /**
   * This flag controls whether the S3 client will be initialized during the S3FileIO
   * initialization, instead of default lazy initialization upon use. This is needed for cases that
   * the credentials to use might change and needs to be preloaded.
   */
  public static final String PRELOAD_CLIENT_ENABLED = "s3.preload-client-enabled";

  public static final boolean PRELOAD_CLIENT_ENABLED_DEFAULT = false;

  private String sseType;
  private String sseKey;
  private String sseMd5;
  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  private int multipartUploadThreads;
  private int multiPartSize;
  private int deleteBatchSize;
  private double multipartThresholdFactor;
  private String stagingDirectory;
  private ObjectCannedACL acl;
  private boolean isChecksumEnabled;
  private final Set<Tag> writeTags;
  private boolean isWriteTableTagEnabled;
  private boolean isWriteNamespaceTagEnabled;
  private final Set<Tag> deleteTags;
  private int deleteThreads;
  private boolean isDeleteEnabled;
  private final Map<String, String> bucketToAccessPointMapping;
  private boolean isPreloadClientEnabled;
  private boolean isDualStackEnabled;
  private boolean isPathStyleAccess;
  private boolean isUseArnRegionEnabled;
  private boolean isAccelerationEnabled;
  private String endpoint;
  private final boolean isRemoteSigningEnabled;
  private final Map<String, String> allProperties;

  public S3FileIOProperties() {
    this.sseType = SSE_TYPE_NONE;
    this.sseKey = null;
    this.sseMd5 = null;
    this.accessKeyId = null;
    this.secretAccessKey = null;
    this.sessionToken = null;
    this.acl = null;
    this.endpoint = null;
    this.multipartUploadThreads = Runtime.getRuntime().availableProcessors();
    this.multiPartSize = MULTIPART_SIZE_DEFAULT;
    this.multipartThresholdFactor = MULTIPART_THRESHOLD_FACTOR_DEFAULT;
    this.deleteBatchSize = DELETE_BATCH_SIZE_DEFAULT;
    this.stagingDirectory = System.getProperty("java.io.tmpdir");
    this.isChecksumEnabled = CHECKSUM_ENABLED_DEFAULT;
    this.writeTags = Sets.newHashSet();
    this.isWriteTableTagEnabled = WRITE_TABLE_TAG_ENABLED_DEFAULT;
    this.isWriteNamespaceTagEnabled = WRITE_NAMESPACE_TAG_ENABLED_DEFAULT;
    this.deleteTags = Sets.newHashSet();
    this.deleteThreads = Runtime.getRuntime().availableProcessors();
    this.isDeleteEnabled = DELETE_ENABLED_DEFAULT;
    this.bucketToAccessPointMapping = Collections.emptyMap();
    this.isPreloadClientEnabled = PRELOAD_CLIENT_ENABLED_DEFAULT;
    this.isDualStackEnabled = DUALSTACK_ENABLED_DEFAULT;
    this.isPathStyleAccess = PATH_STYLE_ACCESS_DEFAULT;
    this.isUseArnRegionEnabled = USE_ARN_REGION_ENABLED_DEFAULT;
    this.isAccelerationEnabled = ACCELERATION_ENABLED_DEFAULT;
    this.isRemoteSigningEnabled = REMOTE_SIGNING_ENABLED_DEFAULT;
    this.allProperties = Maps.newHashMap();

    ValidationException.check(
        keyIdAccessKeyBothConfigured(),
        "S3 client access key ID and secret access key must be set at the same time");
  }

  public S3FileIOProperties(Map<String, String> properties) {
    this.sseType = properties.getOrDefault(SSE_TYPE, SSE_TYPE_NONE);
    this.sseKey = properties.get(SSE_KEY);
    this.sseMd5 = properties.get(SSE_MD5);
    this.accessKeyId = properties.get(ACCESS_KEY_ID);
    this.secretAccessKey = properties.get(SECRET_ACCESS_KEY);
    this.sessionToken = properties.get(SESSION_TOKEN);
    if (SSE_TYPE_CUSTOM.equals(sseType)) {
      Preconditions.checkArgument(
          null != sseKey, "Cannot initialize SSE-C S3FileIO with null encryption key");
      Preconditions.checkArgument(
          null != sseMd5, "Cannot initialize SSE-C S3FileIO with null encryption key MD5");
    }
    this.endpoint = properties.get(ENDPOINT);

    this.multipartUploadThreads =
        PropertyUtil.propertyAsInt(
            properties, MULTIPART_UPLOAD_THREADS, Runtime.getRuntime().availableProcessors());
    this.isPathStyleAccess =
        PropertyUtil.propertyAsBoolean(properties, PATH_STYLE_ACCESS, PATH_STYLE_ACCESS_DEFAULT);
    this.isUseArnRegionEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, USE_ARN_REGION_ENABLED, USE_ARN_REGION_ENABLED_DEFAULT);
    this.isAccelerationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, ACCELERATION_ENABLED, ACCELERATION_ENABLED_DEFAULT);
    this.isDualStackEnabled =
        PropertyUtil.propertyAsBoolean(properties, DUALSTACK_ENABLED, DUALSTACK_ENABLED_DEFAULT);
    try {
      this.multiPartSize =
          PropertyUtil.propertyAsInt(properties, MULTIPART_SIZE, MULTIPART_SIZE_DEFAULT);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Input malformed or exceeded maximum multipart upload size 5GB: %s",
              properties.get(MULTIPART_SIZE)));
    }
    this.multipartThresholdFactor =
        PropertyUtil.propertyAsDouble(
            properties, MULTIPART_THRESHOLD_FACTOR, MULTIPART_THRESHOLD_FACTOR_DEFAULT);
    Preconditions.checkArgument(
        multipartThresholdFactor >= 1.0, "Multipart threshold factor must be >= to 1.0");
    Preconditions.checkArgument(
        multiPartSize >= MULTIPART_SIZE_MIN,
        "Minimum multipart upload object size must be larger than 5 MB.");
    this.stagingDirectory =
        PropertyUtil.propertyAsString(
            properties, STAGING_DIRECTORY, System.getProperty("java.io.tmpdir"));
    String aclType = properties.get(ACL);
    this.acl = ObjectCannedACL.fromValue(aclType);
    Preconditions.checkArgument(
        acl == null || !acl.equals(ObjectCannedACL.UNKNOWN_TO_SDK_VERSION),
        "Cannot support S3 CannedACL " + aclType);
    this.isChecksumEnabled =
        PropertyUtil.propertyAsBoolean(properties, CHECKSUM_ENABLED, CHECKSUM_ENABLED_DEFAULT);
    this.deleteBatchSize =
        PropertyUtil.propertyAsInt(properties, DELETE_BATCH_SIZE, DELETE_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(
        deleteBatchSize > 0 && deleteBatchSize <= DELETE_BATCH_SIZE_MAX,
        String.format("Deletion batch size must be between 1 and %s", DELETE_BATCH_SIZE_MAX));
    this.writeTags = toS3Tags(properties, WRITE_TAGS_PREFIX);
    this.isWriteTableTagEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, WRITE_TABLE_TAG_ENABLED, WRITE_TABLE_TAG_ENABLED_DEFAULT);
    this.isWriteNamespaceTagEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, WRITE_NAMESPACE_TAG_ENABLED, WRITE_NAMESPACE_TAG_ENABLED_DEFAULT);
    this.deleteTags = toS3Tags(properties, DELETE_TAGS_PREFIX);
    this.deleteThreads =
        PropertyUtil.propertyAsInt(
            properties, DELETE_THREADS, Runtime.getRuntime().availableProcessors());
    this.isDeleteEnabled =
        PropertyUtil.propertyAsBoolean(properties, DELETE_ENABLED, DELETE_ENABLED_DEFAULT);
    this.bucketToAccessPointMapping =
        PropertyUtil.propertiesWithPrefix(properties, ACCESS_POINTS_PREFIX);
    this.isPreloadClientEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, PRELOAD_CLIENT_ENABLED, PRELOAD_CLIENT_ENABLED_DEFAULT);
    this.isRemoteSigningEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, REMOTE_SIGNING_ENABLED, REMOTE_SIGNING_ENABLED_DEFAULT);
    this.allProperties = SerializableMap.copyOf(properties);

    ValidationException.check(
        keyIdAccessKeyBothConfigured(),
        "S3 client access key ID and secret access key must be set at the same time");
  }

  public String sseType() {
    return sseType;
  }

  public void setSseType(String sseType) {
    this.sseType = sseType;
  }

  public String sseKey() {
    return sseKey;
  }

  public void setSseKey(String sseKey) {
    this.sseKey = sseKey;
  }

  public int deleteBatchSize() {
    return deleteBatchSize;
  }

  public void setDeleteBatchSize(int deleteBatchSize) {
    this.deleteBatchSize = deleteBatchSize;
  }

  public String sseMd5() {
    return sseMd5;
  }

  public void setSseMd5(String sseMd5) {
    this.sseMd5 = sseMd5;
  }

  public int multipartUploadThreads() {
    return multipartUploadThreads;
  }

  public void setMultipartUploadThreads(int threads) {
    this.multipartUploadThreads = threads;
  }

  public int multiPartSize() {
    return multiPartSize;
  }

  public void setMultiPartSize(int size) {
    this.multiPartSize = size;
  }

  public double multipartThresholdFactor() {
    return multipartThresholdFactor;
  }

  public void setMultipartThresholdFactor(double factor) {
    this.multipartThresholdFactor = factor;
  }

  public String stagingDirectory() {
    return stagingDirectory;
  }

  public void setStagingDirectory(String directory) {
    this.stagingDirectory = directory;
  }

  public ObjectCannedACL acl() {
    return this.acl;
  }

  public void setAcl(ObjectCannedACL acl) {
    this.acl = acl;
  }

  public boolean isPreloadClientEnabled() {
    return isPreloadClientEnabled;
  }

  public void setPreloadClientEnabled(boolean preloadClientEnabled) {
    this.isPreloadClientEnabled = preloadClientEnabled;
  }

  public boolean isDualStackEnabled() {
    return this.isDualStackEnabled;
  }

  public boolean isPathStyleAccess() {
    return this.isPathStyleAccess;
  }

  public boolean isUseArnRegionEnabled() {
    return this.isUseArnRegionEnabled;
  }

  public boolean isAccelerationEnabled() {
    return this.isAccelerationEnabled;
  }

  public boolean isChecksumEnabled() {
    return this.isChecksumEnabled;
  }

  public boolean isRemoteSigningEnabled() {
    return this.isRemoteSigningEnabled;
  }

  public String endpoint() {
    return this.endpoint;
  }

  public void setChecksumEnabled(boolean eTagCheckEnabled) {
    this.isChecksumEnabled = eTagCheckEnabled;
  }

  public Set<Tag> writeTags() {
    return writeTags;
  }

  public boolean writeTableTagEnabled() {
    return isWriteTableTagEnabled;
  }

  public void setWriteTableTagEnabled(boolean s3WriteTableNameTagEnabled) {
    this.isWriteTableTagEnabled = s3WriteTableNameTagEnabled;
  }

  public boolean isWriteNamespaceTagEnabled() {
    return isWriteNamespaceTagEnabled;
  }

  public void setWriteNamespaceTagEnabled(boolean writeNamespaceTagEnabled) {
    this.isWriteNamespaceTagEnabled = writeNamespaceTagEnabled;
  }

  public Set<Tag> deleteTags() {
    return deleteTags;
  }

  public int deleteThreads() {
    return deleteThreads;
  }

  public void setDeleteThreads(int threads) {
    this.deleteThreads = threads;
  }

  public boolean isDeleteEnabled() {
    return isDeleteEnabled;
  }

  public void setDeleteEnabled(boolean deleteEnabled) {
    this.isDeleteEnabled = deleteEnabled;
  }

  public Map<String, String> bucketToAccessPointMapping() {
    return bucketToAccessPointMapping;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String secretAccessKey() {
    return secretAccessKey;
  }

  public String sessionToken() {
    return sessionToken;
  }

  private Set<Tag> toS3Tags(Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
        .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toSet());
  }

  private boolean keyIdAccessKeyBothConfigured() {
    return (accessKeyId == null) == (secretAccessKey == null);
  }

  public <T extends S3ClientBuilder> void applyCredentialConfigurations(
      AwsClientProperties awsClientProperties, T builder) {
    builder.credentialsProvider(
        isRemoteSigningEnabled
            ? AnonymousCredentialsProvider.create()
            : awsClientProperties.credentialsProvider(accessKeyId, secretAccessKey, sessionToken));
  }

  /**
   * Configure services settings for an S3 client. The settings include: s3DualStack,
   * s3UseArnRegion, s3PathStyleAccess, and s3Acceleration
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(s3FileIOProperties::applyS3ServiceConfigurations)
   * </pre>
   */
  public <T extends S3ClientBuilder> void applyServiceConfigurations(T builder) {
    builder
        .dualstackEnabled(isDualStackEnabled)
        .serviceConfiguration(
            S3Configuration.builder()
                .pathStyleAccessEnabled(isPathStyleAccess)
                .useArnRegionEnabled(isUseArnRegionEnabled)
                .accelerateModeEnabled(isAccelerationEnabled)
                .build());
  }

  /**
   * Configure a signer for an S3 client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(s3FileIOProperties::applyS3SignerConfiguration)
   * </pre>
   */
  public <T extends S3ClientBuilder> void applySignerConfiguration(T builder) {
    if (isRemoteSigningEnabled) {
      builder.overrideConfiguration(
          c ->
              c.putAdvancedOption(
                  SdkAdvancedClientOption.SIGNER, S3V4RestSignerClient.create(allProperties)));
    }
  }

  /**
   * Override the endpoint for an S3 client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
   * </pre>
   */
  public <T extends S3ClientBuilder> void applyEndpointConfigurations(T builder) {
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
  }
}

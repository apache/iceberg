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
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.dynamodb.DynamoDbCatalog;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Tag;

public class AwsProperties implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(AwsProperties.class);

  /**
   * Type of S3 Server side encryption used, default to {@link
   * AwsProperties#S3FILEIO_SSE_TYPE_NONE}.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/serv-side-encryption.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_TYPE = "s3.sse.type";

  /**
   * No server side encryption.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_TYPE_NONE = "none";

  /**
   * S3 SSE-KMS encryption.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_TYPE_KMS = "kms";

  /**
   * S3 SSE-S3 encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_TYPE_S3 = "s3";

  /**
   * S3 SSE-C encryption.
   *
   * <p>For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_TYPE_CUSTOM = "custom";

  /**
   * If S3 encryption type is SSE-KMS, input is a KMS Key ID or ARN. In case this property is not
   * set, default key "aws/s3" is used. If encryption type is SSE-C, input is a custom base-64
   * AES256 symmetric key.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_KEY = "s3.sse.key";

  /**
   * If S3 encryption type is SSE-C, input is the base-64 MD5 digest of the secret key. This MD5
   * must be explicitly passed in by the caller to ensure key integrity.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SSE_MD5 = "s3.sse.md5";

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

  public static final boolean GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = true;

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
   * Configure an alternative endpoint of the Glue service for GlueCatalog to access.
   *
   * <p>This could be used to use GlueCatalog with any glue-compatible metastore service that has a
   * different endpoint
   */
  public static final String GLUE_CATALOG_ENDPOINT = "glue.endpoint";

  /**
   * Number of threads to use for uploading parts to S3 (shared pool across all output streams),
   * default to {@link Runtime#availableProcessors()}
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public static final String S3FILEIO_MULTIPART_UPLOAD_THREADS = "s3.multipart.num-threads";

  /**
   * The size of a single part for multipart upload requests in bytes (default: 32MB). based on S3
   * requirement, the part size must be at least 5MB. Too ensure performance of the reader and
   * writer, the part size must be less than 2GB.
   *
   * <p>For more details, see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_MULTIPART_SIZE = "s3.multipart.part-size-bytes";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final int S3FILEIO_MULTIPART_SIZE_DEFAULT = 32 * 1024 * 1024;
  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final int S3FILEIO_MULTIPART_SIZE_MIN = 5 * 1024 * 1024;

  /**
   * The threshold expressed as a factor times the multipart size at which to switch from uploading
   * using a single put object request to uploading using multipart upload (default: 1.5).
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public static final String S3FILEIO_MULTIPART_THRESHOLD_FACTOR = "s3.multipart.threshold";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final double S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT = 1.5;

  /**
   * Location to put staging files for upload to S3, default to temp directory set in
   * java.io.tmpdir.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_STAGING_DIRECTORY = "s3.staging-dir";

  /**
   * Used to configure canned access control list (ACL) for S3 client to use during write. If not
   * set, ACL will not be set for requests.
   *
   * <p>The input must be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL},
   * such as 'public-read-write' For more details:
   * https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_ACL = "s3.acl";

  /**
   * Configure an alternative endpoint of the S3 service for S3FileIO to access.
   *
   * <p>This could be used to use S3FileIO with any s3-compatible object storage service that has a
   * different endpoint, or access a private S3 endpoint in a virtual private cloud.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_ENDPOINT = "s3.endpoint";

  /**
   * If set {@code true}, requests to S3FileIO will use Path-Style, otherwise, Virtual Hosted-Style
   * will be used.
   *
   * <p>For more details: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_PATH_STYLE_ACCESS = "s3.path-style-access";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3FILEIO_PATH_STYLE_ACCESS_DEFAULT = false;

  /**
   * Configure the static access key ID used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #S3FILEIO_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is
   * used.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_ACCESS_KEY_ID = "s3.access-key-id";

  /**
   * Configure the static secret access key used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #S3FILEIO_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is
   * used.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SECRET_ACCESS_KEY = "s3.secret-access-key";

  /**
   * Configure the static session token used to access S3FileIO.
   *
   * <p>When set, the default client factory will use the session credentials provided instead of
   * reading the default credential chain to create S3 access credentials.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_SESSION_TOKEN = "s3.session-token";

  /**
   * Enable to make S3FileIO, to make cross-region call to the region specified in the ARN of an
   * access point.
   *
   * <p>By default, attempting to use an access point in a different region will throw an exception.
   * When enabled, this property allows using access points in other regions.
   *
   * <p>For more details see:
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Configuration.html#useArnRegionEnabled--
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_USE_ARN_REGION_ENABLED_DEFAULT = false;

  /**
   * Enables eTag checks for S3 PUT and MULTIPART upload requests.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_CHECKSUM_ENABLED = "s3.checksum-enabled";
  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_CHECKSUM_ENABLED_DEFAULT = false;
  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";
  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_REMOTE_SIGNING_ENABLED_DEFAULT = false;

  /**
   * Configure the batch size used when deleting multiple files from a given S3 bucket
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_DELETE_BATCH_SIZE = "s3.delete.batch-size";

  /**
   * Default batch size used when deleting files.
   *
   * <p>Refer to https://github.com/apache/hadoop/commit/56dee667707926f3796c7757be1a133a362f05c9
   * for more details on why this value was chosen.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final int S3FILEIO_DELETE_BATCH_SIZE_DEFAULT = 250;

  /**
   * Max possible batch size for deletion. Currently, a max of 1000 keys can be deleted in one
   * batch. https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final int S3FILEIO_DELETE_BATCH_SIZE_MAX = 1000;

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
   * Used by {@link AssumeRoleAwsClientFactory}. Optional session name used to assume an IAM role.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#ck_rolesessionname
   */
  public static final String CLIENT_ASSUME_ROLE_SESSION_NAME = "client.assume-role.session-name";

  /**
   * Configure the AWS credentials provider used to create AWS clients. A fully qualified concrete
   * class with package that implements the {@link AwsCredentialsProvider} interface is required.
   *
   * <p>Additionally, the implementation class must also have a create() or create(Map) method
   * implemented, which returns an instance of the class that provides aws credentials provider.
   *
   * <p>Example:
   * client.credentials-provider=software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider
   *
   * <p>When set, the default client factory {@link
   * org.apache.iceberg.aws.AwsClientFactories.DefaultAwsClientFactory} and also other client
   * factory classes will use this provider to get AWS credentials provided instead of reading the
   * default credential chain to get AWS access credentials.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.AwsClientProperties}
   *     instead
   */
  @Deprecated
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";

  /**
   * Used by the client.credentials-provider configured value that will be used by {@link
   * org.apache.iceberg.aws.AwsClientFactories.DefaultAwsClientFactory} and also other client
   * factory classes to pass provider-specific properties. Each property consists of a key name and
   * an associated value.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.AwsClientProperties}
   *     instead
   */
  @Deprecated
  private static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX = "client.credentials-provider.";

  /**
   * Used by {@link org.apache.iceberg.aws.AwsClientFactories.DefaultAwsClientFactory} and also
   * other client factory classes. If set, all AWS clients except STS client will use the given
   * region instead of the default region chain.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.AwsClientProperties}
   *     instead
   */
  @Deprecated public static final String CLIENT_REGION = "client.region";

  /**
   * The type of {@link software.amazon.awssdk.http.SdkHttpClient} implementation used by {@link
   * AwsClientFactory} If set, all AWS clients will use this specified HTTP client. If not set,
   * {@link #HTTP_CLIENT_TYPE_DEFAULT} will be used. For specific types supported, see
   * HTTP_CLIENT_TYPE_* defined below.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated public static final String HTTP_CLIENT_TYPE = "http-client.type";

  /**
   * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} will be used as the HTTP
   * Client in {@link AwsClientFactory}
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated public static final String HTTP_CLIENT_TYPE_URLCONNECTION = "urlconnection";

  /**
   * If this is set under {@link #HTTP_CLIENT_TYPE}, {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient} will be used as the HTTP Client in {@link
   * AwsClientFactory}
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated public static final String HTTP_CLIENT_TYPE_APACHE = "apache";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated public static final String HTTP_CLIENT_TYPE_DEFAULT = HTTP_CLIENT_TYPE_APACHE;

  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.Builder}. This flag only
   * works when {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_URLCONNECTION}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_URLCONNECTION_CONNECTION_TIMEOUT_MS =
      "http-client.urlconnection.connection-timeout-ms";

  /**
   * Used to configure the socket timeout in milliseconds for {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.Builder}. This flag only
   * works when {@link #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_URLCONNECTION}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/urlconnection/UrlConnectionHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_URLCONNECTION_SOCKET_TIMEOUT_MS =
      "http-client.urlconnection.socket-timeout-ms";

  /**
   * Used to configure the connection timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS =
      "http-client.apache.connection-timeout-ms";

  /**
   * Used to configure the socket timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS =
      "http-client.apache.socket-timeout-ms";

  /**
   * Used to configure the connection acquisition timeout in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_CONNECTION_ACQUISITION_TIMEOUT_MS =
      "http-client.apache.connection-acquisition-timeout-ms";

  /**
   * Used to configure the connection max idle time in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_CONNECTION_MAX_IDLE_TIME_MS =
      "http-client.apache.connection-max-idle-time-ms";

  /**
   * Used to configure the connection time to live in milliseconds for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_CONNECTION_TIME_TO_LIVE_MS =
      "http-client.apache.connection-time-to-live-ms";

  /**
   * Used to configure whether to enable the expect continue setting for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>In default, this is disabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_EXPECT_CONTINUE_ENABLED =
      "http-client.apache.expect-continue-enabled";

  /**
   * Used to configure the max connections number for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_MAX_CONNECTIONS =
      "http-client.apache.max-connections";

  /**
   * Used to configure whether to enable the tcp keep alive setting for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}.
   *
   * <p>In default, this is disabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_TCP_KEEP_ALIVE_ENABLED =
      "http-client.apache.tcp-keep-alive-enabled";

  /**
   * Used to configure whether to use idle connection reaper for {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient.Builder}. This flag only works when {@link
   * #HTTP_CLIENT_TYPE} is set to {@link #HTTP_CLIENT_TYPE_APACHE}.
   *
   * <p>In default, this is enabled.
   *
   * <p>For more details, see
   * https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/http/apache/ApacheHttpClient.Builder.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public static final String HTTP_CLIENT_APACHE_USE_IDLE_CONNECTION_REAPER_ENABLED =
      "http-client.apache.use-idle-connection-reaper-enabled";
  /**
   * Used by {@link S3FileIO} to tag objects when writing. To set, we can pass a catalog property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.tags.my_key=my_val
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_WRITE_TAGS_PREFIX = "s3.write.tags.";

  /**
   * Used by {@link GlueCatalog} to tag objects when writing. To set, we can pass a catalog
   * property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.table-tag-enabled=true
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_WRITE_TABLE_TAG_ENABLED = "s3.write.table-tag-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_WRITE_TABLE_TAG_ENABLED_DEFAULT = false;

  /**
   * Used by {@link GlueCatalog} to tag objects when writing. To set, we can pass a catalog
   * property.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
   *
   * <p>Example: s3.write.namespace-tag-enabled=true
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public static final String S3_WRITE_NAMESPACE_TAG_ENABLED = "s3.write.namespace-tag-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_WRITE_NAMESPACE_TAG_ENABLED_DEFAULT = false;

  /**
   * Tag name that will be used by {@link #S3_WRITE_TAGS_PREFIX} when {@link
   * #S3_WRITE_TABLE_TAG_ENABLED} is enabled
   *
   * <p>Example: iceberg.table=tableName
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_TAG_ICEBERG_TABLE = "iceberg.table";

  /**
   * Tag name that will be used by {@link #S3_WRITE_TAGS_PREFIX} when {@link
   * #S3_WRITE_NAMESPACE_TAG_ENABLED} is enabled
   *
   * <p>Example: iceberg.namespace=namespaceName
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_TAG_ICEBERG_NAMESPACE = "iceberg.namespace";

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
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_DELETE_TAGS_PREFIX = "s3.delete.tags.";

  /**
   * Number of threads to use for adding delete tags to S3 objects, default to {@link
   * Runtime#availableProcessors()}
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3FILEIO_DELETE_THREADS = "s3.delete.num-threads";

  /**
   * Determines if {@link S3FileIO} deletes the object when io.delete() is called, default to true.
   * Once disabled, users are expected to set tags through {@link #S3_DELETE_TAGS_PREFIX} and manage
   * deleted files through S3 lifecycle policy.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_DELETE_ENABLED = "s3.delete-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_DELETE_ENABLED_DEFAULT = true;

  /**
   * Determines if S3 client will use the Acceleration Mode, default to false.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/transfer-acceleration.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_ACCELERATION_ENABLED = "s3.acceleration-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_ACCELERATION_ENABLED_DEFAULT = false;

  /**
   * Determines if S3 client will use the Dualstack Mode, default to false.
   *
   * <p>For more details, see
   * https://docs.aws.amazon.com/AmazonS3/latest/userguide/dual-stack-endpoints.html
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_DUALSTACK_ENABLED = "s3.dualstack-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_DUALSTACK_ENABLED_DEFAULT = false;

  /**
   * Used by {@link S3FileIO}, prefix used for bucket access point configuration. To set, we can
   * pass a catalog property.
   *
   * <p>For more details, see https://aws.amazon.com/s3/features/access-points/
   *
   * <p>Example: s3.access-points.my-bucket=access-point
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_ACCESS_POINTS_PREFIX = "s3.access-points.";

  /**
   * This flag controls whether the S3 client will be initialized during the S3FileIO
   * initialization, instead of default lazy initialization upon use. This is needed for cases that
   * the credentials to use might change and needs to be preloaded.
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final String S3_PRELOAD_CLIENT_ENABLED = "s3.preload-client-enabled";

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated public static final boolean S3_PRELOAD_CLIENT_ENABLED_DEFAULT = false;

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

  /** Region to be used by the SigV4 protocol for signing requests. */
  public static final String REST_SIGNER_REGION = "rest.signing-region";

  /** The service name to be used by the SigV4 protocol for signing requests. */
  public static final String REST_SIGNING_NAME = "rest.signing-name";

  /** The default service name (API Gateway and lambda) used during SigV4 signing. */
  public static final String REST_SIGNING_NAME_DEFAULT = "execute-api";

  /**
   * Configure the static access key ID used for SigV4 signing.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #REST_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String REST_ACCESS_KEY_ID = "rest.access-key-id";

  /**
   * Configure the static secret access key used for SigV4 signing.
   *
   * <p>When set, the default client factory will use the basic or session credentials provided
   * instead of reading the default credential chain to create S3 access credentials. If {@link
   * #REST_SESSION_TOKEN} is set, session credential is used, otherwise basic credential is used.
   */
  public static final String REST_SECRET_ACCESS_KEY = "rest.secret-access-key";

  /**
   * Configure the static session token used for SigV4.
   *
   * <p>When set, the default client factory will use the session credentials provided instead of
   * reading the default credential chain to create access credentials.
   */
  public static final String REST_SESSION_TOKEN = "rest.session-token";

  private static final String HTTP_CLIENT_PREFIX = "http-client.";
  private String httpClientType;
  private final Map<String, String> httpClientProperties;
  private final Set<software.amazon.awssdk.services.sts.model.Tag> stsClientAssumeRoleTags;

  private String clientAssumeRoleArn;
  private String clientAssumeRoleExternalId;
  private int clientAssumeRoleTimeoutSec;
  private String clientAssumeRoleRegion;
  private String clientAssumeRoleSessionName;
  private String clientRegion;
  private String clientCredentialsProvider;
  private final Map<String, String> clientCredentialsProviderProperties;

  private String s3FileIoSseType;
  private String s3FileIoSseKey;
  private String s3FileIoSseMd5;
  private String s3AccessKeyId;
  private String s3SecretAccessKey;
  private String s3SessionToken;
  private int s3FileIoMultipartUploadThreads;
  private int s3FileIoMultiPartSize;
  private int s3FileIoDeleteBatchSize;
  private double s3FileIoMultipartThresholdFactor;
  private String s3fileIoStagingDirectory;
  private ObjectCannedACL s3FileIoAcl;
  private boolean isS3ChecksumEnabled;
  private final Set<Tag> s3WriteTags;
  private boolean s3WriteTableTagEnabled;
  private boolean s3WriteNamespaceTagEnabled;
  private final Set<Tag> s3DeleteTags;
  private int s3FileIoDeleteThreads;
  private boolean isS3DeleteEnabled;
  private final Map<String, String> s3BucketToAccessPointMapping;
  private boolean s3PreloadClientEnabled;
  private boolean s3DualStackEnabled;
  private boolean s3PathStyleAccess;
  private boolean s3UseArnRegionEnabled;
  private boolean s3AccelerationEnabled;
  private String s3Endpoint;

  private String glueEndpoint;
  private String glueCatalogId;
  private boolean glueCatalogSkipArchive;
  private boolean glueCatalogSkipNameValidation;
  private boolean glueLakeFormationEnabled;

  private String dynamoDbTableName;
  private String dynamoDbEndpoint;

  private final boolean s3RemoteSigningEnabled;
  private final Map<String, String> allProperties;

  private String restSigningRegion;
  private String restSigningName;
  private String restAccessKeyId;
  private String restSecretAccessKey;
  private String restSessionToken;

  public AwsProperties() {
    this.httpClientType = HTTP_CLIENT_TYPE_DEFAULT;
    this.httpClientProperties = Collections.emptyMap();
    this.stsClientAssumeRoleTags = Sets.newHashSet();

    this.clientAssumeRoleArn = null;
    this.clientAssumeRoleTimeoutSec = CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT;
    this.clientAssumeRoleExternalId = null;
    this.clientAssumeRoleRegion = null;
    this.clientAssumeRoleSessionName = null;
    this.clientRegion = null;
    this.clientCredentialsProvider = null;
    this.clientCredentialsProviderProperties = null;

    this.s3FileIoSseType = S3FILEIO_SSE_TYPE_NONE;
    this.s3FileIoSseKey = null;
    this.s3FileIoSseMd5 = null;
    this.s3AccessKeyId = null;
    this.s3SecretAccessKey = null;
    this.s3SessionToken = null;
    this.s3FileIoAcl = null;
    this.s3Endpoint = null;
    this.s3FileIoMultipartUploadThreads = Runtime.getRuntime().availableProcessors();
    this.s3FileIoMultiPartSize = S3FILEIO_MULTIPART_SIZE_DEFAULT;
    this.s3FileIoMultipartThresholdFactor = S3FILEIO_MULTIPART_THRESHOLD_FACTOR_DEFAULT;
    this.s3FileIoDeleteBatchSize = S3FILEIO_DELETE_BATCH_SIZE_DEFAULT;
    this.s3fileIoStagingDirectory = System.getProperty("java.io.tmpdir");
    this.isS3ChecksumEnabled = S3_CHECKSUM_ENABLED_DEFAULT;
    this.s3WriteTags = Sets.newHashSet();
    this.s3WriteTableTagEnabled = S3_WRITE_TABLE_TAG_ENABLED_DEFAULT;
    this.s3WriteNamespaceTagEnabled = S3_WRITE_NAMESPACE_TAG_ENABLED_DEFAULT;
    this.s3DeleteTags = Sets.newHashSet();
    this.s3FileIoDeleteThreads = Runtime.getRuntime().availableProcessors();
    this.isS3DeleteEnabled = S3_DELETE_ENABLED_DEFAULT;
    this.s3BucketToAccessPointMapping = Collections.emptyMap();
    this.s3PreloadClientEnabled = S3_PRELOAD_CLIENT_ENABLED_DEFAULT;
    this.s3DualStackEnabled = S3_DUALSTACK_ENABLED_DEFAULT;
    this.s3PathStyleAccess = S3FILEIO_PATH_STYLE_ACCESS_DEFAULT;
    this.s3UseArnRegionEnabled = S3_USE_ARN_REGION_ENABLED_DEFAULT;
    this.s3AccelerationEnabled = S3_ACCELERATION_ENABLED_DEFAULT;

    this.glueCatalogId = null;
    this.glueEndpoint = null;
    this.glueCatalogSkipArchive = GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT;
    this.glueCatalogSkipNameValidation = GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT;
    this.glueLakeFormationEnabled = GLUE_LAKEFORMATION_ENABLED_DEFAULT;

    this.dynamoDbEndpoint = null;
    this.dynamoDbTableName = DYNAMODB_TABLE_NAME_DEFAULT;

    this.s3RemoteSigningEnabled = S3_REMOTE_SIGNING_ENABLED_DEFAULT;
    this.allProperties = Maps.newHashMap();

    this.restSigningName = REST_SIGNING_NAME_DEFAULT;

    ValidationException.check(
        s3KeyIdAccessKeyBothConfigured(),
        "S3 client access key ID and secret access key must be set at the same time");
  }

  @SuppressWarnings("MethodLength")
  public AwsProperties(Map<String, String> properties) {
    this.httpClientType =
        PropertyUtil.propertyAsString(properties, HTTP_CLIENT_TYPE, HTTP_CLIENT_TYPE_DEFAULT);
    this.httpClientProperties =
        PropertyUtil.filterProperties(properties, key -> key.startsWith(HTTP_CLIENT_PREFIX));
    this.stsClientAssumeRoleTags = toStsTags(properties, CLIENT_ASSUME_ROLE_TAGS_PREFIX);
    this.clientAssumeRoleArn = properties.get(CLIENT_ASSUME_ROLE_ARN);
    this.clientAssumeRoleTimeoutSec =
        PropertyUtil.propertyAsInt(
            properties, CLIENT_ASSUME_ROLE_TIMEOUT_SEC, CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT);
    this.clientAssumeRoleExternalId = properties.get(CLIENT_ASSUME_ROLE_EXTERNAL_ID);
    this.clientAssumeRoleRegion = properties.get(CLIENT_ASSUME_ROLE_REGION);
    this.clientAssumeRoleSessionName = properties.get(CLIENT_ASSUME_ROLE_SESSION_NAME);
    this.clientRegion = properties.get(CLIENT_REGION);
    this.clientCredentialsProvider = properties.get(CLIENT_CREDENTIALS_PROVIDER);
    this.clientCredentialsProviderProperties =
        PropertyUtil.propertiesWithPrefix(properties, CLIENT_CREDENTIAL_PROVIDER_PREFIX);

    this.s3FileIoSseType = properties.getOrDefault(S3FILEIO_SSE_TYPE, S3FILEIO_SSE_TYPE_NONE);
    this.s3FileIoSseKey = properties.get(S3FILEIO_SSE_KEY);
    this.s3FileIoSseMd5 = properties.get(S3FILEIO_SSE_MD5);
    this.s3AccessKeyId = properties.get(S3FILEIO_ACCESS_KEY_ID);
    this.s3SecretAccessKey = properties.get(S3FILEIO_SECRET_ACCESS_KEY);
    this.s3SessionToken = properties.get(S3FILEIO_SESSION_TOKEN);
    if (S3FILEIO_SSE_TYPE_CUSTOM.equals(s3FileIoSseType)) {
      Preconditions.checkNotNull(
          s3FileIoSseKey, "Cannot initialize SSE-C S3FileIO with null encryption key");
      Preconditions.checkNotNull(
          s3FileIoSseMd5, "Cannot initialize SSE-C S3FileIO with null encryption key MD5");
    }
    this.s3Endpoint = properties.get(S3FILEIO_ENDPOINT);

    this.glueEndpoint = properties.get(GLUE_CATALOG_ENDPOINT);
    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueCatalogSkipArchive =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_CATALOG_SKIP_ARCHIVE, GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);
    this.glueCatalogSkipNameValidation =
        PropertyUtil.propertyAsBoolean(
            properties,
            GLUE_CATALOG_SKIP_NAME_VALIDATION,
            GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT);
    this.glueLakeFormationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, GLUE_LAKEFORMATION_ENABLED, GLUE_LAKEFORMATION_ENABLED_DEFAULT);
    this.s3FileIoMultipartUploadThreads =
        PropertyUtil.propertyAsInt(
            properties,
            S3FILEIO_MULTIPART_UPLOAD_THREADS,
            Runtime.getRuntime().availableProcessors());
    this.s3PathStyleAccess =
        PropertyUtil.propertyAsBoolean(
            properties, S3FILEIO_PATH_STYLE_ACCESS, S3FILEIO_PATH_STYLE_ACCESS_DEFAULT);
    this.s3UseArnRegionEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_USE_ARN_REGION_ENABLED, S3_USE_ARN_REGION_ENABLED_DEFAULT);
    this.s3AccelerationEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_ACCELERATION_ENABLED, S3_ACCELERATION_ENABLED_DEFAULT);
    this.s3DualStackEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_DUALSTACK_ENABLED, S3_DUALSTACK_ENABLED_DEFAULT);
    try {
      this.s3FileIoMultiPartSize =
          PropertyUtil.propertyAsInt(
              properties, S3FILEIO_MULTIPART_SIZE, S3FILEIO_MULTIPART_SIZE_DEFAULT);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Input malformed or exceeded maximum multipart upload size 5GB: %s",
              properties.get(S3FILEIO_MULTIPART_SIZE)));
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
    this.s3WriteTags = toS3Tags(properties, S3_WRITE_TAGS_PREFIX);
    this.s3WriteTableTagEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_WRITE_TABLE_TAG_ENABLED, S3_WRITE_TABLE_TAG_ENABLED_DEFAULT);
    this.s3WriteNamespaceTagEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_WRITE_NAMESPACE_TAG_ENABLED, S3_WRITE_NAMESPACE_TAG_ENABLED_DEFAULT);
    this.s3DeleteTags = toS3Tags(properties, S3_DELETE_TAGS_PREFIX);
    this.s3FileIoDeleteThreads =
        PropertyUtil.propertyAsInt(
            properties, S3FILEIO_DELETE_THREADS, Runtime.getRuntime().availableProcessors());
    this.isS3DeleteEnabled =
        PropertyUtil.propertyAsBoolean(properties, S3_DELETE_ENABLED, S3_DELETE_ENABLED_DEFAULT);
    this.s3BucketToAccessPointMapping =
        PropertyUtil.propertiesWithPrefix(properties, S3_ACCESS_POINTS_PREFIX);
    this.s3PreloadClientEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_PRELOAD_CLIENT_ENABLED, S3_PRELOAD_CLIENT_ENABLED_DEFAULT);

    this.dynamoDbEndpoint = properties.get(DYNAMODB_ENDPOINT);
    this.dynamoDbTableName =
        PropertyUtil.propertyAsString(properties, DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME_DEFAULT);

    this.s3RemoteSigningEnabled =
        PropertyUtil.propertyAsBoolean(
            properties, S3_REMOTE_SIGNING_ENABLED, S3_REMOTE_SIGNING_ENABLED_DEFAULT);
    this.allProperties = SerializableMap.copyOf(properties);

    this.restSigningRegion = properties.get(REST_SIGNER_REGION);
    this.restSigningName = properties.getOrDefault(REST_SIGNING_NAME, REST_SIGNING_NAME_DEFAULT);
    this.restAccessKeyId = properties.get(REST_ACCESS_KEY_ID);
    this.restSecretAccessKey = properties.get(REST_SECRET_ACCESS_KEY);
    this.restSessionToken = properties.get(REST_SESSION_TOKEN);

    ValidationException.check(
        s3KeyIdAccessKeyBothConfigured(),
        "S3 client access key ID and secret access key must be set at the same time");
  }

  public Set<software.amazon.awssdk.services.sts.model.Tag> stsClientAssumeRoleTags() {
    return stsClientAssumeRoleTags;
  }

  public String clientAssumeRoleArn() {
    return clientAssumeRoleArn;
  }

  public int clientAssumeRoleTimeoutSec() {
    return clientAssumeRoleTimeoutSec;
  }

  public String clientAssumeRoleExternalId() {
    return clientAssumeRoleExternalId;
  }

  public String clientAssumeRoleRegion() {
    return clientAssumeRoleRegion;
  }

  public String clientAssumeRoleSessionName() {
    return clientAssumeRoleSessionName;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public String s3FileIoSseType() {
    return s3FileIoSseType;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoSseType(String sseType) {
    this.s3FileIoSseType = sseType;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public String s3FileIoSseKey() {
    return s3FileIoSseKey;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public int s3FileIoDeleteBatchSize() {
    return s3FileIoDeleteBatchSize;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoDeleteBatchSize(int deleteBatchSize) {
    this.s3FileIoDeleteBatchSize = deleteBatchSize;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoSseKey(String sseKey) {
    this.s3FileIoSseKey = sseKey;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public String s3FileIoSseMd5() {
    return s3FileIoSseMd5;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
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

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public int s3FileIoMultipartUploadThreads() {
    return s3FileIoMultipartUploadThreads;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoMultipartUploadThreads(int threads) {
    this.s3FileIoMultipartUploadThreads = threads;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public int s3FileIoMultiPartSize() {
    return s3FileIoMultiPartSize;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoMultiPartSize(int size) {
    this.s3FileIoMultiPartSize = size;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public double s3FileIOMultipartThresholdFactor() {
    return s3FileIoMultipartThresholdFactor;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoMultipartThresholdFactor(double factor) {
    this.s3FileIoMultipartThresholdFactor = factor;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public String s3fileIoStagingDirectory() {
    return s3fileIoStagingDirectory;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3fileIoStagingDirectory(String directory) {
    this.s3fileIoStagingDirectory = directory;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public ObjectCannedACL s3FileIoAcl() {
    return this.s3FileIoAcl;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoAcl(ObjectCannedACL acl) {
    this.s3FileIoAcl = acl;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3PreloadClientEnabled(boolean s3PreloadClientEnabled) {
    this.s3PreloadClientEnabled = s3PreloadClientEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public boolean s3PreloadClientEnabled() {
    return s3PreloadClientEnabled;
  }

  public String dynamoDbTableName() {
    return dynamoDbTableName;
  }

  public void setDynamoDbTableName(String name) {
    this.dynamoDbTableName = name;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public boolean isS3ChecksumEnabled() {
    return this.isS3ChecksumEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3ChecksumEnabled(boolean eTagCheckEnabled) {
    this.isS3ChecksumEnabled = eTagCheckEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public Set<Tag> s3WriteTags() {
    return s3WriteTags;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public boolean s3WriteTableTagEnabled() {
    return s3WriteTableTagEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3WriteTableTagEnabled(boolean s3WriteTableNameTagEnabled) {
    this.s3WriteTableTagEnabled = s3WriteTableNameTagEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public boolean s3WriteNamespaceTagEnabled() {
    return s3WriteNamespaceTagEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3WriteNamespaceTagEnabled(boolean s3WriteNamespaceTagEnabled) {
    this.s3WriteNamespaceTagEnabled = s3WriteNamespaceTagEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public Set<Tag> s3DeleteTags() {
    return s3DeleteTags;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public int s3FileIoDeleteThreads() {
    return s3FileIoDeleteThreads;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3FileIoDeleteThreads(int threads) {
    this.s3FileIoDeleteThreads = threads;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public boolean isS3DeleteEnabled() {
    return isS3DeleteEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public void setS3DeleteEnabled(boolean s3DeleteEnabled) {
    this.isS3DeleteEnabled = s3DeleteEnabled;
  }

  /**
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public Map<String, String> s3BucketToAccessPointMapping() {
    return s3BucketToAccessPointMapping;
  }

  public Map<String, String> httpClientProperties() {
    return httpClientProperties;
  }

  public String clientRegion() {
    return clientRegion;
  }

  public void setClientRegion(String clientRegion) {
    this.clientRegion = clientRegion;
  }

  /**
   * Configure the credentials for an S3 client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyS3CredentialConfigurations)
   * </pre>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public <T extends S3ClientBuilder> void applyS3CredentialConfigurations(T builder) {
    builder.credentialsProvider(
        s3RemoteSigningEnabled
            ? AnonymousCredentialsProvider.create()
            : credentialsProvider(s3AccessKeyId, s3SecretAccessKey, s3SessionToken));
  }

  /**
   * Configure a client AWS region.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyClientRegionConfiguration)
   * </pre>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.AwsClientProperties}
   *     instead
   */
  @Deprecated
  public <T extends AwsClientBuilder> void applyClientRegionConfiguration(T builder) {
    if (clientRegion != null) {
      builder.region(Region.of(clientRegion));
    }
  }

  /**
   * Configure the credential provider for AWS clients.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     DynamoDbClient.builder().applyMutation(awsProperties::applyClientCredentialConfigurations)
   * </pre>
   */
  public <T extends AwsClientBuilder> void applyClientCredentialConfigurations(T builder) {
    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      builder.credentialsProvider(credentialsProvider(this.clientCredentialsProvider));
    }
  }

  /**
   * Configure services settings for an S3 client. The settings include: s3DualStack,
   * s3UseArnRegion, s3PathStyleAccess, and s3Acceleration
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyS3ServiceConfigurations)
   * </pre>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public <T extends S3ClientBuilder> void applyS3ServiceConfigurations(T builder) {
    builder
        .dualstackEnabled(s3DualStackEnabled)
        .serviceConfiguration(
            S3Configuration.builder()
                .pathStyleAccessEnabled(s3PathStyleAccess)
                .useArnRegionEnabled(s3UseArnRegionEnabled)
                .accelerateModeEnabled(s3AccelerationEnabled)
                .build());
  }

  /**
   * Configure a signer for an S3 client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyS3SignerConfiguration)
   * </pre>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public <T extends S3ClientBuilder> void applyS3SignerConfiguration(T builder) {
    if (s3RemoteSigningEnabled) {
      builder.overrideConfiguration(
          c ->
              c.putAdvancedOption(
                  SdkAdvancedClientOption.SIGNER, S3V4RestSignerClient.create(allProperties)));
    }
  }

  /**
   * Configure the httpClient for a client according to the HttpClientType. The two supported
   * HttpClientTypes are urlconnection and apache
   *
   * <p>Sample usage:
   *
   * <pre>
   *     S3Client.builder().applyMutation(awsProperties::applyHttpClientConfigurations)
   * </pre>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  public <T extends AwsSyncClientBuilder> void applyHttpClientConfigurations(T builder) {
    if (Strings.isNullOrEmpty(httpClientType)) {
      httpClientType = HTTP_CLIENT_TYPE_DEFAULT;
    }
    switch (httpClientType) {
      case HTTP_CLIENT_TYPE_URLCONNECTION:
        UrlConnectionHttpClientConfigurations urlConnectionHttpClientConfigurations =
            loadHttpClientConfigurations(UrlConnectionHttpClientConfigurations.class.getName());
        urlConnectionHttpClientConfigurations.configureHttpClientBuilder(builder);
        break;
      case HTTP_CLIENT_TYPE_APACHE:
        ApacheHttpClientConfigurations apacheHttpClientConfigurations =
            loadHttpClientConfigurations(ApacheHttpClientConfigurations.class.getName());
        apacheHttpClientConfigurations.configureHttpClientBuilder(builder);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized HTTP client type " + httpClientType);
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
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.s3.S3FileIOProperties}
   *     instead
   */
  @Deprecated
  public <T extends S3ClientBuilder> void applyS3EndpointConfigurations(T builder) {
    configureEndpoint(builder, s3Endpoint);
  }

  /**
   * Override the endpoint for a glue client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     GlueClient.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
   * </pre>
   */
  public <T extends GlueClientBuilder> void applyGlueEndpointConfigurations(T builder) {
    configureEndpoint(builder, glueEndpoint);
  }

  /**
   * Override the endpoint for a dynamoDb client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     DynamoDbClient.builder().applyMutation(awsProperties::applyS3EndpointConfigurations)
   * </pre>
   */
  public <T extends DynamoDbClientBuilder> void applyDynamoDbEndpointConfigurations(T builder) {
    configureEndpoint(builder, dynamoDbEndpoint);
  }

  public Region restSigningRegion() {
    if (restSigningRegion == null) {
      this.restSigningRegion = DefaultAwsRegionProviderChain.builder().build().getRegion().id();
    }

    return Region.of(restSigningRegion);
  }

  public String restSigningName() {
    return restSigningName;
  }

  public AwsCredentialsProvider restCredentialsProvider() {
    return credentialsProvider(
        this.restAccessKeyId, this.restSecretAccessKey, this.restSessionToken);
  }

  private Set<Tag> toS3Tags(Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
        .map(e -> Tag.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toSet());
  }

  private Set<software.amazon.awssdk.services.sts.model.Tag> toStsTags(
      Map<String, String> properties, String prefix) {
    return PropertyUtil.propertiesWithPrefix(properties, prefix).entrySet().stream()
        .map(
            e ->
                software.amazon.awssdk.services.sts.model.Tag.builder()
                    .key(e.getKey())
                    .value(e.getValue())
                    .build())
        .collect(Collectors.toSet());
  }

  private boolean s3KeyIdAccessKeyBothConfigured() {
    return (s3AccessKeyId == null) == (s3SecretAccessKey == null);
  }

  private AwsCredentialsProvider credentialsProvider(
      String accessKeyId, String secretAccessKey, String sessionToken) {
    if (accessKeyId != null) {
      if (sessionToken == null) {
        return StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKeyId, secretAccessKey));
      } else {
        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
      }
    }

    if (!Strings.isNullOrEmpty(this.clientCredentialsProvider)) {
      return credentialsProvider(this.clientCredentialsProvider);
    }

    return DefaultCredentialsProvider.create();
  }

  private AwsCredentialsProvider credentialsProvider(String credentialsProviderClass) {
    Class<?> providerClass;
    try {
      providerClass = DynClasses.builder().impl(credentialsProviderClass).buildChecked();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot load class %s, it does not exist in the classpath", credentialsProviderClass),
          e);
    }

    Preconditions.checkArgument(
        AwsCredentialsProvider.class.isAssignableFrom(providerClass),
        String.format(
            "Cannot initialize %s, it does not implement %s.",
            credentialsProviderClass, AwsCredentialsProvider.class.getName()));

    AwsCredentialsProvider provider;
    try {
      try {
        provider =
            DynMethods.builder("create")
                .hiddenImpl(providerClass, Map.class)
                .buildStaticChecked()
                .invoke(clientCredentialsProviderProperties);
      } catch (NoSuchMethodException e) {
        provider =
            DynMethods.builder("create").hiddenImpl(providerClass).buildStaticChecked().invoke();
      }

      return provider;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot create an instance of %s, it does not contain a static 'create' or 'create(Map<String, String>)' method",
              credentialsProviderClass),
          e);
    }
  }

  private <T extends SdkClientBuilder> void configureEndpoint(T builder, String endpoint) {
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
  }

  /**
   * Dynamically load the http client builder to avoid runtime deps requirements of both {@link
   * software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient} and {@link
   * software.amazon.awssdk.http.apache.ApacheHttpClient}, since including both will cause error
   * described in <a href="https://github.com/apache/iceberg/issues/6715">issue#6715</a>
   *
   * @deprecated will be removed in 1.4.0, use {@link org.apache.iceberg.aws.HttpClientProperties}
   *     instead
   */
  @Deprecated
  private <T> T loadHttpClientConfigurations(String impl) {
    Object httpClientConfigurations;
    try {
      httpClientConfigurations =
          DynMethods.builder("create")
              .hiddenImpl(impl, Map.class)
              .buildStaticChecked()
              .invoke(httpClientProperties);
      return (T) httpClientConfigurations;
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot create %s to generate and configure the http client builder", impl),
          e);
    }
  }
}

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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class AwsProperties {

  /**
   * Type of S3 Server side encryption used, default to {@link AwsProperties#S3FILEIO_SSE_TYPE_NONE}.
   */
  public static final String S3FILEIO_SSE_TYPE = "s3fileio.sse.type";

  /**
   * No server side encryption.
   */
  public static final String S3FILEIO_SSE_TYPE_NONE = "none";

  /**
   * S3 SSE-KMS encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_KMS = "kms";

  /**
   * S3 SSE-S3 encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
   */
  public static final String S3FILEIO_SSE_TYPE_S3 = "s3";

  /**
   * S3 SSE-C encryption.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
   */
  public static final String S3FILEIO_SSE_TYPE_CUSTOM = "custom";

  /**
   * If S3 encryption type is SSE-KMS, input is a KMS Key ID or ARN.
   *   In case this property is not set, default key "aws/s3" is used.
   * If encryption type is SSE-C, input is a custom base-64 AES256 symmetric key.
   */
  public static final String S3FILEIO_SSE_KEY = "s3fileio.sse.key";

  /**
   * If S3 encryption type is SSE-C, input is the base-64 MD5 digest of the secret key.
   * This MD5 must be explicitly passed in by the caller to ensure key integrity.
   */
  public static final String S3FILEIO_SSE_MD5 = "s3fileio.sse.md5";

  /**
   * The ID of the Glue Data Catalog where the tables reside.
   * If none is provided, Glue automatically uses the caller's AWS account ID by default.
   * For more details, see https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-databases.html
   */
  public static final String GLUE_CATALOG_ID = "gluecatalog.id";

  /**
   * If Glue should skip archiving an old table version when creating a new version in a commit.
   * By default Glue archives all old table versions after an UpdateTable call,
   * but Glue has a default max number of archived table versions (can be increased).
   * So for streaming use case with lots of commits, it is recommended to set this value to true.
   */
  public static final String GLUE_CATALOG_SKIP_ARCHIVE = "gluecatalog.skip-archive";
  public static final boolean GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT = false;

  /**
   * Number of threads to use for uploading parts to S3 (shared pool across all output streams).
   */
  public static final String S3FILEIO_MULTIPART_UPLOAD_THREADS  = "s3fileio.multipart.num-threads";

  /**
   * The size of a single part for multipart upload requests (default: 32MB).
   */
  public static final String S3FILEIO_MULTIPART_SIZE = "s3fileio.multipart.part.size";

  /**
   * The threshold expressed as a factor times the multipart size at which to
   * switch from uploading using a single put object request to uploading using multipart upload
   * (default: 1.5).
   */
  public static final String S3FILEIO_MULTIPART_THRESHOLD_FACTOR = "s3fileio.multipart.threshold";

  /**
   * Location to put staging files for upload to S3.
   */
  public static final String S3FILEIO_STAGING_DIRECTORY = "s3fileio.staging.dir";

  /**
   * Used to set canned access control list for S3 client to use during write.
   * For more details: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
   * The input must be one of {@link software.amazon.awssdk.services.s3.model.ObjectCannedACL},
   * such as 'public-read-write'
   * If not set, ACL will not be set for requests.
   */
  public static final String S3FILEIO_ACL = "s3fileio.acl";


  static final int MIN_MULTIPART_UPLOAD_SIZE = 5 * 1024 * 1024;
  static final int DEFAULT_MULTIPART_SIZE = 32 * 1024 * 1024;
  static final double DEFAULT_MULTIPART_THRESHOLD = 1.5;

  private String s3FileIoSseType;
  private String s3FileIoSseKey;
  private String s3FileIoSseMd5;
  private int s3FileIoMultipartUploadThreads;
  private int s3FileIoMultiPartSize;
  private double s3FileIoMultipartThresholdFactor;
  private String s3fileIoStagingDirectory;
  private ObjectCannedACL s3FileIoAcl;

  private String glueCatalogId;
  private boolean glueCatalogSkipArchive;

  public AwsProperties() {
    this.s3FileIoSseType = S3FILEIO_SSE_TYPE_NONE;
    this.s3FileIoSseKey = null;
    this.s3FileIoSseMd5 = null;
    this.s3FileIoAcl = null;

    this.s3FileIoMultipartUploadThreads = Runtime.getRuntime().availableProcessors();
    this.s3FileIoMultiPartSize = DEFAULT_MULTIPART_SIZE;
    this.s3FileIoMultipartThresholdFactor = DEFAULT_MULTIPART_THRESHOLD;
    this.s3fileIoStagingDirectory = System.getProperty("java.io.tmpdir");

    this.glueCatalogId = null;
    this.glueCatalogSkipArchive = GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT;
  }

  public AwsProperties(Map<String, String> properties) {
    this.s3FileIoSseType = properties.getOrDefault(
        AwsProperties.S3FILEIO_SSE_TYPE, AwsProperties.S3FILEIO_SSE_TYPE_NONE);
    this.s3FileIoSseKey = properties.get(AwsProperties.S3FILEIO_SSE_KEY);
    this.s3FileIoSseMd5 = properties.get(AwsProperties.S3FILEIO_SSE_MD5);
    if (AwsProperties.S3FILEIO_SSE_TYPE_CUSTOM.equals(s3FileIoSseType)) {
      Preconditions.checkNotNull(s3FileIoSseKey, "Cannot initialize SSE-C S3FileIO with null encryption key");
      Preconditions.checkNotNull(s3FileIoSseMd5, "Cannot initialize SSE-C S3FileIO with null encryption key MD5");
    }

    this.glueCatalogId = properties.get(GLUE_CATALOG_ID);
    this.glueCatalogSkipArchive = PropertyUtil.propertyAsBoolean(properties,
        AwsProperties.GLUE_CATALOG_SKIP_ARCHIVE, AwsProperties.GLUE_CATALOG_SKIP_ARCHIVE_DEFAULT);

    this.s3FileIoMultipartUploadThreads = PropertyUtil.propertyAsInt(properties, S3FILEIO_MULTIPART_UPLOAD_THREADS,
        Runtime.getRuntime().availableProcessors());

    this.s3FileIoMultiPartSize = PropertyUtil.propertyAsInt(properties, S3FILEIO_MULTIPART_SIZE,
        DEFAULT_MULTIPART_SIZE);

    this.s3FileIoMultipartThresholdFactor = PropertyUtil.propertyAsDouble(properties,
        S3FILEIO_MULTIPART_THRESHOLD_FACTOR, DEFAULT_MULTIPART_THRESHOLD);

    Preconditions.checkArgument(s3FileIoMultipartThresholdFactor >= 1.0,
        "Multipart threshold factor must be >= to 1.0");

    Preconditions.checkArgument(s3FileIoMultiPartSize >= MIN_MULTIPART_UPLOAD_SIZE,
        "Minimum multipart upload object size must be larger than 5 MB.");

    this.s3fileIoStagingDirectory = PropertyUtil.propertyAsString(properties, S3FILEIO_STAGING_DIRECTORY,
        System.getProperty("java.io.tmpdir"));

    String aclType = properties.get(S3FILEIO_ACL);
    this.s3FileIoAcl = ObjectCannedACL.fromValue(aclType);
    Preconditions.checkArgument(s3FileIoAcl == null || !s3FileIoAcl.equals(ObjectCannedACL.UNKNOWN_TO_SDK_VERSION),
        "Cannot support S3 CannedACL " + aclType);
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

  public int s3FileIoMultipartUploadThreads() {
    return s3FileIoMultipartUploadThreads;
  }

  public int s3FileIoMultiPartSize() {
    return s3FileIoMultiPartSize;
  }

  public double s3FileIOMultipartThresholdFactor() {
    return s3FileIoMultipartThresholdFactor;
  }

  public String getS3fileIoStagingDirectory() {
    return s3fileIoStagingDirectory;
  }

  public ObjectCannedACL s3FileIoAcl() {
    return this.s3FileIoAcl;
  }

  public void setS3FileIoAcl(ObjectCannedACL acl) {
    this.s3FileIoAcl = acl;
  }
}

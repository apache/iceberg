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
package org.apache.iceberg.aliyun;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

public class AliyunProperties implements Serializable {
  /**
   * The domain name used to access OSS. OSS uses HTTP Restful APIs to provide services. Different
   * regions are accessed by using different endpoints. For the same region, access over the
   * internal network or over the Internet also uses different endpoints. For more information, see:
   * https://www.alibabacloud.com/help/doc-detail/31837.htm
   */
  public static final String OSS_ENDPOINT = "oss.endpoint";

  /**
   * Aliyun uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to
   * implement symmetric encryption and verify the identity of a requester. The AccessKey ID is used
   * to identify a user.
   *
   * <p>For more information about how to obtain an AccessKey pair, see:
   * https://www.alibabacloud.com/help/doc-detail/53045.htm
   */
  public static final String CLIENT_ACCESS_KEY_ID = "client.access-key-id";

  /**
   * Aliyun uses an AccessKey pair, which includes an AccessKey ID and an AccessKey secret to
   * implement symmetric encryption and verify the identity of a requester. The AccessKey secret is
   * used to encrypt and verify the signature string.
   *
   * <p>For more information about how to obtain an AccessKey pair, see:
   * https://www.alibabacloud.com/help/doc-detail/53045.htm
   */
  public static final String CLIENT_ACCESS_KEY_SECRET = "client.access-key-secret";

  /**
   * Aliyun supports Security Token Service (STS) to generate temporary access credentials to
   * authorize a user to access the Object Storage Service (OSS) resources within a specific period
   * of time. In this way, user does not have to share the AccessKey pair and ensures higher level
   * of data security.
   *
   * <p>For more information about how to obtain a security token, see:
   * https://www.alibabacloud.com/help/en/vod/user-guide/sts-tokens
   */
  public static final String CLIENT_SECURITY_TOKEN = "client.security-token";

  /**
   * The implementation class of {@link AliyunClientFactory} to customize Aliyun client
   * configurations. If set, all Aliyun clients will be initialized by the specified factory. If not
   * set, {@link AliyunClientFactories#defaultFactory()} is used as default factory.
   */
  public static final String CLIENT_FACTORY = "client.factory-impl";

  /**
   * Location to put staging files for uploading to OSS, defaults to the directory value of
   * java.io.tmpdir.
   */
  public static final String OSS_STAGING_DIRECTORY = "oss.staging-dir";

  /**
   * The region id used to derive a service endpoint, for example {@code cn-hangzhou}. Used by the
   * Aliyun KMS client to resolve {@code kms.<region>.aliyuncs.com}.
   */
  public static final String CLIENT_REGION = "client.region";

  /**
   * Overrides the KMS endpoint. Defaults to the region-derived {@code kms.<region>.aliyuncs.com}.
   * Set to a KMS Instance (DKMS) endpoint for keys that live in a KMS Instance.
   */
  public static final String CLIENT_KMS_ENDPOINT = "client.kms-endpoint";

  /** The data key spec used when generating data keys with Aliyun KMS: AES_256 or AES_128. */
  public static final String KMS_DATA_KEY_SPEC = "kms.client.aliyun.generation.data_key_spec";

  public static final String KMS_DATA_KEY_SPEC_DEFAULT = "AES_256";

  private final String ossEndpoint;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String securityToken;
  private final String ossStagingDirectory;
  private final String region;
  private final String kmsEndpoint;
  private final String kmsDataKeySpec;

  public AliyunProperties() {
    this(ImmutableMap.of());
  }

  public AliyunProperties(Map<String, String> properties) {
    // OSS endpoint, accessKeyId, accessKeySecret.
    this.ossEndpoint = properties.get(OSS_ENDPOINT);
    this.accessKeyId = properties.get(CLIENT_ACCESS_KEY_ID);
    this.accessKeySecret = properties.get(CLIENT_ACCESS_KEY_SECRET);
    this.securityToken = properties.get(CLIENT_SECURITY_TOKEN);

    this.ossStagingDirectory =
        PropertyUtil.propertyAsString(
            properties, OSS_STAGING_DIRECTORY, System.getProperty("java.io.tmpdir"));

    this.region = properties.get(CLIENT_REGION);
    this.kmsEndpoint = properties.get(CLIENT_KMS_ENDPOINT);
    this.kmsDataKeySpec =
        PropertyUtil.propertyAsString(properties, KMS_DATA_KEY_SPEC, KMS_DATA_KEY_SPEC_DEFAULT);
  }

  public String ossEndpoint() {
    return ossEndpoint;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String accessKeySecret() {
    return accessKeySecret;
  }

  public String securityToken() {
    return securityToken;
  }

  public String ossStagingDirectory() {
    return ossStagingDirectory;
  }

  public String region() {
    return region;
  }

  public String kmsEndpoint() {
    return kmsEndpoint;
  }

  public String kmsDataKeySpec() {
    return kmsDataKeySpec;
  }
}

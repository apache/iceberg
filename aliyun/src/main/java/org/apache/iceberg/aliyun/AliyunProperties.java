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

  private final String ossEndpoint;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String ossStagingDirectory;

  public AliyunProperties() {
    this(ImmutableMap.of());
  }

  public AliyunProperties(Map<String, String> properties) {
    // OSS endpoint, accessKeyId, accessKeySecret.
    this.ossEndpoint = properties.get(OSS_ENDPOINT);
    this.accessKeyId = properties.get(CLIENT_ACCESS_KEY_ID);
    this.accessKeySecret = properties.get(CLIENT_ACCESS_KEY_SECRET);

    this.ossStagingDirectory =
        PropertyUtil.propertyAsString(
            properties, OSS_STAGING_DIRECTORY, System.getProperty("java.io.tmpdir"));
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

  public String ossStagingDirectory() {
    return ossStagingDirectory;
  }
}

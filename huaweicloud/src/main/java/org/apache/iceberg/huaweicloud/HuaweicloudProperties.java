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
package org.apache.iceberg.huaweicloud;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

public class HuaweicloudProperties implements Serializable {
  /**
   * The domain name used to access OBS.
   *
   * <p>For more information, see: https://support.huaweicloud.com/ugobs-obs/obs_41_0003.html
   */
  public static final String OBS_ENDPOINT = "obs.endpoint";

  /**
   * OBS verifies the signature of the AK and SK in the user account to ensure that only authorized
   * accounts can access specified OBS resources. The AccessKey ID is used to identify a user.
   *
   * <p>For more information about how to obtain an AccessKey Key ID, see:
   * https://support.huaweicloud.com/sdk-java-devg-obs/obs_21_0102.html
   */
  public static final String CLIENT_ACCESS_KEY_ID = "client.access-key-id";

  /**
   * OBS verifies the signature of the AK and SK in the user account to ensure that only authorized
   * accounts can access specified OBS resources. The key used by the user to access the OBS.
   *
   * <p>For more information about how to obtain a Secret Access Key, see:
   * https://support.huaweicloud.com/sdk-java-devg-obs/obs_21_0102.html
   */
  public static final String CLIENT_ACCESS_KEY_SECRET = "client.access-key-secret";

  /**
   * The implementation class of {@link HuaweicloudClientFactory} to customize Huaweicloud client
   * configurations.
   */
  public static final String CLIENT_FACTORY = "client.factory-impl";

  /**
   * Location to put staging files for uploading to OBS, defaults to the directory value of
   * java.io.tmpdir.
   */
  public static final String OBS_STAGING_DIRECTORY = "obs.staging-dir";

  private final String obsEndpoint;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String obsStagingDirectory;

  public HuaweicloudProperties() {
    this(ImmutableMap.of());
  }

  public HuaweicloudProperties(Map<String, String> properties) {
    this.obsEndpoint = properties.get(OBS_ENDPOINT);
    this.accessKeyId = properties.get(CLIENT_ACCESS_KEY_ID);
    this.accessKeySecret = properties.get(CLIENT_ACCESS_KEY_SECRET);
    this.obsStagingDirectory =
        PropertyUtil.propertyAsString(
            properties, OBS_STAGING_DIRECTORY, System.getProperty("java.io.tmpdir"));
  }

  public String obsEndpoint() {
    return obsEndpoint;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String accessKeySecret() {
    return accessKeySecret;
  }

  public String obsStagingDirectory() {
    return obsStagingDirectory;
  }
}

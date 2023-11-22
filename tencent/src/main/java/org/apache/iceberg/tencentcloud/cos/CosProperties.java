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
package org.apache.iceberg.tencentcloud.cos;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

public class CosProperties implements Serializable {
  public static final String COS_REGION = "fs.cos.bucket.region";

  public static final String COS_USERINFO_SECRET_ID = "fs.cos.userinfo.secretId";

  public static final String COS_USERINFO_SECRET_KEY = "fs.cos.userinfo.secretKey";

  public static final String CLIENT_FACTORY = "client.factory-impl";

  public static final String COS_TMP_DIR = "fs.cos.tmp.dir";

  public static final String DEFAULT_TMP_DIR = "/tmp/hadoop_cos";

  private final String cosRegion;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String cosTmpDir;

  public CosProperties() {
    this(ImmutableMap.of());
  }

  public CosProperties(Map<String, String> properties) {
    this.cosRegion = properties.get(COS_REGION);
    this.accessKeyId = properties.get(COS_USERINFO_SECRET_ID);
    this.accessKeySecret = properties.get(COS_USERINFO_SECRET_KEY);

    this.cosTmpDir = PropertyUtil.propertyAsString(properties, COS_TMP_DIR, DEFAULT_TMP_DIR);
  }

  public String cosRegion() {
    return cosRegion;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String accessKeySecret() {
    return accessKeySecret;
  }

  public String cosTmpDir() {
    return cosTmpDir;
  }
}

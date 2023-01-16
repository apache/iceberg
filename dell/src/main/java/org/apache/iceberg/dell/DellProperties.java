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
package org.apache.iceberg.dell;

import java.io.Serializable;
import java.util.Map;

public class DellProperties implements Serializable {
  /** S3 Access key id of Dell EMC ECS */
  public static final String ECS_S3_ACCESS_KEY_ID = "ecs.s3.access-key-id";

  /** S3 Secret access key of Dell EMC ECS */
  public static final String ECS_S3_SECRET_ACCESS_KEY = "ecs.s3.secret-access-key";

  /** S3 endpoint of Dell EMC ECS */
  public static final String ECS_S3_ENDPOINT = "ecs.s3.endpoint";

  /**
   * The implementation class of {@link DellClientFactory} to customize Dell client configurations.
   * If set, all Dell clients will be initialized by the specified factory. If not set, {@link
   * DellClientFactories.DefaultDellClientFactory} is used as default factory.
   */
  public static final String CLIENT_FACTORY = "client.factory";

  private String ecsS3Endpoint;
  private String ecsS3AccessKeyId;
  private String ecsS3SecretAccessKey;

  public DellProperties() {}

  public DellProperties(Map<String, String> properties) {
    this.ecsS3AccessKeyId = properties.get(DellProperties.ECS_S3_ACCESS_KEY_ID);
    this.ecsS3SecretAccessKey = properties.get(DellProperties.ECS_S3_SECRET_ACCESS_KEY);
    this.ecsS3Endpoint = properties.get(DellProperties.ECS_S3_ENDPOINT);
  }

  public String ecsS3Endpoint() {
    return ecsS3Endpoint;
  }

  public void setEcsS3Endpoint(String ecsS3Endpoint) {
    this.ecsS3Endpoint = ecsS3Endpoint;
  }

  public String ecsS3AccessKeyId() {
    return ecsS3AccessKeyId;
  }

  public void setEcsS3AccessKeyId(String ecsS3AccessKeyId) {
    this.ecsS3AccessKeyId = ecsS3AccessKeyId;
  }

  public String ecsS3SecretAccessKey() {
    return ecsS3SecretAccessKey;
  }

  public void setEcsS3SecretAccessKey(String ecsS3SecretAccessKey) {
    this.ecsS3SecretAccessKey = ecsS3SecretAccessKey;
  }
}

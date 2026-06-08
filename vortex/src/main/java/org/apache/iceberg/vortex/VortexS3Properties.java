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
package org.apache.iceberg.vortex;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class VortexS3Properties implements ObjectStoreProperties {
  private static final String ACCESS_KEY = "aws_access_key_id";
  private static final String SECRET_KEY = "aws_secret_access_key";
  private static final String SESSION_TOKEN = "aws_session_token";
  private static final String REGION = "aws_region";
  private static final String ENDPOINT = "aws_endpoint";
  private static final String SKIP_SIGNATURE = "aws_skip_signature";

  private final Map<String, String> properties = Maps.newHashMap();

  public Optional<String> accessKeyId() {
    return Optional.ofNullable(properties.get(ACCESS_KEY));
  }

  public Optional<String> secretAccessKey() {
    return Optional.ofNullable(properties.get(SECRET_KEY));
  }

  public Optional<String> sessionToken() {
    return Optional.ofNullable(properties.get(SESSION_TOKEN));
  }

  public Optional<String> region() {
    return Optional.ofNullable(properties.get(REGION));
  }

  public Optional<String> endpoint() {
    return Optional.ofNullable(properties.get(ENDPOINT));
  }

  public boolean skipSignature() {
    return Boolean.parseBoolean(properties.getOrDefault(SKIP_SIGNATURE, "false"));
  }

  public void setAccessKeyId(String accessKeyId) {
    properties.put(ACCESS_KEY, accessKeyId);
  }

  public void setSecretAccessKey(String secretAccessKey) {
    properties.put(SECRET_KEY, secretAccessKey);
  }

  public void setSessionToken(String sessionToken) {
    properties.put(SESSION_TOKEN, sessionToken);
  }

  public void setRegion(String region) {
    properties.put(REGION, region);
  }

  public void setEndpoint(String endpoint) {
    properties.put(ENDPOINT, endpoint);
  }

  public void setSkipSignature(boolean skipSignature) {
    properties.put(SKIP_SIGNATURE, Boolean.toString(skipSignature));
  }

  @Override
  public Map<String, String> asProperties() {
    return ImmutableMap.copyOf(properties);
  }
}

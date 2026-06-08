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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class VortexAzureProperties implements ObjectStoreProperties {
  private static final String ACCOUNT_KEY = "azure_storage_account_key";
  private static final String SAS_KEY = "azure_storage_sas_key";
  private static final String SKIP_SIGNATURE = "azure_skip_signature";

  private final Map<String, String> properties = Maps.newHashMap();

  public VortexAzureProperties setAccessKey(String accountKey) {
    properties.put(ACCOUNT_KEY, accountKey);
    return this;
  }

  public VortexAzureProperties setSasKey(String sasKey) {
    properties.put(SAS_KEY, sasKey);
    return this;
  }

  public VortexAzureProperties setSkipSignature(boolean skipSignature) {
    properties.put(SKIP_SIGNATURE, String.valueOf(skipSignature));
    return this;
  }

  @Override
  public Map<String, String> asProperties() {
    return ImmutableMap.copyOf(properties);
  }
}

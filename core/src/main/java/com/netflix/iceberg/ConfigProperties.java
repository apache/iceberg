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
package com.netflix.iceberg;

import org.apache.hadoop.conf.Configuration;

public class ConfigProperties {

  // properties are in alphanumeric order

  public static final String CASE_SENSITIVE = "iceberg.case.sensitive";
  public static final boolean CASE_SENSITIVE_DEFAULT = true;

  public static final String COMPRESS_METADATA = "iceberg.compress.metadata";
  public static final boolean COMPRESS_METADATA_DEFAULT = false;


  public static final boolean isCaseSensitive(Configuration configuration) {
    return configuration.getBoolean(CASE_SENSITIVE, CASE_SENSITIVE_DEFAULT);
  }

  public static final boolean shouldCompress(Configuration configuration) {
    return configuration.getBoolean(COMPRESS_METADATA, COMPRESS_METADATA_DEFAULT);
  }

}

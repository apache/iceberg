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
package org.apache.iceberg.snowflake;

final class SnowflakeResources {
  static final String DEFAULT_CATALOG_NAME = "snowlog";
  static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";
  static final int MAX_NAMESPACE_DEPTH = 2;
  static final int NAMESPACE_DB_LEVEL = 1;
  static final int NAMESPACE_SCHEMA_LEVEL = 2;

  private SnowflakeResources() {}
}

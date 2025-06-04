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
package org.apache.iceberg.connect.data.routers;

import org.apache.iceberg.connect.IcebergSinkConfig;

public class RouterConstants {
  private RouterConstants() {}

  public static final String TABLES_PROP = IcebergSinkConfig.TABLES_PROP;
  public static final String TABLES_ROUTE_FIELD_PROP = IcebergSinkConfig.TABLES_ROUTE_FIELD_PROP;
  public static final String TABLE_MAPPING = "iceberg.table-mapping";
  public static final String HEADER_ROUTE_KEYS = "iceberg.header-route-keys";
  public static final String TABLE_PROP_PREFIX = IcebergSinkConfig.TABLE_PROP_PREFIX;
  public static final String ROUTE_REGEX = IcebergSinkConfig.ROUTE_REGEX;
}

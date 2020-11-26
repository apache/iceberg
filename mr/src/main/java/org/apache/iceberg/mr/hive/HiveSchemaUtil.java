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

package org.apache.iceberg.mr.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSchemaUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaUtil.class);

  private HiveSchemaUtil() {
  }

  /**
   * Converts the list of Hive FieldSchemas to an Iceberg schema.
   * <p>
   * The list should contain the columns and the partition columns as well.
   * @param fieldSchemas The list of the columns
   * @return An equivalent Iceberg Schema
   */
  public static Schema schema(List<FieldSchema> fieldSchemas) {
    List<String> names = new ArrayList<>(fieldSchemas.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fieldSchemas.size());

    for (FieldSchema col : fieldSchemas) {
      names.add(col.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(col.getType()));
    }

    return HiveSchemaConverter.convert(names, typeInfos);
  }

  /**
   * Converts the Hive properties defining the columns to an Iceberg schema.
   * @param columnNames The property containing the column names
   * @param columnTypes The property containing the column types
   * @param columnNameDelimiter The name delimiter
   * @return The Iceberg schema
   */
  public static Schema schema(String columnNames, String columnTypes, String columnNameDelimiter) {
    // Parse the configuration parameters
    List<String> names = new ArrayList<>();
    Collections.addAll(names, columnNames.split(columnNameDelimiter));

    return HiveSchemaConverter.convert(names, TypeInfoUtils.getTypeInfosFromTypeString(columnTypes));
  }
}

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
package org.apache.iceberg.spark;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ChangelogRowBuilder {
  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType, false, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_ORDINAL.name(),
                DataTypes.IntegerType,
                false,
                Metadata.empty()),
            new StructField(
                MetadataColumns.COMMIT_SNAPSHOT_ID.name(),
                DataTypes.LongType,
                false,
                Metadata.empty())
          });

  private final List<Object[]> rows = Lists.newArrayList();

  public ChangelogRowBuilder insert(int id, String name, String data, int ordinal, long snapshot) {
    return add(id, name, data, ChangelogOperation.INSERT, ordinal, snapshot);
  }

  public ChangelogRowBuilder delete(int id, String name, String data, int ordinal, long snapshot) {
    return add(id, name, data, ChangelogOperation.DELETE, ordinal, snapshot);
  }

  public ChangelogRowBuilder updateBefore(
      int id, String name, String data, int ordinal, long snapshot) {
    return add(id, name, data, ChangelogOperation.UPDATE_BEFORE, ordinal, snapshot);
  }

  public ChangelogRowBuilder updateAfter(
      int id, String name, String data, int ordinal, long snapshot) {
    return add(id, name, data, ChangelogOperation.UPDATE_AFTER, ordinal, snapshot);
  }

  private ChangelogRowBuilder add(
      int id, String name, String data, ChangelogOperation op, int ordinal, long snapshot) {
    rows.add(new Object[] {id, name, data, op.name(), ordinal, snapshot});
    return this;
  }

  public List<Row> buildRows() {
    return rows.stream()
        .map(row -> new GenericRowWithSchema(row, SCHEMA))
        .collect(Collectors.toList());
  }

  public List<Object[]> build() {
    return rows;
  }

  public static StructType schema() {
    return SCHEMA;
  }
}

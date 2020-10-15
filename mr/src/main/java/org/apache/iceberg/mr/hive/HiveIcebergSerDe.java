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

import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;

public class HiveIcebergSerDe extends AbstractSerDe {
  private ObjectInspector inspector;
  private Schema tableSchema;

  @Override
  public void initialize(@Nullable Configuration configuration, Properties serDeProperties) throws SerDeException {
    // HiveIcebergSerDe.initialize is called multiple places in Hive code:
    // - When we are trying to create a table - HiveDDL data is stored at the serDeProperties, but no Iceberg table
    // is created yet.
    // - When we are compiling the Hive query on HiveServer2 side - We only have table information (location/name),
    // and we have to read the schema using the table data. This is called multiple times so there is room for
    // optimizing here.
    // - When we are executing the Hive query in the execution engine - We do not want to load the table data on every
    // executor, but serDeProperties are populated by HiveIcebergStorageHandler.configureInputJobProperties() and
    // the resulting properties are serialized and distributed to the executors

    if (inspector == null) {
      if (configuration.get(InputFormatConfig.TABLE_SCHEMA) != null) {
        this.tableSchema = SchemaParser.fromJson(configuration.get(InputFormatConfig.TABLE_SCHEMA));
      } else if (serDeProperties.get(InputFormatConfig.TABLE_SCHEMA) != null) {
        this.tableSchema = SchemaParser.fromJson((String) serDeProperties.get(InputFormatConfig.TABLE_SCHEMA));
      } else {
        try {
          this.tableSchema = Catalogs.loadTable(configuration, serDeProperties).schema();
        } catch (NoSuchTableException nte) {
          throw new SerDeException("Please provide an existing table or a valid schema", nte);
        }
      }

      try {
        this.inspector = IcebergObjectInspector.create(tableSchema);
      } catch (Exception e) {
        throw new SerDeException(e);
      }
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Container.class;
  }

  @Override
  public IcebergWritable serialize(Object obj, ObjectInspector outerObjectInspector) throws SerDeException {
    return new IcebergWritable(DeserializerHelper.deserialize(obj, tableSchema, outerObjectInspector));
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) {
    return ((Container<?>) writable).get();
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }
}

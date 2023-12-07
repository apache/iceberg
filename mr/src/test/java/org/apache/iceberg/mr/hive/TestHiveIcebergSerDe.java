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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHiveIcebergSerDe {

  private static final Schema schema =
      new Schema(required(1, "string_field", Types.StringType.get()));

  @TempDir
  public Path tmp;

  @Test
  public void testInitialize() throws IOException, SerDeException {
    File location = tmp.toFile();
    Assertions.assertThat(location.delete()).isTrue();

    Configuration conf = new Configuration();

    Properties properties = new Properties();
    properties.setProperty("location", location.toString());
    properties.setProperty(InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_HADOOP_TABLE_NAME);

    HadoopTables tables = new HadoopTables(conf);
    tables.create(schema, location.toString());

    HiveIcebergSerDe serDe = new HiveIcebergSerDe();
    serDe.initialize(conf, properties);

    Assertions.assertThat(serDe.getObjectInspector()).isEqualTo(IcebergObjectInspector.create(schema));
  }

  @Test
  public void testDeserialize() {
    HiveIcebergSerDe serDe = new HiveIcebergSerDe();

    Record record = RandomGenericData.generate(schema, 1, 0).get(0);
    Container<Record> container = new Container<>();
    container.set(record);

    Assertions.assertThat(serDe.deserialize(container)).isEqualTo(record);
  }
}

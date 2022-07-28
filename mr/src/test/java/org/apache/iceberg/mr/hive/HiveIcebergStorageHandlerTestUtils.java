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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.junit.rules.TemporaryFolder;

public class HiveIcebergStorageHandlerTestUtils {
  static final FileFormat[] FILE_FORMATS =
      new FileFormat[] {FileFormat.AVRO, FileFormat.ORC, FileFormat.PARQUET};

  static final Schema CUSTOMER_SCHEMA =
      new Schema(
          optional(1, "customer_id", Types.LongType.get()),
          optional(2, "first_name", Types.StringType.get(), "This is first name"),
          optional(3, "last_name", Types.StringType.get(), "This is last name"));

  static final Schema CUSTOMER_SCHEMA_WITH_UPPERCASE =
      new Schema(
          optional(1, "CustomER_Id", Types.LongType.get()),
          optional(2, "First_name", Types.StringType.get()),
          optional(3, "Last_name", Types.StringType.get()));

  static final List<Record> CUSTOMER_RECORDS =
      TestHelper.RecordsBuilder.newInstance(CUSTOMER_SCHEMA)
          .add(0L, "Alice", "Brown")
          .add(1L, "Bob", "Green")
          .add(2L, "Trudy", "Pink")
          .build();

  private HiveIcebergStorageHandlerTestUtils() {
    // Empty constructor for the utility class
  }

  static TestHiveShell shell() {
    return shell(Collections.emptyMap());
  }

  static TestHiveShell shell(Map<String, String> configs) {
    TestHiveShell shell = new TestHiveShell();
    shell.setHiveConfValue("hive.notification.event.poll.interval", "-1");
    shell.setHiveConfValue("hive.tez.exec.print.summary", "true");
    configs.forEach((k, v) -> shell.setHiveConfValue(k, v));
    // We would like to make sure that ORC reading overrides this config, so reading Iceberg tables
    // could work in
    // systems (like Hive 3.2 and higher) where this value is set to true explicitly.
    shell.setHiveConfValue(OrcConf.FORCE_POSITIONAL_EVOLUTION.getHiveConfName(), "true");
    shell.start();
    return shell;
  }

  static TestTables testTables(
      TestHiveShell shell, TestTables.TestTableType testTableType, TemporaryFolder temp)
      throws IOException {
    return testTables(shell, testTableType, temp, Catalogs.ICEBERG_DEFAULT_CATALOG_NAME);
  }

  static TestTables testTables(
      TestHiveShell shell,
      TestTables.TestTableType testTableType,
      TemporaryFolder temp,
      String catalogName)
      throws IOException {
    return testTableType.instance(shell.metastore().hiveConf(), temp, catalogName);
  }

  static void init(
      TestHiveShell shell, TestTables testTables, TemporaryFolder temp, String engine) {
    shell.openSession();

    for (Map.Entry<String, String> property : testTables.properties().entrySet()) {
      shell.setHiveSessionValue(property.getKey(), property.getValue());
    }

    shell.setHiveSessionValue("hive.execution.engine", engine);
    shell.setHiveSessionValue("hive.jar.directory", temp.getRoot().getAbsolutePath());
    shell.setHiveSessionValue("tez.staging-dir", temp.getRoot().getAbsolutePath());
  }

  static void close(TestHiveShell shell) throws Exception {
    shell.closeSession();
    shell.metastore().reset();
    // HiveServer2 thread pools are using thread local Hive -> HMSClient objects. These are not
    // cleaned up when the
    // HiveServer2 is stopped. Only Finalizer closes the HMS connections.
    System.gc();
  }
}

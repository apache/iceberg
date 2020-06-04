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

package org.apache.iceberg.mr.mapred;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveIcebergInputFormat {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableLocation;

  @Before
  public void before() throws IOException {
    tableLocation = temp.newFolder();
    Schema schema = new Schema(required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    HadoopTables tables = new HadoopTables();
    Table table = tables.create(schema, spec, tableLocation.getAbsolutePath());
  }

  @Test
  public void emptyTable() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.table_a ")
            .append("(id INT, data STRING) ")
            .append("STORED AS ")
            .append("INPUTFORMAT 'org.apache.iceberg.mr.mapred.IcebergInputFormat' ")
            .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
            .append("LOCATION '")
            .append(tableLocation.getAbsolutePath())
            .append("' TBLPROPERTIES ('iceberg.catalog'='hadoop.tables'")
            .append(")")
            .toString());

    List<Object[]> result = shell.executeStatement("SELECT id, data FROM source_db.table_a");

    assertEquals(0, result.size());
  }

}

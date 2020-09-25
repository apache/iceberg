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

package org.apache.iceberg.flink.source;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.DeleteReadTests;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkInputFormatReaderDeletes extends DeleteReadTests {
  private final Configuration conf = new Configuration();
  private final HadoopTables tables = new HadoopTables(conf);
  private final FileFormat format;

  private String tableLocation;

  @Parameterized.Parameters(name = "fileFormat={0}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { FileFormat.PARQUET },
        new Object[] { FileFormat.AVRO },
        new Object[] { FileFormat.ORC }
    };
  }

  public TestFlinkInputFormatReaderDeletes(FileFormat inputFormat) {
    this.format = inputFormat;
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) throws IOException {
    File location = temp.newFolder(format.name(), name);
    Assert.assertTrue(location.delete());
    this.tableLocation = location.toURI().toString();

    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.DEFAULT_FILE_FORMAT, format.name());

    Table table = tables.create(schema, spec, props, tableLocation);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    return table;
  }

  @Override
  protected void dropTable(String name) {
    tables.dropTable(tableLocation, true);
  }

  @Override
  protected StructLikeSet rowSet(String name, Table testTable, String... columns) throws IOException {
    FlinkSource.Builder builder = FlinkSource.forRowData().tableLoader(TableLoader.fromHadoopTable(tableLocation));
    Schema projected = testTable.schema().select(columns);
    RowType rowType = FlinkSchemaUtil.convert(projected);
    FlinkInputFormat inputFormat = builder.project(FlinkSchemaUtil.toSchema(rowType)).buildFormat();

    StructLikeSet set = StructLikeSet.create(projected.asStruct());
    TestFlinkInputFormat.getRowData(inputFormat).forEach(rowData -> {
      RowDataWrapper wrapper = new RowDataWrapper(rowType, projected.asStruct());
      set.add(wrapper.wrap(rowData));
    });

    return set;
  }

  @Override
  protected boolean expectPruned() {
    return false;
  }
}

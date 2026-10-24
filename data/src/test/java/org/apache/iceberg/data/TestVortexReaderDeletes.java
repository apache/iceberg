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
package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Exercises the full delete-read path for Vortex tables: data and (v2) position-delete files are
 * written through the standard {@link GenericFileWriterFactory} (which routes Vortex through the
 * {@link org.apache.iceberg.formats.FormatModelRegistry}), and reads apply position deletes via
 * native scan pushdown.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestVortexReaderDeletes extends DeleteReadTests {
  @TempDir private File tableDir;

  @Parameters(name = "fileFormat = {0}, formatVersion = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {FileFormat.VORTEX, 2},
      new Object[] {FileFormat.VORTEX, 3},
    };
  }

  @Override
  protected Table createTable(String name, Schema schema, PartitionSpec spec) throws IOException {
    return TestTables.create(
        tableDir,
        name,
        schema,
        spec,
        formatVersion,
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.VORTEX.name()));
  }

  @Override
  protected void dropTable(String name) {
    TestTables.clearTables();
  }

  @Override
  public StructLikeSet rowSet(String name, Table table, String... columns) throws IOException {
    Types.StructType schema = table.schema().select(columns).asStruct();
    StructLikeSet set = StructLikeSet.create(schema);
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      Iterables.addAll(
          set,
          CloseableIterable.transform(
              reader, record -> new InternalRecordWrapper(schema).wrap(record)));
    }
    return set;
  }

  @Override
  protected boolean expectPruned() {
    return false;
  }
}

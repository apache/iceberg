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

package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public abstract class TestWriterMetrics<T> {

  private static final int FORMAT_V2 = 2;

  protected static final Types.NestedField ID_FIELD = required(1, "id", Types.IntegerType.get());
  protected static final Types.NestedField DATA_FIELD = optional(2, "data", Types.StringType.get());

  protected static final Types.StructType NESTED_FIELDS = Types.StructType.of(
      required(4, "booleanField", Types.BooleanType.get()),
      optional(5, "longValue", Types.LongType.get()));

  protected static final Types.NestedField STRUCT_FIELD = optional(3, "structField", NESTED_FIELDS);

  // create a schema with all supported fields
  protected static final Schema SCHEMA = new Schema(
      ID_FIELD,
      DATA_FIELD,
      STRUCT_FIELD
  );

  protected static final SortOrder sortOrder =
      SortOrder.builderFor(SCHEMA).asc("id").asc("structField.longValue").build();

  protected static final Map<String, String> properties =
      ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none");

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected FileFormat fileFormat;
  protected TestTables.TestTable table = null;
  protected File metadataDir = null;
  private OutputFileFactory fileFactory = null;

  @Parameterized.Parameters(name = "FileFormat = {0}")
  public static Object[][] parameters() {
    return new Object[][] {
        {FileFormat.ORC},
        {FileFormat.PARQUET}
    };
  }

  public TestWriterMetrics(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }

  protected abstract FileWriterFactory<T> newWriterFactory(Schema dataSchema);

  protected abstract T toRow(Integer id, String data, boolean boolValue, Long longValue);

  @Before
  public void setupTable() throws Exception {
    File tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = TestTables.create(
        tableDir,
        "test",
        SCHEMA,
        PartitionSpec.unpartitioned(),
        sortOrder,
        FORMAT_V2);
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none").commit();

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @After
  public void after() {
    TestTables.clearTables();
  }

  @Test
  public void verifySortedColMetric() throws Exception {
    T row = toRow(3, "3", true, 3L);
    DataWriter dataWriter = newWriterFactory(SCHEMA).newDataWriter(
        fileFactory.newOutputFile(),
        PartitionSpec.unpartitioned(),
        null
    );
    dataWriter.write(row);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();

    // Only two sorted fields (id, structField.longValue) will have metrics
    Map<Integer, ByteBuffer> lowerBounds = dataFile.lowerBounds();
    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertFalse(lowerBounds.containsKey(2));
    Assert.assertFalse(lowerBounds.containsKey(3));
    Assert.assertFalse(lowerBounds.containsKey(4));
    Assert.assertEquals(3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)));

    Map<Integer, ByteBuffer> upperBounds = dataFile.upperBounds();
    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertFalse(upperBounds.containsKey(2));
    Assert.assertFalse(upperBounds.containsKey(3));
    Assert.assertFalse(upperBounds.containsKey(4));
    Assert.assertEquals(3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)));
  }

  @Test
  public void testPositionDeleteMetrics() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(SCHEMA);
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    PositionDeleteWriter<T> deleteWriter = writerFactory.newPositionDeleteWriter(outputFile, table.spec(), null);

    try {
      T deletedRow = toRow(3, "3", true, 3L);
      PositionDelete<T> positionDelete = PositionDelete.create();
      positionDelete.set("File A", 1, deletedRow);
      deleteWriter.write(positionDelete);
    } finally {
      deleteWriter.close();
    }

    DeleteFile deleteFile = deleteWriter.toDeleteFile();

    int pathFieldId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    int posFieldId = MetadataColumns.DELETE_FILE_POS.fieldId();

    // should have metrics for _file and _pos as well as two sorted fields (id, structField.longValue)

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();

    Assert.assertEquals(
        CharBuffer.wrap("File A"),
        Conversions.fromByteBuffer(Types.StringType.get(), lowerBounds.get(pathFieldId)));
    Assert.assertEquals(1L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(posFieldId)));

    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), lowerBounds.get(1)));
    Assert.assertFalse(lowerBounds.containsKey(2));
    Assert.assertFalse(lowerBounds.containsKey(3));
    Assert.assertFalse(lowerBounds.containsKey(4));
    Assert.assertEquals(3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), lowerBounds.get(5)));

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();

    Assert.assertEquals(
        CharBuffer.wrap("File A"),
        Conversions.fromByteBuffer(Types.StringType.get(), upperBounds.get(pathFieldId)));
    Assert.assertEquals(1L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(posFieldId)));

    Assert.assertEquals(3, (int) Conversions.fromByteBuffer(Types.IntegerType.get(), upperBounds.get(1)));
    Assert.assertFalse(upperBounds.containsKey(2));
    Assert.assertFalse(upperBounds.containsKey(3));
    Assert.assertFalse(upperBounds.containsKey(4));
    Assert.assertEquals(3L, (long) Conversions.fromByteBuffer(Types.LongType.get(), upperBounds.get(5)));
  }
}

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

package org.apache.iceberg.data.orc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class TestReadOrcFileWithoutIDs {

  private static final Types.StructType SUPPORTED_PRIMITIVES = Types.StructType.of(
      required(100, "id", Types.LongType.get()),
      optional(101, "data", Types.StringType.get()),
      required(102, "b", Types.BooleanType.get()),
      optional(103, "i", Types.IntegerType.get()),
      required(104, "l", Types.LongType.get()),
      optional(105, "f", Types.FloatType.get()),
      required(106, "d", Types.DoubleType.get()),
      optional(107, "date", Types.DateType.get()),
      required(108, "tsTz", Types.TimestampType.withZone()),
      required(109, "ts", Types.TimestampType.withoutZone()),
      required(110, "s", Types.StringType.get()),
      optional(113, "bytes", Types.BinaryType.get()),
      required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(116, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
//      Disabled some primitives because they cannot work without Iceberg's type attributes and hence won't be present
//      in old data anyway
//      required(112, "fixed", Types.FixedType.ofLength(7))
//      required(117, "time", Types.TimeType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateORCFileWithoutIds() throws IOException {
    Types.StructType structType = Types.StructType.of(
        required(0, "id", Types.LongType.get()),
        optional(1, "list_of_maps",
            Types.ListType.ofOptional(2, Types.MapType.ofOptional(3, 4,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES))),
        optional(5, "map_of_lists",
            Types.MapType.ofOptional(6, 7,
                Types.StringType.get(),
                Types.ListType.ofOptional(8, SUPPORTED_PRIMITIVES))),
        required(9, "list_of_lists",
            Types.ListType.ofOptional(10, Types.ListType.ofOptional(11, SUPPORTED_PRIMITIVES))),
        required(12, "map_of_maps",
            Types.MapType.ofOptional(13, 14,
                Types.StringType.get(),
                Types.MapType.ofOptional(15, 16,
                    Types.StringType.get(),
                    SUPPORTED_PRIMITIVES))),
        required(17, "list_of_struct_of_nested_types", Types.ListType.ofOptional(19, Types.StructType.of(
            Types.NestedField.required(20, "m1", Types.MapType.ofOptional(21, 22,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(23, "l1", Types.ListType.ofRequired(24, SUPPORTED_PRIMITIVES)),
            Types.NestedField.required(25, "l2", Types.ListType.ofRequired(26, SUPPORTED_PRIMITIVES)),
            Types.NestedField.optional(27, "m2", Types.MapType.ofOptional(28, 29,
                Types.StringType.get(),
                SUPPORTED_PRIMITIVES))
        )))
    );

    Schema schema = new Schema(TypeUtil.assignFreshIds(structType, new AtomicInteger(0)::incrementAndGet)
        .asStructType().fields());

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    List<Record> expected = RandomGenericData.generate(schema, 100, 0L);

    try (OrcWriter writer = new OrcWriter(schema, testFile)) {
      for (Record record : expected) {
        writer.write(record);
      }
    }

    Assert.assertEquals("Ensure written file does not have IDs in the file schema", 0,
        clearAttributes(orcFileSchema(testFile)));

    List<Record> rows;
    try (CloseableIterable<Record> reader = ORC.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(schema, fileSchema))
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  private static TypeDescription orcFileSchema(File file) throws IOException {
    return OrcFile.createReader(new Path(file.getPath()), OrcFile.readerOptions(new Configuration())).getSchema();
  }

  /**
   * Remove attributes from a given {@link TypeDescription}
   * @param schema the {@link TypeDescription} to remove attributes from
   * @return number of attributes removed
   */
  public static int clearAttributes(TypeDescription schema) {
    int result = 0;
    for (String attribute : schema.getAttributeNames()) {
      schema.removeAttribute(attribute);
      result += 1;
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        result += clearAttributes(child);
      }
    }
    return result;
  }

  private static class OrcWriter implements Closeable {

    private final VectorizedRowBatch batch;
    private final Writer writer;
    private final OrcRowWriter<Record> valueWriter;
    private final File outputFile;
    private boolean isClosed = false;

    private OrcWriter(Schema schema, File file) {
      TypeDescription orcSchema = ORCSchemaUtil.convert(schema);
      // clear attributes before writing schema to file so that file schema does not have IDs
      TypeDescription orcSchemaWithoutAttributes = orcSchema.clone();
      clearAttributes(orcSchemaWithoutAttributes);

      this.outputFile = file;
      this.batch = orcSchemaWithoutAttributes.createRowBatch(VectorizedRowBatch.DEFAULT_SIZE);
      OrcFile.WriterOptions options = OrcFile.writerOptions(new Configuration()).useUTCTimestamp(true);
      options.setSchema(orcSchemaWithoutAttributes);

      final Path locPath = new Path(file.getPath());
      try {
        this.writer = OrcFile.createWriter(locPath, options);
      } catch (IOException e) {
        throw new RuntimeException("Can't create file " + locPath, e);
      }
      this.valueWriter = GenericOrcWriter.buildWriter(schema, orcSchema);
    }

    void write(Record record) {
      try {
        valueWriter.write(record, batch);
        if (batch.size == VectorizedRowBatch.DEFAULT_SIZE) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      } catch (IOException e) {
        throw new RuntimeException("Problem writing to ORC file " + outputFile.getPath(), e);
      }
    }

    @Override
    public void close() throws IOException {
      if (!isClosed) {
        try {
          if (batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
          }
        } finally {
          writer.close();
          this.isClosed = true;
        }
      }
    }
  }
}

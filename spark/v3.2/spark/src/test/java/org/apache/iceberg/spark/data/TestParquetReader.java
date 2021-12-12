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

package org.apache.iceberg.spark.data;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestParquetReader {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testParquet2LevelList() throws IOException {
    Schema icebergSchema = new Schema(
            optional(1, "key", Types.StringType.get()),
            optional(2, "val", Types.ListType.ofRequired(3, Types.StructType.of(
                              optional(4, "a1", Types.StringType.get()),
                              optional(5, "a2", Types.StringType.get())
                      )
                    )));
    List<InternalRow> rows;

    /* Using a static file rather than generating test data in test, as parquet writers in Iceberg only supports
     * three level lists. The twoLevelList.pq is a parquet file that contains following Parquet schema.
     * message hive_schema {
     *  optional binary key (STRING);
     *  optional group val (LIST) {
     *    repeated group bag {
     *      optional group array_element {
     *        optional binary a1 (STRING);
     *        optional binary a2 (STRING);
     *      }
     *    }
     *  }
     * }
     *
     * It contains only one row. Below is the json dump of the file.
     * {"key":"k1","val":{"bag":[{"array_element":{"a1":"a","a2":"b"}}]}}
     */
    try (CloseableIterable<InternalRow> reader =
                 Parquet.read(Files.localInput(
                         this.getClass().getClassLoader().getResource("twoLevelList.pq").getPath()))
                         .project(icebergSchema)
                         .createReaderFunc(type -> SparkParquetReaders.buildReader(icebergSchema, type))
                         .build()) {
      rows = Lists.newArrayList(reader);
    }

    Assert.assertEquals(1, rows.size());
    InternalRow row = rows.get(0);
    Assert.assertEquals("k1", row.getString(0));

    /*
     * Iceberg's parquet reader would read 2-level list as null, which is due to Iceberg's parquet reader
     * does not support reading 2-level lists.
     */
    Assert.assertTrue("Parquet reader silently incorrectly reads null.", row.isNullAt(1));
  }
}

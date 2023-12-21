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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcWrite {
  @TempDir private Path temp;

  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  public void splitOffsets() throws IOException {
    File testFile = File.createTempFile(UUID.randomUUID().toString(), null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    Iterable<InternalRow> rows = RandomData.generateSpark(SCHEMA, 1, 0L);
    FileAppender<InternalRow> writer =
        ORC.write(Files.localOutput(testFile))
            .createWriterFunc(SparkOrcWriter::new)
            .schema(SCHEMA)
            .build();

    writer.addAll(rows);
    writer.close();
    assertThat(writer.splitOffsets()).as("Split offsets not present").isNotNull();
  }
}

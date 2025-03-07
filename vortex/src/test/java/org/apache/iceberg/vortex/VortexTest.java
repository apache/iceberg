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
package org.apache.iceberg.vortex;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.vortex.GenericVortexReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public final class VortexTest {
  private static final Schema EMPLOYEE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "name", Types.StringType.get()),
          required(3, "salary", Types.LongType.get()));

  @TempDir private static File tempDir;

  @Test
  public void testGenericReader() throws IOException {
    InputFile inputFile = loadResourceFile("employees.vortex", tempDir);

    try (CloseableIterable<Record> records =
        Vortex.read(inputFile)
            .project(EMPLOYEE_SCHEMA)
            .readerFunction(GenericVortexReader::buildReader)
            .build()) {

      assertThat(records)
          .containsExactly(
              makeEmployee(1L, "Alice", 1_000),
              makeEmployee(2L, "Bob", 2_000),
              makeEmployee(3L, "Carol", 3_000));
    }
  }

  private static InputFile loadResourceFile(String location, File outDir) {
    outDir.mkdirs();

    var outPath = outDir.toPath().resolve(location);

    try (InputStream inputStream = VortexTest.class.getResourceAsStream(location);
        OutputStream outputStream = Files.newOutputStream(outPath)) {
      if (inputStream == null) {
        throw new IllegalArgumentException("Resource not found: " + location);
      }
      ByteStreams.copy(inputStream, outputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource: " + location, e);
    }

    return new InputFile() {
      @Override
      public long getLength() {
        return 0;
      }

      @Override
      public SeekableInputStream newStream() {
        return null;
      }

      @Override
      public String location() {
        return outPath.toAbsolutePath().toString();
      }

      @Override
      public boolean exists() {
        return true;
      }
    };
  }

  private static Record makeEmployee(long id, String name, long salary) {
    GenericRecord record = GenericRecord.create(EMPLOYEE_SCHEMA);
    record.set(0, id);
    record.set(1, name);
    record.set(2, salary);

    return record;
  }
}

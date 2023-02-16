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
package org.apache.iceberg.avro;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InMemoryOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AvroTest {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @BeforeEach
  void setUp() {}

  @Test
  void overwriteIsUsedForLocalOutputFile() throws IOException {
    OutputFile out = mock(Files.LocalOutputFile.class);
    Mockito.when(out.createOrOverwrite()).thenReturn(testOutputStream());

    buildAvroWriter(out);
    verify(out, times(1)).createOrOverwrite();
  }

  @Test
  void overwriteIsNotUsedForNonHadoopLocalOutputFile() throws IOException {
    OutputFile out = mock(InMemoryOutputFile.class);
    Mockito.when(out.create()).thenReturn(testOutputStream());

    buildAvroWriter(out);
    verify(out, times(1)).create();
  }

  @Test
  void overwriteIsUsedForHadoopOutputFile() throws IOException {
    OutputFile out = mock(HadoopOutputFile.class);
    Mockito.when(out.createOrOverwrite()).thenReturn(testOutputStream());

    buildAvroWriter(out);
    verify(out, times(1)).createOrOverwrite();
  }

  private void buildAvroWriter(OutputFile out) throws IOException {
    try (FileAppender<Object> writer =
        Avro.write(out)
            .set(TableProperties.AVRO_COMPRESSION, "uncompressed")
            .createWriterFunc(DataWriter::create)
            .schema(SCHEMA)
            .overwriteIfNeeded()
            .build()) {
    } catch (Exception e) {
      // ignore write exception as we are only verifying file creation
    }
  }

  private PositionOutputStream testOutputStream() {
    return new PositionOutputStream() {
      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void write(int b) throws IOException {
        // do nothing
      }
    };
  }
}

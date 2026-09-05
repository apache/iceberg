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
package org.apache.iceberg.spark.source;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

class StreamingInitialOffsetStore {
  private static final Joiner SLASH = Joiner.on("/");

  private final FileIO io;
  private final String initialOffsetLocation;
  private final Supplier<StreamingOffset> offsetSupplier;

  StreamingInitialOffsetStore(
      String checkpointLocation,
      Configuration conf,
      Supplier<StreamingOffset> offsetSupplier) {
    this.io = new HadoopFileIO(conf);
    this.initialOffsetLocation = SLASH.join(checkpointLocation, "offsets/0");
    this.offsetSupplier = offsetSupplier;
  }

  StreamingOffset initialOffset() {
    InputFile inputFile = io.newInputFile(initialOffsetLocation);
    if (inputFile.exists()) {
      return readOffset(inputFile);
    }

    StreamingOffset offset = offsetSupplier.get();
    writeOffset(offset, io.newOutputFile(initialOffsetLocation));
    return offset;
  }

  private void writeOffset(StreamingOffset offset, OutputFile file) {
    try (OutputStream outputStream = file.create();
        BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
      writer.write(offset.json());
      writer.flush();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed writing offset to: " + initialOffsetLocation, e);
    }
  }

  private StreamingOffset readOffset(InputFile file) {
    try (InputStream in = file.newStream()) {
      return StreamingOffset.fromJson(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed reading offset from: " + initialOffsetLocation, e);
    }
  }
}

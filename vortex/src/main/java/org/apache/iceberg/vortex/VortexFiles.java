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

import dev.vortex.io.NativeReadable;
import dev.vortex.jni.NativeFiles;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.io.InputFile;

/** Public accessors for the file-level metadata a Vortex file carries. */
public class VortexFiles {
  private VortexFiles() {}

  /**
   * Reads the key/value metadata stored in a Vortex file.
   *
   * <p>This is the channel Iceberg uses to persist the writer's schema (under {@link
   * VortexSchemas#ICEBERG_SCHEMA_KEY}, so that readers can rebind renamed columns by field id) as
   * well as any user metadata set through the write builder's {@code meta} methods.
   *
   * @param inputFile the Vortex file to read
   * @return the file's metadata, keyed by name, with values exactly as written
   * @throws IOException if the file cannot be read
   */
  public static Map<String, byte[]> metadata(InputFile inputFile) throws IOException {
    try (NativeReadable readable = VortexIO.readable(inputFile)) {
      return NativeFiles.readMetadata(VortexSessions.shared(), readable);
    }
  }
}

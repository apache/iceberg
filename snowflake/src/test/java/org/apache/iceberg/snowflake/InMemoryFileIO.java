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
package org.apache.iceberg.snowflake;

import java.util.Map;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class InMemoryFileIO implements FileIO {

  private Map<String, InMemoryInputFile> inMemoryFiles = Maps.newHashMap();

  public void addFile(String path, byte[] contents) {
    inMemoryFiles.put(path, new InMemoryInputFile(path, contents));
  }

  @Override
  public InputFile newInputFile(String path) {
    if (!inMemoryFiles.containsKey(path)) {
      throw new NotFoundException("No in-memory file found for path: %s", path);
    }
    return inMemoryFiles.get(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return null;
  }

  @Override
  public void deleteFile(String path) {}
}

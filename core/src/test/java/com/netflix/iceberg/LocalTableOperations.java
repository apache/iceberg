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

package com.netflix.iceberg;

import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static com.netflix.iceberg.Files.localInput;

class LocalTableOperations implements TableOperations {
  private final TemporaryFolder temp;

  LocalTableOperations(TemporaryFolder temp) {
    this.temp = temp;
  }

  @Override
  public TableMetadata current() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public TableMetadata refresh() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Not implemented for tests");
  }

  @Override
  public InputFile newInputFile(String path) {
    return localInput(path);
  }

  @Override
  public OutputFile newMetadataFile(String filename) {
    try {
      File metadataFile = temp.newFile(filename);
      metadataFile.delete();
      metadataFile.deleteOnExit();
      return Files.localOutput(metadataFile);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public void deleteFile(String path) {
    new File(path).delete();
  }

  @Override
  public long newSnapshotId() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }
}

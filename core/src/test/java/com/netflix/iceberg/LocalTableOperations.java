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
import com.netflix.iceberg.io.FileIO;
import java.io.IOException;
import org.junit.rules.TemporaryFolder;

class LocalTableOperations implements TableOperations {
  private final TemporaryFolder temp;
  private FileIO io;

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
  public FileIO io() {
    if (io == null) {
      try {
        io = new TestTables.LocalFileIO(
            temp.newFolder("metadata"),
            temp.newFolder("data"));
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }
    }
    return io;
  }

  @Override
  public long newSnapshotId() {
    throw new UnsupportedOperationException("Not implemented for tests");
  }
}

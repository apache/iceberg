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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.TestTables;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestFileIOTracker {

  @TempDir private File tableDir;

  @SuppressWarnings("resource")
  @Test
  public void nullTableOps() {
    assertThatThrownBy(() -> new FileIOTracker().track(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table ops: null");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void fileIOGetsClosed() throws NoSuchFieldException, IllegalAccessException {
    FileIOTracker fileIOTracker = new FileIOTracker();

    FileIO firstFileIO = Mockito.spy(new TestTables.LocalFileIO());
    TestTables.TestTableOperations firstOps =
        new TestTables.TestTableOperations("x", tableDir, firstFileIO);
    fileIOTracker.track(firstOps);
    assertThat(fileIOTracker.tracker().estimatedSize()).isEqualTo(1);

    FileIO secondFileIO = Mockito.spy(new TestTables.LocalFileIO());
    TestTables.TestTableOperations secondOps =
        new TestTables.TestTableOperations("y", tableDir, secondFileIO);
    fileIOTracker.track(secondOps);
    assertThat(fileIOTracker.tracker().estimatedSize()).isEqualTo(2);

    fileIOTracker.close();
    Awaitility.await("FileIO gets closed")
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(fileIOTracker.tracker().estimatedSize()).isEqualTo(0);
              Mockito.verify(firstFileIO, times(1)).close();
              Mockito.verify(secondFileIO, times(1)).close();
            });
  }
}

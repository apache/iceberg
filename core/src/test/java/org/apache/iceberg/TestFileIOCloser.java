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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;

import com.github.benmanes.caffeine.cache.Cache;
import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOCloser;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestFileIOCloser {

  @TempDir private File tableDir;

  @SuppressWarnings("resource")
  @Test
  public void nullTableOps() {
    assertThatThrownBy(() -> new FileIOCloser().trackFileIO(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table ops: null");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void fileIOGetsClosed() throws NoSuchFieldException, IllegalAccessException {
    FileIOCloser fileIOCloser = new FileIOCloser();
    Field declaredField = fileIOCloser.getClass().getDeclaredField("closer");
    declaredField.setAccessible(true);
    assertThat(declaredField.get(fileIOCloser)).isInstanceOf(Cache.class);
    Cache<TableOperations, FileIO> closer =
        ((Cache<TableOperations, FileIO>) declaredField.get(fileIOCloser));

    FileIO firstFileIO = Mockito.spy(new TestTables.LocalFileIO());
    TestTables.TestTableOperations firstOps =
        new TestTables.TestTableOperations("x", tableDir, firstFileIO);
    fileIOCloser.trackFileIO(firstOps);
    assertThat(closer.estimatedSize()).isEqualTo(1);

    FileIO secondFileIO = Mockito.spy(new TestTables.LocalFileIO());
    TestTables.TestTableOperations secondOps =
        new TestTables.TestTableOperations("y", tableDir, secondFileIO);
    fileIOCloser.trackFileIO(secondOps);
    assertThat(closer.estimatedSize()).isEqualTo(2);

    fileIOCloser.close();
    Awaitility.await("FileIO gets closed")
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(closer.estimatedSize()).isEqualTo(0);
              Mockito.verify(firstFileIO, times(1)).close();
              Mockito.verify(secondFileIO, times(1)).close();
            });
  }
}

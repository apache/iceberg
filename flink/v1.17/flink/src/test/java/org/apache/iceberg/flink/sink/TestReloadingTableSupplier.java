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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.awaitility.Awaitility;
import org.junit.Test;

public class TestReloadingTableSupplier {

  @Test
  public void testCheckArguments() {
    Table initialTable = mock(Table.class);

    Table loadedTable = mock(Table.class);
    TableLoader tableLoader = mock(TableLoader.class);
    when(tableLoader.loadTable()).thenReturn(loadedTable);

    new ReloadingTableSupplier(initialTable, tableLoader, 100);

    assertThatThrownBy(() -> new ReloadingTableSupplier(initialTable, tableLoader, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("reloadIntervalMs must be > 0");
    assertThatThrownBy(() -> new ReloadingTableSupplier(null, tableLoader, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("initialTable cannot be null");
    assertThatThrownBy(() -> new ReloadingTableSupplier(initialTable, null, 100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("tableLoader cannot be null");
  }

  @Test
  public void testTableReload() {
    Table initialTable = mock(Table.class);

    Table loadedTable = mock(Table.class);
    TableLoader tableLoader = mock(TableLoader.class);
    when(tableLoader.loadTable()).thenReturn(loadedTable);

    Supplier<Table> supplier = new ReloadingTableSupplier(initialTable, tableLoader, 100);
    assertThat(supplier.get()).isEqualTo(initialTable);
    Awaitility.await()
        .atLeast(100, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(supplier.get()).isEqualTo(loadedTable));
  }

  @Test
  public void testCalcNextReloadTimeMs() {
    Table initialTable = mock(Table.class);

    Table loadedTable = mock(Table.class);
    TableLoader tableLoader = mock(TableLoader.class);
    when(tableLoader.loadTable()).thenReturn(loadedTable);

    long intervalMs = 1000;
    long maxJitter = Math.round(intervalMs * ReloadingTableSupplier.JITTER_PCT);
    ReloadingTableSupplier supplier =
        new ReloadingTableSupplier(initialTable, tableLoader, intervalMs);
    IntStream.range(0, 100)
        .forEach(
            i -> {
              long result = supplier.calcNextReloadTimeMs(0);
              assertThat(result).isBetween(intervalMs, intervalMs + maxJitter);
            });
  }
}

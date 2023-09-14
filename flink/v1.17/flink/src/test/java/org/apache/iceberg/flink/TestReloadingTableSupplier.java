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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

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
        .hasMessage("minReloadIntervalMs must be > 0");
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

    ReloadingTableSupplier supplier = new ReloadingTableSupplier(initialTable, tableLoader, 100);

    // initial refresh shouldn't do anything as the min reload interval hasn't passed
    supplier.refreshTable();
    assertThat(supplier.get()).isEqualTo(initialTable);

    // refresh after waiting past the min reload interval
    Awaitility.await()
        .atLeast(100, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              supplier.refreshTable();
              assertThat(supplier.get()).isEqualTo(loadedTable);
            });
  }
}

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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

public class TestIcebergSinkTask {

  interface CloseableCatalog extends Catalog, AutoCloseable {}

  @Test
  public void testStopClosesCatalogEvenIfCommitterThrows() throws Exception {
    IcebergSinkTask task = new IcebergSinkTask();

    Committer committer = mock(Committer.class);
    doThrow(new ConnectException("coordinator shutdown timeout")).when(committer).close(any());

    CloseableCatalog catalog = mock(CloseableCatalog.class);

    Field committerField = IcebergSinkTask.class.getDeclaredField("committer");
    Field catalogField = IcebergSinkTask.class.getDeclaredField("catalog");
    committerField.setAccessible(true);
    catalogField.setAccessible(true);
    committerField.set(task, committer);
    catalogField.set(task, catalog);

    assertThatThrownBy(task::stop)
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("timeout");
    verify(catalog).close();
  }
}

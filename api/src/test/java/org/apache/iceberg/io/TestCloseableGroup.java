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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Closeable;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCloseableGroup {

  @Test
  public void callCloseToAllCloseables() throws IOException {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    Closeable closeable2 = Mockito.mock(Closeable.class);
    Closeable closeable3 = Mockito.mock(Closeable.class);

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(closeable1);
    closeableGroup.addCloseable(closeable2);
    closeableGroup.addCloseable(closeable3);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void callCloseHandlesAutoCloseable() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(closeable1);
    closeableGroup.addCloseable(closeable2);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
  }

  @Test
  public void suppressExceptionIfSetSuppressIsTrue() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);
    Closeable closeable3 = Mockito.mock(Closeable.class);
    Mockito.doThrow(new IOException("exception1")).when(closeable1).close();
    Mockito.doThrow(new RuntimeException("exception2")).when(closeable2).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(closeable1);
    closeableGroup.addCloseable(closeable2);
    closeableGroup.addCloseable(closeable3);

    closeableGroup.setSuppressCloseFailure(true);
    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void notSuppressExceptionIfSetSuppressIsFalse() throws Exception {
    IOException ioException = new IOException("e1");

    Closeable closeable1 = Mockito.mock(Closeable.class);
    Closeable closeable2 = Mockito.mock(Closeable.class);
    Closeable closeable3 = Mockito.mock(Closeable.class);
    Mockito.doThrow(ioException).when(closeable2).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(closeable1);
    closeableGroup.addCloseable(closeable2);
    closeableGroup.addCloseable(closeable3);

    assertThatThrownBy(closeableGroup::close).isEqualTo(ioException);
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verifyNoInteractions(closeable3);
  }

  @Test
  public void notSuppressExceptionIfSetSuppressIsFalseForAutoCloseable() throws Exception {
    IOException ioException = new IOException("e1");

    AutoCloseable closeable1 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable3 = Mockito.mock(AutoCloseable.class);
    Mockito.doThrow(ioException).when(closeable2).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(closeable1);
    closeableGroup.addCloseable(closeable2);
    closeableGroup.addCloseable(closeable3);

    assertThatThrownBy(closeableGroup::close).isEqualTo(ioException);
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verifyNoInteractions(closeable3);
  }

  @Test
  public void wrapAutoCloseableFailuresWithRuntimeException() throws Exception {
    Exception generalException = new Exception("e");
    AutoCloseable throwingAutoCloseable = Mockito.mock(AutoCloseable.class);
    Mockito.doThrow(generalException).when(throwingAutoCloseable).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(throwingAutoCloseable);

    assertThatThrownBy(closeableGroup::close)
        .isInstanceOf(RuntimeException.class)
        .hasRootCause(generalException);
  }

  @Test
  public void notWrapRuntimeException() throws Exception {
    RuntimeException runtimeException = new RuntimeException("e2");

    Closeable throwingCloseable = Mockito.mock(Closeable.class);
    Mockito.doThrow(runtimeException).when(throwingCloseable).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(throwingCloseable);

    assertThatThrownBy(closeableGroup::close).isEqualTo(runtimeException);
  }

  @Test
  public void notWrapRuntimeExceptionFromAutoCloseable() throws Exception {
    RuntimeException runtimeException = new RuntimeException("e2");
    AutoCloseable throwingAutoCloseable = Mockito.mock(AutoCloseable.class);
    Mockito.doThrow(runtimeException).when(throwingAutoCloseable).close();

    CloseableGroup closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(throwingAutoCloseable);

    assertThatThrownBy(closeableGroup::close).isEqualTo(runtimeException);
  }
}

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

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

public class CloseGroupUtilTest {

  @Test
  public void callCloseToAllCloseables() throws Exception {
    AutoCloseable closeable1 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable3 = Mockito.mock(AutoCloseable.class);

    CloseGroupUtil.closeAutoCloseables(false, closeable1, closeable2, closeable3);
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void callCloseHandlesNull() throws Exception {
    AutoCloseable closeable1 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);

    CloseGroupUtil.closeAutoCloseables(false, closeable1, null, closeable2);
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
  }

  @Test
  public void suppressExceptionIfSetSuppressIsTrue() throws Exception {
    AutoCloseable closeable1 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable3 = Mockito.mock(AutoCloseable.class);
    Mockito.doThrow(new IOException("exception1")).when(closeable1).close();
    Mockito.doThrow(new RuntimeException("exception2")).when(closeable2).close();

    CloseGroupUtil.closeAutoCloseables(true, closeable1, closeable2, closeable3);

    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void notSuppressExceptionIfSetSuppressIsFalse() throws Exception {
    IOException ioException = new IOException("e1");

    AutoCloseable closeable1 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);
    AutoCloseable closeable3 = Mockito.mock(AutoCloseable.class);
    Mockito.doThrow(ioException).when(closeable2).close();

    Assertions.assertThatThrownBy(
        () -> CloseGroupUtil.closeAutoCloseables(false, closeable1, closeable2, closeable3))
        .isEqualTo(ioException);
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verifyNoInteractions(closeable3);
  }
}

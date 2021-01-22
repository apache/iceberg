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

package org.apache.iceberg.hive;

import org.junit.Test;

public class TestClientPool {

  @Test(expected = RuntimeException.class)
  public void testNewClientFailure() throws Exception {
    try (MockClientPool pool = new MockClientPool(2, Exception.class)) {
      pool.run(Object::toString);
    }
  }

  private static class MockClientPool extends ClientPool<Object, Exception> {

    MockClientPool(int poolSize, Class<? extends Exception> reconnectExc) {
      super(poolSize, reconnectExc);
    }

    @Override
    protected Object newClient() {
      throw new RuntimeException();
    }

    @Override
    protected Object reconnect(Object client) {
      return null;
    }

    @Override
    protected void close(Object client) {

    }
  }
}

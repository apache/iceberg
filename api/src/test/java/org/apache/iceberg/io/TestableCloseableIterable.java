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

public class TestableCloseableIterable implements CloseableIterable<Integer> {
  private Boolean closed = false;
  private final TestableCloseableIterator iterator = new TestableCloseableIterator();

  @Override
  public CloseableIterator<Integer> iterator() {
    return iterator;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }

  public Boolean closed() {
    return closed;
  }

  class TestableCloseableIterator implements CloseableIterator<Integer> {
    private Boolean closed = false;

    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Integer next() {
      return null;
    }

    public Boolean closed() {
      return closed;
    }
  }
}

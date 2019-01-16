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

package com.netflix.iceberg.io;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

public abstract class CloseableGroup implements Closeable {
  private final LinkedList<Closeable> closeables = Lists.newLinkedList();

  protected void addCloseable(Closeable closeable) {
    closeables.add(closeable);
  }

  @Override
  public void close() throws IOException {
    while (!closeables.isEmpty()) {
      Closeable toClose = closeables.removeFirst();
      if (toClose != null) {
        toClose.close();
      }
    }
  }

  static class ClosingIterable<T> extends CloseableGroup implements CloseableIterable<T> {
    private final Iterable<T> iterable;

    public ClosingIterable(Iterable<T> iterable, Iterable<Closeable> closeables) {
      this.iterable = iterable;
      if (iterable instanceof Closeable) {
        addCloseable((Closeable) iterable);
      }
      for (Closeable closeable : closeables) {
        addCloseable(closeable);
      }
    }

    @Override
    public Iterator<T> iterator() {
      return iterable.iterator();
    }
  }
}

/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.io;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;

public abstract class ClosingIterable implements Closeable {
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
}

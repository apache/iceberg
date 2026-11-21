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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.inmemory.InMemoryCatalog;

class CloseCountingCatalog extends InMemoryCatalog {
  private int closeCount = 0;

  CloseCountingCatalog() {
    initialize("close-counting", Map.of());
  }

  @Override
  public void close() throws IOException {
    closeCount += 1;
    super.close();
  }

  int closeCount() {
    return closeCount;
  }

  CatalogLoader loader() {
    return new CatalogLoader() {
      @Override
      public Catalog loadCatalog() {
        return CloseCountingCatalog.this;
      }

      @Override
      @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
      public CatalogLoader clone() {
        return this;
      }
    };
  }
}

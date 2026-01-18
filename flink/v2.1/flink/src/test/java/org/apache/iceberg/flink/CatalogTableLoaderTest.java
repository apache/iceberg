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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

public class CatalogTableLoaderTest {

  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "tbl");

  // Simple serializable CatalogLoader that creates a proxy Catalog on each loadCatalog() call.
  static class SerializableCatalogLoader
      implements org.apache.iceberg.flink.CatalogLoader, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Catalog loadCatalog() {
      // Create a simple Table proxy object
      Table tableProxy =
          (Table)
              Proxy.newProxyInstance(
                  Table.class.getClassLoader(),
                  new Class[] {Table.class},
                  new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) {
                      // For tests the Table object does not need to implement methods
                      if (method.getName().equals("toString")) {
                        return "fake-table";
                      }
                      return null;
                    }
                  });

      // Create a Catalog proxy that returns loadTable(identifier) and is Closeable
      InvocationHandler handler =
          new InvocationHandler() {
            private boolean closed = false;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
              String name = method.getName();
              if ("loadTable".equals(name)) {
                return tableProxy;
              } else if ("close".equals(name)) {
                closed = true;
                return null;
              } else if ("toString".equals(name)) {
                return "fake-catalog";
              }
              throw new UnsupportedOperationException("Not implemented in test proxy: " + name);
            }
          };

      return (Catalog)
          Proxy.newProxyInstance(
              Catalog.class.getClassLoader(),
              new Class[] {Catalog.class, Closeable.class},
              handler);
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public org.apache.iceberg.flink.CatalogLoader clone() {
      // A new instance is sufficient for tests
      return new SerializableCatalogLoader();
    }
  }

  private static <T extends Serializable> T roundTripSerialize(T obj) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    byte[] bytes = baos.toByteArray();
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      @SuppressWarnings("unchecked")
      T deserialized = (T) ois.readObject();
      return deserialized;
    }
  }

  @Test
  public void testOpenLoadClose() throws Exception {
    CatalogLoader catalogLoader = new SerializableCatalogLoader();
    TableLoader loader = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    // initially closed
    assertThat(loader.isOpen()).isFalse();

    // open and load
    loader.open();
    assertThat(loader.isOpen()).isTrue();
    Table table = loader.loadTable();
    assertThat(table).isNotNull();
    assertThat(table).hasToString("fake-table");

    // close
    loader.close();
    assertThat(loader.isOpen()).isFalse();
  }

  @Test
  public void testSerializationKeepsLoaderFunctional() throws Exception {
    org.apache.iceberg.flink.CatalogLoader catalogLoader = new SerializableCatalogLoader();
    TableLoader original = TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    // serialize / deserialize the TableLoader
    TableLoader deserialized = roundTripSerialize(original);

    // should still work after deserialization
    assertThat(deserialized.isOpen()).isFalse();
    deserialized.open();
    assertThat(deserialized.isOpen()).isTrue();
    Table table = deserialized.loadTable();
    assertThat(table).isNotNull();
    assertThat(table).hasToString("fake-table");
    deserialized.close();
    assertThat(deserialized.isOpen()).isFalse();
  }
}

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
import org.junit.Test;

import static org.junit.Assert.*;

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
    org.apache.iceberg.flink.CatalogLoader catalogLoader = new SerializableCatalogLoader();
    org.apache.iceberg.flink.TableLoader loader =
        org.apache.iceberg.flink.TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    // initially closed
    assertFalse(loader.isOpen());

    // open and load
    loader.open();
    assertTrue(loader.isOpen());
    Table t = loader.loadTable();
    assertNotNull(t);
    assertEquals("fake-table", t.toString());

    // close
    loader.close();
    assertFalse(loader.isOpen());
  }

  @Test
  public void testSerializationKeepsLoaderFunctional() throws Exception {
    org.apache.iceberg.flink.CatalogLoader catalogLoader = new SerializableCatalogLoader();
    org.apache.iceberg.flink.TableLoader original =
        org.apache.iceberg.flink.TableLoader.fromCatalog(catalogLoader, IDENTIFIER);

    // serialize / deserialize the TableLoader
    org.apache.iceberg.flink.TableLoader deserialized = roundTripSerialize(original);

    // should still work after deserialization
    assertFalse(deserialized.isOpen());
    deserialized.open();
    assertTrue(deserialized.isOpen());
    Table t = deserialized.loadTable();
    assertNotNull(t);
    assertEquals("fake-table", t.toString());
    deserialized.close();
    assertFalse(deserialized.isOpen());
  }
}

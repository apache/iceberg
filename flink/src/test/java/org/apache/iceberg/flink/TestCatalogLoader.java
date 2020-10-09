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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogLoader {

  @Test
  public void testHadoopJavaSerialization() throws IOException, ClassNotFoundException {
    CatalogLoader loader = CatalogLoader.hadoop("my_name", new Configuration(), "my_location");
    CatalogLoader copied = javaSerAndDeSer(loader);
    Assert.assertEquals("HadoopCatalogLoader{catalogName=my_name, warehouseLocation=my_location}", copied.toString());
  }

  @Test
  public void testHiveJavaSerialization() throws IOException, ClassNotFoundException {
    CatalogLoader loader = CatalogLoader.hive("my_name", new Configuration(), "my_uri", 2);
    CatalogLoader copied = javaSerAndDeSer(loader);
    Assert.assertEquals("HiveCatalogLoader{catalogName=my_name, uri=my_uri, clientPoolSize=2}", copied.toString());
  }

  @SuppressWarnings("unchecked")
  static <T> T javaSerAndDeSer(T object) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }

    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}

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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestInternalDataRegistration {
  @TempDir private Path temp;

  @Test
  void registersParquetWhenTheContextClassLoaderCannotSeeIceberg() throws Exception {
    // formats are registered once per loaded copy of InternalData, so this needs a fresh copy
    try (URLClassLoader isolated =
        new URLClassLoader(testClasspath(), ClassLoader.getPlatformClassLoader())) {
      AtomicReference<Object> writeBuilder = new AtomicReference<>();
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread thread =
          new Thread(
              () -> {
                try {
                  writeBuilder.set(writeParquet(isolated));
                } catch (InvocationTargetException e) {
                  failure.set(e.getCause());
                } catch (ReflectiveOperationException e) {
                  failure.set(e);
                }
              });
      thread.setContextClassLoader(null);
      thread.start();
      thread.join();

      assertThat(failure.get()).isNull();
      assertThat(writeBuilder.get()).isNotNull();
    }
  }

  private Object writeParquet(ClassLoader loader) throws ReflectiveOperationException {
    Class<?> internalData = Class.forName("org.apache.iceberg.InternalData", true, loader);
    assertThat(internalData).isNotSameAs(InternalData.class);

    Class<?> fileFormat = Class.forName("org.apache.iceberg.FileFormat", true, loader);
    Class<?> outputFile = Class.forName("org.apache.iceberg.io.OutputFile", true, loader);
    Object parquet = fileFormat.getField("PARQUET").get(null);
    Object file =
        Class.forName("org.apache.iceberg.Files", true, loader)
            .getMethod("localOutput", File.class)
            .invoke(null, temp.resolve("internal.parquet").toFile());

    return internalData.getMethod("write", fileFormat, outputFile).invoke(null, parquet, file);
  }

  private static URL[] testClasspath() throws Exception {
    List<URL> urls = Lists.newArrayList();
    for (String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      urls.add(new File(path).toURI().toURL());
    }

    return urls.toArray(new URL[0]);
  }
}

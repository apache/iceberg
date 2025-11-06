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
package org.apache.iceberg.encryption;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.junit.jupiter.api.Test;

public class TestEncryptionUtil {
  @Test
  public void testClassLoader()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Map<String, String> arg =
        Map.of(CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getName());

    CustomClassLoader customClassLoader = new CustomClassLoader();

    Class<?> aClass = customClassLoader.findClass(EncryptionUtil.class.getName());

    // Load the UnitestKMS into the customClassLoader instead of looking it up
    // in its parent class loader
    customClassLoader.findClass(UnitestKMS.class.getName());

    Object kmsClientObj = aClass.getDeclaredMethod("createKmsClient", Map.class).invoke(null, arg);

    assertThat(kmsClientObj.getClass().getClassLoader())
        .isNotInstanceOf(Thread.currentThread().getContextClassLoader().getClass());

    assertThat(kmsClientObj.getClass().getClassLoader().getName())
        .isEqualTo(customClassLoader.getName());
  }
}

class CustomClassLoader extends ClassLoader {

  @Override
  public String getName() {
    return "CustomClassLoaderForTest";
  }

  @Override
  public Class findClass(String name) {
    byte[] b = loadClassData(name);
    return defineClass(name, b, 0, b.length);
  }

  private byte[] loadClassData(String fileName) {
    InputStream inputStream =
        getClass()
            .getClassLoader()
            .getResourceAsStream(fileName.replace('.', File.separatorChar) + ".class");
    byte[] buffer;
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
      int nextValue;
      while ((nextValue = inputStream.read()) != -1) {
        byteStream.write(nextValue);
      }
      buffer = byteStream.toByteArray();
      return buffer;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

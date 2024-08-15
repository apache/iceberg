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
package org.apache.iceberg.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;

public class SerializationUtil {

  private SerializationUtil() {}

  /**
   * Serialize an object to bytes. If the object implements {@link HadoopConfigurable}, its Hadoop
   * configuration will be serialized into a {@link SerializableConfiguration}.
   *
   * @param obj object to serialize
   * @return serialized bytes
   */
  public static byte[] serializeToBytes(Object obj) {
    return serializeToBytes(obj, conf -> new SerializableConfiguration(conf)::get);
  }

  /**
   * Serialize an object to bytes. If the object implements {@link HadoopConfigurable}, the
   * confSerializer will be used to serialize Hadoop configuration used by the object.
   *
   * @param obj object to serialize
   * @param confSerializer serializer for the Hadoop configuration
   * @return serialized bytes
   */
  public static byte[] serializeToBytes(
      Object obj, Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    if (obj instanceof HadoopConfigurable) {
      ((HadoopConfigurable) obj).serializeConfWith(confSerializer);
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize object", e);
    }
  }

  @SuppressWarnings({"DangerousJavaDeserialization", "unchecked"})
  public static <T> T deserializeFromBytes(byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (T) ois.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize object", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not read object ", e);
    }
  }

  public static String serializeToBase64(Object obj) {
    byte[] bytes = serializeToBytes(obj);
    return new String(Base64.getMimeEncoder().encode(bytes), StandardCharsets.UTF_8);
  }

  public static <T> T deserializeFromBase64(String base64) {
    if (base64 == null) {
      return null;
    }
    byte[] bytes = Base64.getMimeDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
    return deserializeFromBytes(bytes);
  }
}

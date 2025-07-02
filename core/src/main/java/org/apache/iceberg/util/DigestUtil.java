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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public class DigestUtil {
  private static final byte[] MAGIC_DELIMITER =
      "0c1d0aaa-b115-42e0-bfa5-38495d6b3f5b".getBytes(StandardCharsets.UTF_8);

  private DigestUtil() {}

  /**
   * Computes a digest string for the input object array that can be used for equality check. For
   * example, given input A and input B are computed to digest strings D1 and D2 respectively, and
   * if we tested D1.equals(D2) == true, then A can be guaranteed equal to B, and vice versa.
   */
  public static String computeDigest(Object... objects) {
    final MessageDigest md5;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new UnsupportedOperationException("Failed to create MD5 instance", e);
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      for (Object object : objects) {
        writeObject(oos, object);
        writeObject(oos, MAGIC_DELIMITER);
      }
      oos.flush();
      final byte[] objectsMd5 = md5.digest(baos.toByteArray());
      return Base64.getEncoder().encodeToString(objectsMd5);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write objects: " + Arrays.toString(objects), e);
    }
  }

  private static void writeObject(ObjectOutputStream oos, Object object) throws IOException {
    if (object == null) {
      oos.writeObject(null);
    } else if (object instanceof Integer) {
      oos.writeInt((Integer) object);
    } else if (object instanceof Long) {
      oos.writeLong((Long) object);
    } else if (object instanceof Short) {
      oos.writeShort((Short) object);
    } else if (object instanceof Byte) {
      oos.writeByte((Byte) object);
    } else if (object instanceof Float) {
      oos.writeFloat((Float) object);
    } else if (object instanceof Double) {
      oos.writeDouble((Double) object);
    } else if (object instanceof Character) {
      oos.writeChar((Character) object);
    } else if (object instanceof Boolean) {
      oos.writeBoolean((Boolean) object);
    } else if (object instanceof String) {
      oos.writeUTF((String) object);
    } else if (object instanceof byte[]) {
      oos.write((byte[]) object);
    } else {
      // We don't write objects that are of unknown types to the stream
      // for digest computation. Because serialization process can be
      // customizable so doesn't essentially represent the identity of
      // that object.
      throw new UnsupportedOperationException(
          "Digest computation not supported for object type: " + object.getClass());
    }
  }
}

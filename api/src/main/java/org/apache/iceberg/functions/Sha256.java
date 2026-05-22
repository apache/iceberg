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
package org.apache.iceberg.functions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Type;

/** Shared SHA-256 masking function. One instance per (type, salt) pair. */
final class Sha256 extends IcebergFunctions.NullSafeFunction<Object, Object> {

  private static final ThreadLocal<MessageDigest> DIGEST =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new IllegalStateException("SHA-256 not available", e);
            }
          });

  enum Codec {
    STRING {
      @Override
      void update(MessageDigest md, Object value) {
        md.update(((String) value).getBytes(StandardCharsets.UTF_8));
      }

      @Override
      Object encode(byte[] digest) {
        return BaseEncoding.base16().lowerCase().encode(digest);
      }
    },
    INTEGER {
      @Override
      void update(MessageDigest md, Object value) {
        int intVal = (Integer) value;
        md.update((byte) intVal);
        md.update((byte) (intVal >>> 8));
        md.update((byte) (intVal >>> 16));
        md.update((byte) (intVal >>> 24));
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      }
    },
    LONG {
      @Override
      void update(MessageDigest md, Object value) {
        long longVal = (Long) value;
        for (int i = 0; i < 8; i++) {
          md.update((byte) longVal);
          longVal >>>= 8;
        }
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
      }
    },
    BINARY {
      @Override
      void update(MessageDigest md, Object value) {
        md.update(((ByteBuffer) value).duplicate());
      }

      @Override
      Object encode(byte[] digest) {
        return ByteBuffer.wrap(digest);
      }
    };

    abstract void update(MessageDigest md, Object value);

    abstract Object encode(byte[] digest);
  }

  static boolean isSupported(Type type) {
    switch (type.typeId()) {
      case STRING:
      case INTEGER:
      case LONG:
      case BINARY:
        return true;
      default:
        return false;
    }
  }

  static Sha256 forType(Type type, byte[] salt) {
    switch (type.typeId()) {
      case STRING:
        return new Sha256(Codec.STRING, salt);
      case INTEGER:
        return new Sha256(Codec.INTEGER, salt);
      case LONG:
        return new Sha256(Codec.LONG, salt);
      case BINARY:
        return new Sha256(Codec.BINARY, salt);
      default:
        throw new IllegalArgumentException("sha-256 is not supported for type: " + type);
    }
  }

  private final Codec codec;
  private final byte[] salt;

  private Sha256(Codec codec, byte[] salt) {
    this.codec = codec;
    this.salt = salt != null ? salt.clone() : null;
  }

  @Override
  protected Object applyNonNull(Object value) {
    MessageDigest md = DIGEST.get();
    md.reset();
    if (salt != null) {
      md.update(salt);
    }
    codec.update(md, value);
    return codec.encode(md.digest());
  }
}

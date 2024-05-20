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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBStructLikeSet extends StructLikeSet {

  private final RocksDB db;
  private int size = 0;
  private final StructLikeSerializer serializer;

  private static volatile RocksDBStructLikeSet instance;
  private static volatile boolean loaded = false;

  private RocksDBStructLikeSet(Types.StructType type) {
    super(type);
    try {
      Options options = new Options().setCreateIfMissing(true);
      this.db = RocksDB.open(options, mkDir());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.serializer = new StructLikeSerializer(type);
  }

  static {
    RocksDB.loadLibrary();
  }

  public static StructLikeSet getOrCreate(
      Types.StructType type, Iterable<Iterable<StructLike>> deletes) {
    if (!loaded) {
      synchronized (RocksDBStructLikeSet.class) {
        if (!loaded) {
          instance = new RocksDBStructLikeSet(type);
          Iterables.addAll(instance, Iterables.concat(deletes));
          loaded = true;
        }
      }
    }
    return instance;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean contains(Object obj) {
    if (obj instanceof StructLike || obj == null) {
      byte[] key = serializer.serialize((StructLike) obj);
      try {
        return db.get(key) != null;
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public Iterator<StructLike> iterator() {
    throw new UnsupportedOperationException("iterator is not supported");
  }

  @Override
  public boolean add(StructLike struct) {
    byte[] key = serializer.serialize(struct);
    try {
      db.put(key, new byte[0]);
      size += 1;
      return true;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean remove(Object obj) {
    throw new UnsupportedOperationException("remove is not supported");
  }

  private static class StructLikeSerializer {

    private static final int NULL_MARK = -1;
    private final Types.StructType struct;
    private final List<Type> types;

    private StructLikeSerializer(Types.StructType struct) {
      this.struct = struct;
      this.types =
          struct.fields().stream().map(Types.NestedField::type).collect(Collectors.toList());
    }

    public byte[] serialize(StructLike object) {
      if (object == null) {
        return null;
      }

      try (ByteArrayOutputStream out = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(out)) {

        for (int i = 0; i < types.size(); i += 1) {
          Type type = types.get(i);
          Class<?> valueClass = type.typeId().javaClass();

          byte[] fieldData =
              ByteBuffers.toByteArray(Conversions.toByteBuffer(type, object.get(i, valueClass)));
          if (fieldData == null) {
            dos.writeInt(NULL_MARK);
          } else {
            dos.writeInt(fieldData.length);
            dos.write(fieldData);
          }
        }
        return out.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public StructLike deserialize(byte[] data) {
      if (data == null) {
        return null;
      }

      try (ByteArrayInputStream in = new ByteArrayInputStream(data);
          DataInputStream dis = new DataInputStream(in)) {

        GenericRecord record = GenericRecord.create(struct);
        for (int i = 0; i < types.size(); i += 1) {
          int length = dis.readInt();

          if (length == NULL_MARK) {
            record.set(i, null);
          } else {
            byte[] fieldData = new byte[length];
            int fieldDataSize = dis.read(fieldData);
            Preconditions.checkState(length == fieldDataSize, "%s != %s", length, fieldDataSize);
            record.set(i, Conversions.fromByteBuffer(types.get(i), ByteBuffer.wrap(fieldData)));
          }
        }

        return record;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private String mkDir() throws IOException {
    Path path = Files.createTempDirectory("iceberg-rocksdb");
    return path.toString();
  }
}

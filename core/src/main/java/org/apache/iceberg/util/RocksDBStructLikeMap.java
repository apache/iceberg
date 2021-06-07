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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Serializer;
import org.apache.iceberg.types.Serializers;
import org.apache.iceberg.types.Types;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

public class RocksDBStructLikeMap<T> extends AbstractMap<StructLike, T> implements Map<StructLike, T> {

  static {
    RocksDB.loadLibrary();
  }

  public static <T> RocksDBStructLikeMap<T> create(String path,
                                                   Types.StructType keyType,
                                                   Types.StructType valType) {
    return new RocksDBStructLikeMap<>(path, keyType, valType);
  }

  private final String path;
  private final WriteOptions writeOptions;
  private final RocksDB db;

  private final Serializer<StructLike> keySerializer;
  private final Serializer<StructLike> valSerializer;

  // It's expensive to get the RocksDB's data size, so we maintain the size when put/delete rows.
  private int size = 0;

  private RocksDBStructLikeMap(String path, Types.StructType keyType, Types.StructType valType) {
    this.path = path;
    this.writeOptions = new WriteOptions().setDisableWAL(true);
    try {
      Options options = new Options().setCreateIfMissing(true);
      this.db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
    this.keySerializer = Serializers.forType(keyType);
    this.valSerializer = Serializers.forType(valType);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size <= 0;
  }

  @Override
  public boolean containsKey(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        return db.get(keyData) != null;
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    if (value instanceof StructLike) {
      byte[] valData = valSerializer.serialize((StructLike) value);
      try (RocksIterator iter = db.newIterator()) {
        for (iter.next(); iter.isValid(); iter.next()) {
          if (Arrays.equals(valData, iter.value())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        byte[] valData = db.get(keyData);
        if (valData == null) {
          return null;
        }

        return (T) valSerializer.deserialize(valData);
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T put(StructLike key, T value) {
    if (value instanceof StructLike) {
      byte[] keyData = keySerializer.serialize(key);
      byte[] newValue = valSerializer.serialize((StructLike) value);
      try {
        byte[] oldValue = db.get(keyData);
        db.put(writeOptions, keyData, newValue);

        if (oldValue == null) {
          // Add a new row into the map.
          size += 1;
          return null;
        } else {
          // Replace the old row with the new row.
          return (T) valSerializer.deserialize(oldValue);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IllegalArgumentException("Value isn't the expected StructLike: " + value);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T remove(Object key) {
    if (key instanceof StructLike) {
      byte[] keyData = keySerializer.serialize((StructLike) key);
      try {
        byte[] valData = db.get(keyData);
        if (valData != null) {
          db.delete(writeOptions, keyData);

          size -= 1;
          return (T) valSerializer.deserialize(valData);
        }
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  @Override
  public void clear() {
    size = 0;
    db.close();
    try {
      FileUtils.cleanDirectory(new File(path));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Set<StructLike> keySet() {
    throw new UnsupportedOperationException("Unsupported keySet() in RocksDBStructLikeMap.");
  }

  @Override
  public Collection<T> values() {
    throw new UnsupportedOperationException("Unsupported values() in RocksDBStructLikeMap.");
  }

  @Override
  public Set<Entry<StructLike, T>> entrySet() {
    throw new UnsupportedOperationException("Unsupported entrySet() in RocksDBStructLikeMap");
  }
}

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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.ByteBufferInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Base class for information_schema tables.
 *
 * <p>Serializing and deserializing a metadata table object returns a read only implementation of
 * the metadata table using a {@link StaticTableOperations}. This way no Catalog related calls are
 * needed when reading the table data after deserialization.
 */
abstract class InformationSchemaTable extends BaseReadOnlyTable implements Serializable {
  private static final String INFORMATION_SCHEMA = "information_schema";

  enum Type {
    NAMESPACES("namespaces"),
    TABLES("tables");

    public static Type from(String name) {
      try {
        return Type.valueOf(name.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException ignored) {
        return null;
      }
    }

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String typeName() {
      return name;
    }
  }

  private final Catalog catalog;
  private final Type type;

  protected InformationSchemaTable(Catalog catalog, Type type) {
    super(INFORMATION_SCHEMA);
    this.catalog = catalog;
    this.type = type;
  }

  String typeName() {
    return type.typeName();
  }

  @Override
  public String name() {
    return catalog.name() + "." + INFORMATION_SCHEMA + "." + typeName();
  }

  protected Catalog catalog() {
    return catalog;
  }

  @Override
  public TableScan newScan() {
    throw new UnsupportedOperationException(
        "newBatchScan() must be used for information_schema tables");
  }

  @Override
  public FileIO io() {
    return null;
  }

  @Override
  public String location() {
    return typeName();
  }

  @Override
  public EncryptionManager encryption() {
    return new PlaintextEncryptionManager();
  }

  @Override
  public LocationProvider locationProvider() {
    return null;
  }

  @Override
  public void refresh() {}

  @Override
  public Map<Integer, Schema> schemas() {
    return ImmutableMap.of(TableMetadata.INITIAL_SCHEMA_ID, schema());
  }

  @Override
  public PartitionSpec spec() {
    return PartitionSpec.unpartitioned();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return ImmutableMap.of(0, PartitionSpec.unpartitioned());
  }

  @Override
  public SortOrder sortOrder() {
    return SortOrder.unsorted();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return ImmutableMap.of(0, SortOrder.unsorted());
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public Snapshot currentSnapshot() {
    return null;
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return ImmutableList.of();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return null;
  }

  @Override
  public List<HistoryEntry> history() {
    return null;
  }

  @Override
  public List<StatisticsFile> statisticsFiles() {
    return ImmutableList.of();
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return ImmutableMap.of();
  }

  @Override
  public String toString() {
    return name();
  }

  final Object writeReplace() {
    return SerializableTable.copyOf(this);
  }

  /** An InputFile instance for static tasks. */
  static class EmptyInputFile implements InputFile {
    private final String location;

    EmptyInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public SeekableInputStream newStream() {
      return ByteBufferInputStream.wrap(ByteBuffer.allocate(0));
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }
}

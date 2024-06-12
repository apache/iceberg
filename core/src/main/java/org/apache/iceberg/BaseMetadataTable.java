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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transforms;

/**
 * Base class for metadata tables.
 *
 * <p>Serializing and deserializing a metadata table object returns a read only implementation of
 * the metadata table using a {@link StaticTableOperations}. This way no Catalog related calls are
 * needed when reading the table data after deserialization.
 */
public abstract class BaseMetadataTable extends BaseReadOnlyTable implements Serializable {
  private final PartitionSpec spec = PartitionSpec.unpartitioned();
  private final SortOrder sortOrder = SortOrder.unsorted();
  private final BaseTable table;
  private final String name;
  private final UUID uuid;

  protected BaseMetadataTable(Table table, String name) {
    super("metadata");
    Preconditions.checkArgument(
        table instanceof BaseTable, "Cannot create metadata table for non-data table: %s", table);
    this.table = (BaseTable) table;
    this.name = name;
    this.uuid = UUID.randomUUID();
  }

  /**
   * This method transforms the table's partition spec to a spec that is used to rewrite the
   * user-provided filter expression against the given metadata table.
   *
   * <p>The resulting partition spec maps partition.X fields to partition X using an identity
   * partition transform. When this spec is used to project an expression for the given metadata
   * table, the projection will remove predicates for non-partition fields (not in the spec) and
   * will remove the "partition." prefix from fields.
   *
   * @param metadataTableSchema schema of the metadata table
   * @param spec spec on which the metadata table schema is based
   * @return a spec used to rewrite the metadata table filters to partition filters using an
   *     inclusive projection
   */
  static PartitionSpec transformSpec(Schema metadataTableSchema, PartitionSpec spec) {
    PartitionSpec.Builder builder =
        PartitionSpec.builderFor(metadataTableSchema)
            .withSpecId(spec.specId())
            .checkConflicts(false);

    Map<Integer, Integer> reassignedFields = metadataTableSchema.idsToReassigned();

    for (PartitionField field : spec.fields()) {
      int newFieldId = reassignedFields.getOrDefault(field.fieldId(), field.fieldId());
      builder.add(newFieldId, newFieldId, field.name(), Transforms.identity());
    }
    return builder.build();
  }

  /**
   * This method transforms the given partition specs to specs that are used to rewrite the
   * user-provided filter expression against the given metadata table.
   *
   * <p>See: {@link #transformSpec(Schema, PartitionSpec)}
   *
   * @param metadataTableSchema schema of the metadata table
   * @param specs specs on which the metadata table schema is based
   * @return specs used to rewrite the metadata table filters to partition filters using an
   *     inclusive projection
   */
  static Map<Integer, PartitionSpec> transformSpecs(
      Schema metadataTableSchema, Map<Integer, PartitionSpec> specs) {
    return specs.values().stream()
        .map(spec -> transformSpec(metadataTableSchema, spec))
        .collect(Collectors.toMap(PartitionSpec::specId, spec -> spec));
  }

  abstract MetadataTableType metadataTableType();

  public BaseTable table() {
    return table;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public FileIO io() {
    return table().io();
  }

  @Override
  public String location() {
    return table().location();
  }

  @Override
  public EncryptionManager encryption() {
    return table().encryption();
  }

  @Override
  public LocationProvider locationProvider() {
    return table().locationProvider();
  }

  @Override
  public void refresh() {
    table().refresh();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    return ImmutableMap.of(TableMetadata.INITIAL_SCHEMA_ID, schema());
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return ImmutableMap.of(spec.specId(), spec);
  }

  @Override
  public SortOrder sortOrder() {
    return sortOrder;
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return ImmutableMap.of(sortOrder.orderId(), sortOrder);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  public Snapshot currentSnapshot() {
    return table().currentSnapshot();
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return table().snapshots();
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    return table().snapshot(snapshotId);
  }

  @Override
  public List<HistoryEntry> history() {
    return table().history();
  }

  @Override
  public List<StatisticsFile> statisticsFiles() {
    return ImmutableList.of();
  }

  @Override
  public List<PartitionStatisticsFile> partitionStatisticsFiles() {
    return ImmutableList.of();
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return table().refs();
  }

  @Override
  public UUID uuid() {
    return uuid;
  }

  @Override
  public String toString() {
    return name();
  }

  final Object writeReplace() {
    return SerializableTable.copyOf(this);
  }
}

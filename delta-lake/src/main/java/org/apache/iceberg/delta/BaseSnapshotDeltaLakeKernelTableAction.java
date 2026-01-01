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
package org.apache.iceberg.delta;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseSnapshotDeltaLakeKernelTableAction implements SnapshotDeltaLakeTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotDeltaLakeKernelTableAction.class);

  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";

  private final ImmutableMap.Builder<String, String> icebergPropertiesBuilder =
      ImmutableMap.builder();

  private final String deltaTableLocation;
  private Engine deltaEngine;
  private Table deltaTable;

  private Catalog icebergCatalog;
  private TableIdentifier newTableIdentifier;
  private String newTableLocation;
  private HadoopFileIO deltaLakeFileIO;

  public BaseSnapshotDeltaLakeKernelTableAction(String deltaTableLocation) {
    this.deltaTableLocation = deltaTableLocation;
    this.newTableLocation = deltaTableLocation;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperties(Map<String, String> properties) {
    icebergPropertiesBuilder.putAll(properties);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperty(String name, String value) {
    icebergPropertiesBuilder.put(name, value);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableLocation(String location) {
    newTableLocation = location;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable as(TableIdentifier identifier) {
    newTableIdentifier = identifier;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable icebergCatalog(Catalog catalog) {
    icebergCatalog = catalog;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable deltaLakeConfiguration(Configuration conf) {
    deltaEngine = DefaultEngine.create(conf);
    deltaLakeFileIO = new HadoopFileIO(conf);
    deltaTable = Table.forPath(deltaEngine, deltaTableLocation);
    return this;
  }

  @Override
  public Result execute() {
    Preconditions.checkArgument(
        icebergCatalog != null && newTableIdentifier != null,
        "Iceberg catalog and identifier cannot be null. Make sure to configure the action with a valid Iceberg catalog and identifier.");
    Preconditions.checkArgument(
        deltaTable != null && deltaLakeFileIO != null,
        "Make sure to configure the action with a valid deltaLakeConfiguration");

    // TODO get initial snapshot
    SnapshotImpl deltaSnapshot = getLatestDeltaSnapshot();
    Schema icebergSchema = convertToIcebergSchema(deltaSnapshot.getSchema());
    PartitionSpec partitionSpec =
        buildPartitionSpec(icebergSchema, deltaSnapshot.getPartitionColumnNames());

    // TODO
    // 2. Delta log to Iceberg history
    // 3. Delta versions and Delta tags
    Transaction transaction =
        icebergCatalog.newCreateTableTransaction(
            newTableIdentifier,
            icebergSchema,
            partitionSpec,
            newTableLocation,
            buildTablePropertiesWithDelta(deltaSnapshot, deltaTableLocation, icebergSchema));

    transaction.commitTransaction();
    long totalDataFiles = 0;
    LOG.info(
        "Successfully created Iceberg table {} from Delta Lake table at {}, total data file count: {}",
        newTableIdentifier,
        deltaTableLocation,
        totalDataFiles);
    return ImmutableSnapshotDeltaLakeTable.Result.builder()
        .snapshotDataFilesCount(totalDataFiles)
        .build();
  }

  @Nonnull
  private static Schema convertToIcebergSchema(StructType deltaSchema) {
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType();
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private SnapshotImpl getLatestDeltaSnapshot() {
    Snapshot latestSnapshot;
    try {
      latestSnapshot = deltaTable.getLatestSnapshot(deltaEngine);

      if (!(latestSnapshot instanceof SnapshotImpl)) {
        throw new IllegalStateException(
            "Unsupported impl of delta Snapshot: " + latestSnapshot.getClass());
      }

      return (SnapshotImpl) latestSnapshot;
    } catch (TableNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Delta Lake table does not exist at the given location: %s", deltaTableLocation));
    }
  }

  private PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionNames) {
    if (partitionNames.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (String partitionName : partitionNames) {
      builder.identity(partitionName);
    }
    return builder.build();
  }

  private Map<String, String> buildTablePropertiesWithDelta(
      SnapshotImpl deltaSnapshot, String originalLocation, Schema icebergSchema) {
    icebergPropertiesBuilder.putAll(
        org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
            SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE, ORIGINAL_LOCATION_PROP, originalLocation));
    icebergPropertiesBuilder.put(
        TableProperties.DEFAULT_NAME_MAPPING,
        NameMappingParser.toJson(MappingUtil.create(icebergSchema)));

    Map<String, String> configuration = deltaSnapshot.getMetadata().getConfiguration();
    icebergPropertiesBuilder.putAll(configuration);
    if (!configuration.isEmpty()) {
      System.out.println(configuration);
    }

    return icebergPropertiesBuilder.build();
  }
}

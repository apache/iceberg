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
package org.apache.iceberg.hudi;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseSnapshotHudiTableAction implements SnapshotHudiTable {

  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotHudiTableAction.class.getName());
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String HOODIE_SOURCE_VALUE = "hudi";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String PARQUET_SUFFIX = ".parquet";
  private static final String AVRO_SUFFIX = ".avro";
  private static final String ORC_SUFFIX = ".orc";
  private HoodieTableMetaClient hoodieTableMetaClient;
  private HoodieTableConfig hoodieTableConfig;
  private HoodieEngineContext hoodieEngineContext;
  private HoodieMetadataConfig hoodieMetadataConfig;
  private String hoodieTableBasePath;
  private Catalog icebergCatalog;
  private TableIdentifier newTableIdentifier;
  private HadoopFileIO hoodieFileIO;
  private ImmutableMap.Builder<String, String> additionalPropertiesBuilder = ImmutableMap.builder();

  public BaseSnapshotHudiTableAction(
      Configuration hoodieConfiguration,
      String hoodieTableBasePath,
      Catalog icebergCatalog,
      TableIdentifier newTableIdentifier) {
    this.hoodieTableMetaClient = buildTableMetaClient(hoodieConfiguration, hoodieTableBasePath);
    this.hoodieTableConfig = hoodieTableMetaClient.getTableConfig();
    this.hoodieEngineContext = new HoodieLocalEngineContext(hoodieConfiguration);
    this.hoodieTableBasePath = hoodieTableBasePath;
    this.hoodieMetadataConfig = HoodieInputFormatUtils.buildMetadataConfig(hoodieConfiguration);
    this.hoodieFileIO = new HadoopFileIO(hoodieConfiguration);
    this.icebergCatalog = icebergCatalog;
    this.newTableIdentifier = newTableIdentifier;
  }

  @Override
  public SnapshotHudiTable tableProperties(Map<String, String> properties) {
    return null;
  }

  @Override
  public SnapshotHudiTable tableProperty(String key, String value) {
    return null;
  }

  @Override
  public Result execute() {
    LOG.info("Alpha test: hoodie table base path: {}", hoodieTableMetaClient.getBasePathV2());
    LOG.info(
        "Alpha test: hoodie getBootStrapIndexByFileId: {}",
        hoodieTableMetaClient.getBootstrapIndexByFileIdFolderNameFolderPath());
    LOG.info(
        "Alpha test: hoodie getBootStrapIndexByPartitionPath: {}",
        hoodieTableMetaClient.getBootstrapIndexByPartitionFolderPath());

    // Convert Hoodie table schema to Iceberg schema and extract the partition spec
    InternalSchema hudiSchema = getHudiSchema();
    LOG.info("Alpha test: hoodie table schema: {}", hudiSchema);
    LOG.info("Alpha test: get record type: {}", hudiSchema.getRecord());
    Schema icebergSchema = convertToIcebergSchema(hudiSchema);
    LOG.info("Alpha test: get converted schema: {}", icebergSchema);
    PartitionSpec partitionSpec = getPartitionSpecFromHoodieMetadataData(icebergSchema);
    LOG.info("Alpha test: get partition spec: {}", partitionSpec);

    // TODO: add support for newTableLocation
    Transaction icebergTransaction =
        icebergCatalog.newCreateTableTransaction(
            newTableIdentifier, icebergSchema, partitionSpec, destTableProperties());
    // We need name mapping to ensure we can read data files correctly as iceberg table has its own
    // rule to assign field id
    // Although the field id rule seems to be the same as hudi, but the rule is not guaranteed by
    // any API
    icebergTransaction
        .table()
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(icebergTransaction.table().schema())))
        .commit();

    // Pre-process the timeline, we only need to process all COMPLETED commit for COW table
    // Commit that has been rollbacked will not be in either REQUESTED or INFLIGHT state
    HoodieTimeline timeline =
        hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    LOG.info("Alpha test: hoodie timeline: {}", timeline);
    // Initialize the FileSystemView for querying table data files
    // TODO: need to choose the correct implementation of the FileSystemView
    HoodieTableFileSystemView hoodieTableFileSystemView =
        FileSystemViewManager.createInMemoryFileSystemViewWithTimeline(
            hoodieEngineContext, hoodieTableMetaClient, hoodieMetadataConfig, timeline);
    // get all instants on the timeline
    Stream<HoodieInstant> completedInstants = timeline.getInstants();
    LOG.info("Alpha test: get completed instants: {}", completedInstants);
    // file group id -> Map<timestamp, HoodieBaseFile>
    // This pre-process aims to make a timestamp to HoodieBaseFile map for each file group
    Map<HoodieFileGroupId, Map<String, HoodieBaseFile>> allStampedDataFiles =
        hoodieTableFileSystemView
            .getAllFileGroups()
            .collect(
                ImmutableMap.toImmutableMap(
                    HoodieFileGroup::getFileGroupId,
                    fileGroup ->
                        fileGroup
                            .getAllBaseFiles()
                            .collect(
                                ImmutableMap.toImmutableMap(
                                    HoodieBaseFile::getCommitTime, baseFile -> baseFile))));
    // BEGIN TEST ONLY CODE
    List<HoodieFileGroup> testGroups =
        hoodieTableFileSystemView.getAllFileGroups().collect(Collectors.toList());
    LOG.info("Alpha test: get all stamped data files: {}", allStampedDataFiles);
    LOG.info("Alpha test: get all file groups: {}", testGroups);
    // END TEST ONLY CODE

    // Help tracked if a previous version of the data file has been added to the iceberg table
    Map<HoodieFileGroupId, DataFile> convertedDataFiles = Maps.newHashMap();
    // Replay the timeline from beginning to the end
    completedInstants.forEachOrdered(
        instant -> {
          LOG.info("Alpha test: get completed instant: {}", instant);
          // copyInstants to iceberg table
          // TODO: need to verify the order of the instants, make sure it is from the oldest to the
          // newest

          // commit each instant as a transaction to the iceberg table
          commitHoodieInstantToIcebergTransaction(
              instant,
              hoodieTableFileSystemView.getAllFileGroups(),
              allStampedDataFiles,
              convertedDataFiles,
              icebergTransaction);
        });
    Snapshot icebergSnapshot = icebergTransaction.table().currentSnapshot();
    long totalDataFiles =
        icebergSnapshot != null
            ? Long.parseLong(icebergSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP))
            : 0;
    icebergTransaction.commitTransaction();
    LOG.info(
        "Successfully created Iceberg table {} from hudi table at {}, total data file count: {}",
        newTableIdentifier,
        hoodieTableBasePath,
        totalDataFiles);
    return new BaseSnapshotHudiTableActionResult(totalDataFiles);
  }

  /**
   * In COW Hoodie table, each file group is a combination of different versions of the same data
   * file.
   *
   * <p>During each write, a new version of the file will be copied and modified to be a new version
   * in the file group. Therefore, when committing the datafile to the iceberg table, we need to
   * make sure that the older version of the data file is deleted before adding the newer version of
   * the data file.
   *
   * <p>In other words, the COW behavior can be mapped to the overwrite operation in the iceberg.
   */
  public void commitHoodieInstantToIcebergTransaction(
      HoodieInstant instant,
      Stream<HoodieFileGroup> fileGroups,
      Map<HoodieFileGroupId, Map<String, HoodieBaseFile>> allStampedDataFiles,
      Map<HoodieFileGroupId, DataFile> convertedDataFiles,
      Transaction transaction) {
    List<DataFile> filesToAdd = Lists.newArrayList();
    List<DataFile> filesToRemove = Lists.newArrayList();

    // TODO: may need to add synchronization lock for parallelism
    fileGroups
        .sequential()
        .forEach(
            fileGroup -> {
              HoodieFileGroupId fileGroupId = fileGroup.getFileGroupId();
              LOG.info("Alpha test: get file group: {}", fileGroup);
              DataFile currentDataFile =
                  buildDataFileFromHoodieBaseFile(
                      instant,
                      fileGroup,
                      allStampedDataFiles.get(fileGroupId),
                      transaction.table());

              if (currentDataFile != null) {
                filesToAdd.add(currentDataFile);

                DataFile previousDataFile = convertedDataFiles.get(fileGroupId);
                if (previousDataFile != null) {
                  // need to delete the previous data file since a new version will be added
                  filesToRemove.add(previousDataFile);
                }

                // update the converted data file map
                convertedDataFiles.put(fileGroupId, currentDataFile);
              }
            });
    LOG.info("Alpha test: get files to add: {} at instant {}", filesToAdd, instant);
    if (filesToAdd.size() > 0 && filesToRemove.size() > 0) {
      // OverwriteFiles case
      OverwriteFiles overwriteFiles = transaction.newOverwrite();
      filesToAdd.forEach(overwriteFiles::addFile);
      filesToRemove.forEach(overwriteFiles::deleteFile);
      overwriteFiles.commit();
    } else if (filesToAdd.size() > 0) {
      // AppendFiles case
      AppendFiles appendFiles = transaction.newAppend();
      filesToAdd.forEach(appendFiles::appendFile);
      appendFiles.commit();
    } else if (filesToRemove.size() > 0) {
      // DeleteFiles case
      DeleteFiles deleteFiles = transaction.newDelete();
      filesToRemove.forEach(deleteFiles::deleteFile);
      deleteFiles.commit();
    }
  }

  @Nullable
  private DataFile buildDataFileFromHoodieBaseFile(
      HoodieInstant instant,
      HoodieFileGroup fileGroup,
      Map<String, HoodieBaseFile> stampedDataFiles,
      Table table) {
    HoodieBaseFile baseFile = stampedDataFiles.get(instant.getTimestamp());
    if (baseFile == null) {
      LOG.info(
          "Alpha test: does not have base file for instant: {}, fileGroupId {}",
          instant,
          fileGroup.getFileGroupId());
      return null;
    }

    PartitionSpec spec = table.spec();
    // TODO: need to verify the path is absolute (the field's name is fullPath)
    String path = baseFile.getPath();
    long fileSize = baseFile.getFileSize();
    String partitionPath = fileGroup.getPartitionPath();

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;

    InputFile file = hoodieFileIO.newInputFile(path);
    FileFormat format = determineFileFormatFromPath(path);
    Metrics metrics = getMetricsForFile(file, format, metricsConfig, nameMapping);

    return DataFiles.builder(spec)
        .withPath(path)
        .withFormat(format)
        .withFileSizeInBytes(fileSize)
        .withPartitionPath(partitionPath) // TODO: need to verify the partition path is correct
        .withMetrics(metrics)
        .build();
  }

  /**
   * Taken from <a
   * href="https://github.com/apache/hudi/blob/a70355f44571036d7f99b3ca3cb240674bd1cf91/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java#L1405-L1414">getInternalSchema</a>
   * in HoodieWriteClient.
   */
  private InternalSchema getHudiSchema() {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(hoodieTableMetaClient);
    Option<InternalSchema> hudiSchema = schemaUtil.getTableInternalSchemaFromCommitMetadata();
    LOG.info("Alpha test: hoodie schema: {}", hudiSchema);
    LOG.info("Alpha test: active timeline: {}", hoodieTableMetaClient.getActiveTimeline());
    LOG.info(
        "Alpha test: active timeline commit timeline: {}",
        hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline());
    LOG.info(
        "Alpha test: active timeline commit timeline instants: {}",
        hoodieTableMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    return hudiSchema.orElseGet(
        () -> {
          try {
            return AvroInternalSchemaConverter.convert(schemaUtil.getTableAvroSchema());
          } catch (Exception e) {
            throw new HoodieException("cannot find schema for current table");
          }
        });
  }

  /**
   * Use nested type visitor to convert the internal schema to iceberg schema.
   *
   * <p>just like what we did with spark table's schema and delta lake table's schema.
   */
  private Schema convertToIcebergSchema(InternalSchema hudiSchema) {
    Type converted =
        HudiDataTypeVisitor.visit(
            hudiSchema.getRecord(), new HudiDataTypeToType(hudiSchema.getRecord()));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private PartitionSpec getPartitionSpecFromHoodieMetadataData(Schema schema) {
    Option<String[]> partitionNames = hoodieTableConfig.getPartitionFields();
    if (partitionNames.isPresent()) {
      PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
      for (String partitionName : partitionNames.get()) {
        builder.identity(partitionName);
      }
      return builder.build();
    }

    return PartitionSpec.unpartitioned();
  }

  private Map<String, String> destTableProperties() {
    // TODO: need to check which hoodie properties to add to
    additionalPropertiesBuilder.putAll(hoodieTableConfig.propsMap());
    additionalPropertiesBuilder.putAll(
        ImmutableMap.of(
            SNAPSHOT_SOURCE_PROP,
            HOODIE_SOURCE_VALUE,
            ORIGINAL_LOCATION_PROP,
            hoodieTableMetaClient.getBasePathV2().toString()));

    return additionalPropertiesBuilder.build();
  }

  private static HoodieTableMetaClient buildTableMetaClient(Configuration conf, String basePath) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(conf)
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    return metaClient;
  }

  private FileFormat determineFileFormatFromPath(String path) {
    if (path.endsWith(PARQUET_SUFFIX)) {
      return FileFormat.PARQUET;
    } else if (path.endsWith(AVRO_SUFFIX)) {
      return FileFormat.AVRO;
    } else if (path.endsWith(ORC_SUFFIX)) {
      return FileFormat.ORC;
    } else {
      throw new ValidationException("Cannot determine file format from path %s", path);
    }
  }

  private Metrics getMetricsForFile(
      InputFile file, FileFormat format, MetricsConfig metricsSpec, NameMapping mapping) {
    switch (format) {
      case AVRO:
        long rowCount = Avro.rowCount(file);
        return new Metrics(rowCount, null, null, null, null);
      case PARQUET:
        return ParquetUtil.fileMetrics(file, metricsSpec, mapping);
      case ORC:
        return OrcMetrics.fromInputFile(file, metricsSpec, mapping);
      default:
        throw new ValidationException("Cannot get metrics from file format: %s", format);
    }
  }
}

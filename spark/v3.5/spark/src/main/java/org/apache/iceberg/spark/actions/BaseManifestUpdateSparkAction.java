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
package org.apache.iceberg.spark.actions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkContentFile;
import org.apache.iceberg.spark.SparkDataFile;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseManifestUpdateSparkAction<T> extends BaseSnapshotUpdateSparkAction<T> {

  private final Table table;

  private static final Logger LOG = LoggerFactory.getLogger(BaseManifestUpdateSparkAction.class);

  public static final String USE_CACHING = "use-caching";
  public static final boolean USE_CACHING_DEFAULT = false;

  private PartitionSpec spec = null;

  private final int formatVersion;

  protected BaseManifestUpdateSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.spec = table.spec();
    TableOperations ops = ((HasTableOperations) table).operations();
    this.formatVersion = ops.current().formatVersion();
  }

  protected void validateManifest(ManifestFile manifest) {
    ValidationException.check(
        hasFileCounts(manifest), "No file counts in manifest: %s", manifest.path());
  }

  private boolean hasFileCounts(ManifestFile manifest) {
    return manifest.addedFilesCount() != null
        && manifest.existingFilesCount() != null
        && manifest.deletedFilesCount() != null;
  }

  protected <T, U> U withReusableDS(Dataset<T> ds, Function<Dataset<T>, U> func) {
    boolean useCaching =
        PropertyUtil.propertyAsBoolean(options(), USE_CACHING, USE_CACHING_DEFAULT);
    Dataset<T> reusableDS = useCaching ? ds.cache() : ds;

    try {
      return func.apply(reusableDS);
    } finally {
      if (useCaching) {
        reusableDS.unpersist(false);
      }
    }
  }

  protected WriteManifests<?> newWriteManifestsFunc(
      ManifestContent content,
      StructType sparkType,
      String outputLocation,
      long targetManifestSizeBytes,
      String fileNamePrefix) {
    ManifestWriterFactory writers =
        manifestWriters(outputLocation, targetManifestSizeBytes, fileNamePrefix);

    StructType sparkFileType = (StructType) sparkType.apply("data_file").dataType();
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType fileType = DataFile.getType(spec.partitionType());

    if (content == ManifestContent.DATA) {
      return new WriteDataManifests(writers, combinedFileType, fileType, sparkFileType);
    } else {
      return new WriteDeleteManifests(writers, combinedFileType, fileType, sparkFileType);
    }
  }

  protected ManifestWriterFactory manifestWriters(
      String outputLocation, long targetManifestSizeBytes, String fileNamePrefix) {
    return new ManifestWriterFactory(
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table)),
        formatVersion,
        spec.specId(),
        outputLocation,
        // allow the actual size of manifests to be 20% higher as the estimation is not precise
        (long) (1.2 * targetManifestSizeBytes),
        fileNamePrefix);
  }

  protected List<ManifestFile> loadManifests(
      ManifestContent content, Snapshot snapshot, Function<ManifestFile, Boolean> filter) {

    List<ManifestFile> manifests;

    if (snapshot == null) {
      manifests = ImmutableList.of();
    } else {
      switch (content) {
        case DATA:
          manifests = snapshot.dataManifests(table.io());
          break;
        case DELETES:
          manifests = snapshot.deleteManifests(table.io());
          break;
        default:
          throw new IllegalArgumentException("Unknown manifest content: " + content);
      }
    }

    return manifests.stream().filter(filter::apply).collect(Collectors.toList());
  }

  protected void replaceManifests(
      Iterable<ManifestFile> deletedManifests,
      Iterable<ManifestFile> addedManifests,
      boolean shouldStageManifests) {

    BiFunction<Long, Long, Void> comparisonFn =
        (createdManifestsFilesCount, replacedManifestsFilesCount) -> {
          if (!createdManifestsFilesCount.equals(replacedManifestsFilesCount)) {
            throw new ValidationException(
                "Replaced and created manifests must have the same number of active files: %d (new), %d (old)",
                createdManifestsFilesCount, replacedManifestsFilesCount);
          }
          return null;
        };

    replaceManifests(deletedManifests, addedManifests, shouldStageManifests, comparisonFn);
  }

  protected void replaceManifests(
      Iterable<ManifestFile> deletedManifests,
      Iterable<ManifestFile> addedManifests,
      boolean shouldStageManifests,
      BiFunction<Long, Long, Void> comparisonFunction) {
    try {

      org.apache.iceberg.RewriteManifests rewriteManifests = table.rewriteManifests();
      rewriteManifests.validateWith(comparisonFunction);

      deletedManifests.forEach(rewriteManifests::deleteManifest);
      addedManifests.forEach(rewriteManifests::addManifest);
      commit(rewriteManifests);

      if (shouldStageManifests) {
        // delete new manifests as they were rewritten before the commit
        deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      }
    } catch (CommitStateUnknownException commitStateUnknownException) {
      // don't clean up added manifest files, because they may have been successfully committed.
      throw commitStateUnknownException;
    } catch (Exception e) {
      // delete all new manifests because the rewrite failed
      deleteFiles(Iterables.transform(addedManifests, ManifestFile::path));
      throw e;
    }
  }

  private void deleteFiles(Iterable<String> locations) {
    Tasks.foreach(locations)
        .executeWith(ThreadPools.getWorkerPool())
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((location, exc) -> LOG.warn("Failed to delete: {}", location, exc))
        .run(location -> table.io().deleteFile(location));
  }

  protected static class WriteDataManifests extends WriteManifests<DataFile> {

    WriteDataManifests(
        ManifestWriterFactory manifestWriters,
        Types.StructType combinedPartitionType,
        Types.StructType partitionType,
        StructType sparkFileType) {
      super(manifestWriters, combinedPartitionType, partitionType, sparkFileType);
    }

    @Override
    protected SparkDataFile newFileWrapper() {
      return new SparkDataFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DataFile> newManifestWriter() {
      return writers().newRollingManifestWriter();
    }
  }

  protected static class WriteDeleteManifests extends WriteManifests<DeleteFile> {

    WriteDeleteManifests(
        ManifestWriterFactory manifestWriters,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType) {
      super(manifestWriters, combinedFileType, fileType, sparkFileType);
    }

    @Override
    protected SparkDeleteFile newFileWrapper() {
      return new SparkDeleteFile(combinedFileType(), fileType(), sparkFileType());
    }

    @Override
    protected RollingManifestWriter<DeleteFile> newManifestWriter() {
      return writers().newRollingDeleteManifestWriter();
    }
  }

  protected abstract static class WriteManifests<F extends ContentFile<F>>
      implements MapPartitionsFunction<Row, ManifestFile> {

    private static final Encoder<ManifestFile> MANIFEST_ENCODER =
        Encoders.javaSerialization(ManifestFile.class);

    private final ManifestWriterFactory writers;
    private final Types.StructType combinedFileType;
    private final Types.StructType fileType;
    private final StructType sparkFileType;

    WriteManifests(
        ManifestWriterFactory writers,
        Types.StructType combinedFileType,
        Types.StructType fileType,
        StructType sparkFileType) {
      this.writers = writers;
      this.combinedFileType = combinedFileType;
      this.fileType = fileType;
      this.sparkFileType = sparkFileType;
    }

    protected abstract SparkContentFile<F> newFileWrapper();

    protected abstract RollingManifestWriter<F> newManifestWriter();

    public Dataset<ManifestFile> apply(Dataset<Row> input) {
      return input.mapPartitions(this, MANIFEST_ENCODER);
    }

    @Override
    public Iterator<ManifestFile> call(Iterator<Row> rows) throws Exception {
      SparkContentFile<F> fileWrapper = newFileWrapper();
      RollingManifestWriter<F> writer = newManifestWriter();

      try {
        while (rows.hasNext()) {
          Row row = rows.next();
          long snapshotId = row.getLong(0);
          long sequenceNumber = row.getLong(1);
          Long fileSequenceNumber = row.isNullAt(2) ? null : row.getLong(2);
          Row file = row.getStruct(3);
          writer.existing(fileWrapper.wrap(file), snapshotId, sequenceNumber, fileSequenceNumber);
        }
      } finally {
        writer.close();
      }

      return writer.toManifestFiles().iterator();
    }

    protected ManifestWriterFactory writers() {
      return writers;
    }

    protected Types.StructType combinedFileType() {
      return combinedFileType;
    }

    protected Types.StructType fileType() {
      return fileType;
    }

    protected StructType sparkFileType() {
      return sparkFileType;
    }
  }

  protected static class ManifestWriterFactory implements Serializable {
    private final Broadcast<Table> tableBroadcast;
    private final int formatVersion;
    private final int specId;
    private final String outputLocation;
    private final long maxManifestSizeBytes;

    private final String fileNamePrefix;

    ManifestWriterFactory(
        Broadcast<Table> tableBroadcast,
        int formatVersion,
        int specId,
        String outputLocation,
        long maxManifestSizeBytes,
        String fileNamePrefix) {
      this.tableBroadcast = tableBroadcast;
      this.formatVersion = formatVersion;
      this.specId = specId;
      this.outputLocation = outputLocation;
      this.maxManifestSizeBytes = maxManifestSizeBytes;
      this.fileNamePrefix = fileNamePrefix;
    }

    public RollingManifestWriter<DataFile> newRollingManifestWriter() {
      return new RollingManifestWriter<>(this::newManifestWriter, maxManifestSizeBytes);
    }

    private ManifestWriter<DataFile> newManifestWriter() {
      return ManifestFiles.write(formatVersion, spec(), newOutputFile(), null);
    }

    public RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter() {
      return new RollingManifestWriter<>(this::newDeleteManifestWriter, maxManifestSizeBytes);
    }

    private ManifestWriter<DeleteFile> newDeleteManifestWriter() {
      return ManifestFiles.writeDeleteManifest(formatVersion, spec(), newOutputFile(), null);
    }

    private PartitionSpec spec() {
      return table().specs().get(specId);
    }

    private OutputFile newOutputFile() {
      return table().io().newOutputFile(newManifestLocation());
    }

    private String newManifestLocation() {
      String fileName = FileFormat.AVRO.addExtension(fileNamePrefix + UUID.randomUUID());
      Path filePath = new Path(outputLocation, fileName);
      return filePath.toString();
    }

    private Table table() {
      return tableBroadcast.value();
    }
  }
}

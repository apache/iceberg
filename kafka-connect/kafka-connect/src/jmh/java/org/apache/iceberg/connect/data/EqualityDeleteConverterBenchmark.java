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
package org.apache.iceberg.connect.data;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures one conversion of a commit's equality deletes against an unpartitioned format version 3
 * table. With no partition to prune by, the key filter alone decides which data files are read.
 *
 * <ul>
 *   <li>{@code keyLayout}: {@code SEQUENTIAL} keys are increasing IDs, so the data files hold
 *       disjoint key ranges. {@code UNSORTED_UUID} keys are random UUID strings stored in arrival
 *       order, so every data file spans almost the whole key range. {@code SORTED_UUID} stores the
 *       same keys sorted, as after a sort compaction.
 *   <li>{@code changedRows}: {@code RECENT} deletes the newest rows, {@code SCATTERED} deletes rows
 *       spread evenly over arrival order, and {@code NEW} deletes keys that no row has, as an
 *       upsert of a new row does.
 * </ul>
 *
 * <p>The counters report the data files planned as candidates, the deletion vectors written, and
 * the rows they delete. JMH sums them over the measurement iterations, so divide them by {@code
 * Cnt} for one conversion.
 *
 * <p>Each table is written once and reused by later runs. By default the tables are kept in a
 * Hadoop catalog in the JVM's temporary directory, so the files are read from local disk, which
 * costs less per file than an object store. To start over, delete the {@code
 * kc-eq-delete-converter-bench} directory there, or drop the tables of another catalog. To use
 * another catalog, set the environment variable {@code BENCHMARK_CATALOG_PROPERTIES} to a file with
 * its catalog properties. {@code
 * kafka-connect/kafka-connect-runtime/docker/benchmark-catalog.properties} uses the REST catalog
 * and the object store of the integration tests, started with {@code docker compose -f
 * kafka-connect/kafka-connect-runtime/docker/docker-compose.yml up -d iceberg}.
 *
 * <p>Run with {@code ./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:jmh
 * -PjmhIncludeRegex=EqualityDeleteConverterBenchmark
 * -PjmhOutputPath=benchmark/eq-delete-converter-benchmark.txt}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class EqualityDeleteConverterBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityDeleteConverterBenchmark.class);
  private static final String CATALOG_PROPERTIES_ENV = "BENCHMARK_CATALOG_PROPERTIES";
  private static final Namespace NAMESPACE = Namespace.of("kc_eq_delete_converter_bench");
  private static final int KEY_FIELD_ID = 1;
  private static final String PAYLOAD = "x".repeat(64);
  // maps arrival order to sort order; coprime with any realistic row count
  private static final long PERMUTATION_PRIME = 2_147_483_647L;

  public enum KeyLayout {
    SEQUENTIAL,
    UNSORTED_UUID,
    SORTED_UUID
  }

  public enum ChangedRows {
    RECENT,
    SCATTERED,
    NEW
  }

  @Param({"1000", "10000"})
  private int dataFiles;

  @Param("1000")
  private int rowsPerFile;

  @Param({"SEQUENTIAL", "UNSORTED_UUID", "SORTED_UUID"})
  private KeyLayout keyLayout;

  @Param({"RECENT", "SCATTERED", "NEW"})
  private ChangedRows changedRows;

  // below, at, and above KeyFilters.MAX_LISTED_KEYS
  @Param({"100", "1000", "10000"})
  private int deletedKeys;

  private long totalRows;
  private long uuidStep;
  private Table table;
  private List<DeleteFile> eqDeleteFiles;
  private final Set<String> dvLocations = Sets.newHashSet();

  @Setup(Level.Trial)
  public void setupBenchmark() {
    this.totalRows = (long) dataFiles * rowsPerFile;
    this.uuidStep = Long.divideUnsigned(-1L, totalRows);
    String tableName =
        String.format(Locale.ROOT, "%s_%dx%d", keyLayout, dataFiles, rowsPerFile)
            .toLowerCase(Locale.ROOT);
    this.table = loadOrWrite(catalog(), TableIdentifier.of(NAMESPACE, tableName));
    this.eqDeleteFiles = ImmutableList.of(writeEqualityDeletes(deletedKeys()));
    LOG.info(
        "Converting {} {} keys against {} data files with {} keys",
        deletedKeys,
        changedRows,
        dataFiles,
        keyLayout);
  }

  @TearDown(Level.Iteration)
  public void removeDeletionVectors() {
    dvLocations.forEach(table.io()::deleteFile);
    dvLocations.clear();
  }

  @TearDown(Level.Trial)
  public void removeEqualityDeletes() {
    eqDeleteFiles.forEach(deleteFile -> table.io().deleteFile(deleteFile.location()));
  }

  @Benchmark
  @Threads(1)
  public void convert(Blackhole blackhole, Counters counters) {
    EqualityDeleteConverter.Result result =
        new EqualityDeleteConverter(table, null).convert(eqDeleteFiles);
    counters.record(result);
    result.dvFiles().forEach(dv -> dvLocations.add(dv.location()));
    blackhole.consume(result);
  }

  private static Catalog catalog() {
    String propertiesFile = System.getenv(CATALOG_PROPERTIES_ENV);
    Map<String, String> properties;
    if (propertiesFile == null) {
      File warehouse =
          new File(System.getProperty("java.io.tmpdir"), "kc-eq-delete-converter-bench");
      properties =
          ImmutableMap.of(
              CatalogUtil.ICEBERG_CATALOG_TYPE,
              CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
              CatalogProperties.WAREHOUSE_LOCATION,
              warehouse.getAbsolutePath());
    } else {
      Properties loaded = new Properties();
      try (Reader reader =
          Files.newBufferedReader(Paths.get(propertiesFile), StandardCharsets.UTF_8)) {
        loaded.load(reader);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      properties = Maps.fromProperties(loaded);
    }

    return CatalogUtil.buildIcebergCatalog("benchmark", properties, new Configuration());
  }

  private Table loadOrWrite(Catalog catalog, TableIdentifier identifier) {
    if (catalog instanceof SupportsNamespaces namespaces
        && !namespaces.namespaceExists(NAMESPACE)) {
      namespaces.createNamespace(NAMESPACE);
    }

    Table loaded =
        catalog.tableExists(identifier)
            ? catalog.loadTable(identifier)
            : catalog.createTable(
                identifier,
                schema(),
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    if (loaded.currentSnapshot() == null) {
      LOG.info(
          "Writing {} data files with {} rows each to {}",
          dataFiles,
          rowsPerFile,
          loaded.location());
      DataFile[] files = new DataFile[dataFiles];
      Tasks.range(dataFiles)
          .executeWith(ThreadPools.getWorkerPool())
          .run(file -> files[file] = writeDataFile(loaded, file));

      AppendFiles append = loaded.newAppend();
      for (DataFile file : files) {
        append.appendFile(file);
      }

      append.commit();
    }

    return loaded;
  }

  private Schema schema() {
    Type keyType =
        keyLayout == KeyLayout.SEQUENTIAL ? Types.LongType.get() : Types.StringType.get();
    return new Schema(
        ImmutableList.of(
            Types.NestedField.required(KEY_FIELD_ID, "id", keyType),
            Types.NestedField.optional(2, "payload", Types.StringType.get())),
        ImmutableSet.of(KEY_FIELD_ID));
  }

  /** Writes the rows at positions [file * rowsPerFile, (file + 1) * rowsPerFile) of the table. */
  private DataFile writeDataFile(Table target, int file) {
    DataWriter<Record> writer =
        writerFactory(target)
            .newDataWriter(fileFactory(target).newOutputFile(), target.spec(), null);
    try (DataWriter<Record> closeable = writer) {
      long first = (long) file * rowsPerFile;
      for (long position = first; position < first + rowsPerFile; position++) {
        Record row = GenericRecord.create(target.schema());
        row.setField("id", storedKey(position));
        row.setField("payload", PAYLOAD);
        closeable.write(row);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return writer.toDataFile();
  }

  private DeleteFile writeEqualityDeletes(List<Object> keys) {
    Schema keySchema = table.schema().select("id");
    EqualityDeleteWriter<Record> writer =
        writerFactory(table)
            .newEqualityDeleteWriter(fileFactory(table).newOutputFile(), table.spec(), null);
    try (EqualityDeleteWriter<Record> closeable = writer) {
      for (Object key : keys) {
        Record record = GenericRecord.create(keySchema);
        record.setField("id", key);
        closeable.write(record);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return writer.toDeleteFile();
  }

  private static FileWriterFactory<Record> writerFactory(Table target) {
    return new GenericFileWriterFactory.Builder(target)
        .dataSchema(target.schema())
        .dataFileFormat(FileFormat.PARQUET)
        .deleteFileFormat(FileFormat.PARQUET)
        .equalityFieldIds(new int[] {KEY_FIELD_ID})
        .equalityDeleteRowSchema(target.schema().select("id"))
        .build();
  }

  private static OutputFileFactory fileFactory(Table target) {
    return OutputFileFactory.builderFor(target, 1, System.nanoTime())
        .format(FileFormat.PARQUET)
        .build();
  }

  private List<Object> deletedKeys() {
    List<Object> keys = Lists.newArrayListWithCapacity(deletedKeys);
    for (int i = 0; i < deletedKeys; i++) {
      keys.add(
          switch (changedRows) {
            case RECENT -> arrivedKey(totalRows - deletedKeys + i);
            case SCATTERED -> arrivedKey(i * totalRows / deletedKeys);
            case NEW -> newKey(i);
          });
    }

    return keys;
  }

  /** Key of the row stored at the given position of the table's data files. */
  private Object storedKey(long position) {
    return keyLayout == KeyLayout.SORTED_UUID ? uuidKey(position, 0L) : arrivedKey(position);
  }

  /** Key of the row that arrived at the given position. */
  private Object arrivedKey(long arrival) {
    return keyLayout == KeyLayout.SEQUENTIAL ? arrival : uuidKey(sortPosition(arrival), 0L);
  }

  /** A key that no row has, placed at a pseudo-random point of the key range. */
  private Object newKey(long index) {
    return keyLayout == KeyLayout.SEQUENTIAL
        ? totalRows + index
        : uuidKey(sortPosition(index), uuidStep / 2);
  }

  /** Position of an arrived row in key order, a permutation of [0, totalRows). */
  private long sortPosition(long arrival) {
    return arrival * PERMUTATION_PRIME % totalRows;
  }

  /**
   * UUID strings that sort as their positions do, spread over the whole key range. The low half is
   * random but fixed per position, so the keys compress like random UUIDs.
   */
  private String uuidKey(long sortPosition, long offset) {
    long random = new SplittableRandom(sortPosition).nextLong();
    return new UUID(sortPosition * uuidStep + offset, random).toString();
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class Counters {
    private long plannedDataFiles;
    private long deletionVectors;
    private long deletedRows;

    @Setup(Level.Iteration)
    public void reset() {
      this.plannedDataFiles = 0;
      this.deletionVectors = 0;
      this.deletedRows = 0;
    }

    void record(EqualityDeleteConverter.Result result) {
      this.plannedDataFiles += result.plannedDataFiles();
      this.deletionVectors += result.dvFiles().size();
      for (DeleteFile dv : result.dvFiles()) {
        this.deletedRows += dv.recordCount();
      }
    }

    public long plannedDataFiles() {
      return plannedDataFiles;
    }

    public long deletionVectors() {
      return deletionVectors;
    }

    public long deletedRows() {
      return deletedRows;
    }
  }
}

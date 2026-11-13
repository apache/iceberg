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
package org.apache.iceberg.tornadovm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;

/**
 * The host half of a merge-on-read scan: what one device batch needs to know about its data files
 * and their deletes, prepared off the critical path while the device works on the previous batch.
 *
 * <p>Delete files go one of two ways:
 *
 * <ul>
 *   <li><b>read on the device</b> -- file-scoped Parquet position-delete files (the ones whose
 *       {@link DeleteFile#referencedDataFile()} is the data file, which is what Iceberg writes by
 *       default) and Parquet equality-delete files over a required key. cuDF reads their {@code
 *       pos} or key column straight into device memory; the host only names the files.
 *   <li><b>loaded on the host</b> -- deletion vectors, through Iceberg's own {@link
 *       BaseDeleteLoader} (a Puffin roaring bitmap is already compact), and anything the device
 *       path does not cover: partition-scoped position deletes, a nullable equality key, a
 *       non-Parquet delete file. Positions become one bit a row; keys become a host set that is
 *       uploaded.
 * </ul>
 *
 * <p>Which delete files apply to which data file -- the sequence-number rules -- is decided by
 * Iceberg's scan planning before any of this runs; {@link FileScanTask#deletes()} is already that
 * answer.
 */
final class HostDeletes {

  /** One device batch: one or more data files, read as one. */
  static final class FilePrep {
    private String path;
    private long rows;
    private int[] columns;
    private byte[] positionBits;
    private EqualitySet equality;
    private KeySet keySet;
    private long hostNanos;
    private int files = 1;

    // Position-delete files read on the device: their pos columns concatenated, and for each run of
    // them, where its data file starts in the batch.
    private final StringBuilder positionPaths = new StringBuilder();
    private long positionRows;
    private int positionColumn = -1;
    private final List<Integer> segmentStarts = Lists.newArrayList();
    private final List<Integer> segmentOffsets = Lists.newArrayList();

    // Equality-delete files read on the device.
    private String setPaths;
    private long setRows;
    private int setColumn = -1;

    // Each data file's start in the batch, first row id and data sequence number (v3 lineage).
    private final List<Integer> fileStarts = Lists.newArrayList();
    private final List<Long> firstRowIds = Lists.newArrayList();
    private final List<Long> sequences = Lists.newArrayList();
    private boolean storedLineage;
    // The batch's file scan tasks, in the order fileStarts lists them.
    private final List<FileScanTask> tasks = Lists.newArrayList();

    String path() {
      return path;
    }

    long rows() {
      return rows;
    }

    // The batch's partition spec and partition (a rewrite's batch is one partition's).
    private org.apache.iceberg.PartitionSpec spec;
    private org.apache.iceberg.StructLike partition;

    org.apache.iceberg.PartitionSpec spec() {
      return spec;
    }

    org.apache.iceberg.StructLike partition() {
      return partition;
    }

    int files() {
      return files;
    }

    int[] columns() {
      return columns;
    }

    /** One bit a row, set when the row is deleted; null when nothing was loaded on the host. */
    byte[] positionBits() {
      return positionBits;
    }

    /** Host-loaded equality keys, or null when none apply or they are read on the device. */
    EqualitySet equality() {
      return equality;
    }

    long hostNanos() {
      return hostNanos;
    }

    List<Integer> fileStarts() {
      return fileStarts;
    }

    /** The data files' first row ids; an entry is null where the table assigned none. */
    List<Long> firstRowIds() {
      return firstRowIds;
    }

    List<Long> sequences() {
      return sequences;
    }

    List<FileScanTask> tasks() {
      return tasks;
    }

    /** True when a data file of the batch stores lineage columns of its own. */
    boolean storedLineage() {
      return storedLineage;
    }

    String positionPaths() {
      return positionPaths.toString();
    }

    long positionRows() {
      return positionRows;
    }

    int positionColumn() {
      return positionColumn;
    }

    List<Integer> segmentStarts() {
      return segmentStarts;
    }

    List<Integer> segmentOffsets() {
      return segmentOffsets;
    }

    /** Equality-delete files to read on the device, one a line, or null. */
    String setPaths() {
      return setPaths;
    }

    long setRows() {
      return setRows;
    }

    int setColumn() {
      return setColumn;
    }

    /** Multi-column equality keys loaded on the host, or null. */
    KeySet keySet() {
      return keySet;
    }

    /** The identity of the equality deletes that apply, read either way; empty when none. */
    String equalityId() {
      return setPaths != null
          ? setPaths
          : equality != null ? equality.id() : keySet != null ? keySet.id() : "";
    }
  }

  /** The keys of a set of equality-delete files over one column, loaded on the host. */
  static final class KeySet {
    private final String id;
    private final int columns;
    private final int size;
    private final long[] hashes;
    private final long[] keys; // column c at c * size
    private final byte[] nulls; // column c at c * size
    private volatile int[] table; // open addressing on the hash, for the host probe; built once

    KeySet(String id, int columns, int size, long[] hashes, long[] keys, byte[] nulls) {
      this.id = id;
      this.columns = columns;
      this.size = size;
      this.hashes = hashes;
      this.keys = keys;
      this.nulls = nulls;
    }

    String id() {
      return id;
    }

    int columns() {
      return columns;
    }

    int size() {
      return size;
    }

    long[] hashes() {
      return hashes;
    }

    long[] keys() {
      return keys;
    }

    byte[] nulls() {
      return nulls;
    }

    /**
     * Whether a key is in the set: the host probe, for the CPU scan. The device binary-searches
     * sorted hashes; a CPU does better with an open-addressing table on the same hash, so the
     * baseline uses that. Every candidate is compared column by column, a null equal to a null.
     */
    boolean contains(long[] key, byte[] isNull) {
      int[] slots = table();
      long hash = DeviceKernels.KEY_HASH_SEED;
      for (int c = 0; c < columns; c++) {
        hash = DeviceKernels.keyHash(hash, key[c], isNull[c]);
      }
      int mask = slots.length - 1;
      for (int at = (int) hash & mask; slots[at] != 0; at = (at + 1) & mask) {
        int entry = slots[at] - 1;
        if (hashes[entry] != hash) {
          continue;
        }
        boolean same = true;
        for (int c = 0; c < columns && same; c++) {
          byte entryNull = nulls[c * size + entry];
          same = entryNull == isNull[c] && (isNull[c] == 1 || keys[c * size + entry] == key[c]);
        }
        if (same) {
          return true;
        }
      }
      return false;
    }

    private int[] table() {
      int[] built = table;
      return built != null ? built : buildTable();
    }

    private synchronized int[] buildTable() {
      if (table == null) {
        int[] slots = new int[Integer.highestOneBit(Math.max(2, size * 2 - 1)) << 1];
        int mask = slots.length - 1;
        for (int entry = 0; entry < size; entry++) {
          int at = (int) hashes[entry] & mask;
          while (slots[at] != 0) {
            at = (at + 1) & mask;
          }
          slots[at] = entry + 1;
        }
        table = slots;
      }
      return table;
    }
  }

  static final class EqualitySet {
    private final String id;
    private final long[] keys;
    private final boolean hasNull;

    EqualitySet(String id, long[] keys, boolean hasNull) {
      this.id = id;
      this.keys = keys;
      this.hasNull = hasNull;
    }

    String id() {
      return id;
    }

    long[] keys() {
      return keys;
    }

    boolean hasNull() {
      return hasNull;
    }
  }

  private final Table table;
  private final ScanLayout layout;
  private final Integer equalityKeyId;
  private final boolean deviceDeletes;
  private final BaseDeleteLoader deleteLoader;
  // Host-loaded equality sets, kept across scans: their identity is their delete files' paths,
  // which never change content.
  private static final com.github.benmanes.caffeine.cache.Cache<String, EqualitySet> EQUALITY_SETS =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(16).build();

  // Parquet footers and column positions, kept across scans: Iceberg never rewrites a file in
  // place,
  // so a path names the same bytes for as long as it exists. Small (a schema a file), and bounded.
  /**
   * What a scan needs from a Parquet footer: the schema, and for each top-level string column (by
   * field id) the bytes its values decode to, summed over the row groups -- parquet-java's size
   * statistics, -1 when the writer left them out; and for each top-level INT32 or INT64 column the
   * smallest and largest value over the row groups, absent when a row group has no statistics.
   */
  record Footer(MessageType schema, Map<Integer, Long> stringBytes, Map<Integer, long[]> bounds) {}

  private static final com.github.benmanes.caffeine.cache.Cache<String, Footer> FOOTERS =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(200_000).build();
  // Multi-column equality sets, loaded on the host once a set of delete files.
  private static final com.github.benmanes.caffeine.cache.Cache<String, KeySet> KEY_SETS =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(16).build();
  private List<Integer> keyIds = List.of();
  private static final com.github.benmanes.caffeine.cache.Cache<String, Integer> COLUMNS =
      com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(200_000).build();

  /**
   * @param deviceDeletes whether delete files the device can read are left to it; false loads every
   *     delete on the host, as the CPU scan needs
   */
  HostDeletes(Table table, ScanLayout layout, Integer equalityKeyId, boolean deviceDeletes) {
    this.table = table;
    this.layout = layout;
    this.equalityKeyId = equalityKeyId;
    this.deviceDeletes = deviceDeletes;
    this.deleteLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file.location()));
  }

  /**
   * The single column every equality delete in the scan is keyed on, or null when there are none.
   * Multi-column equality deletes are declined rather than half-applied.
   */
  static Integer equalityKeyId(List<FileScanTask> tasks) {
    List<Integer> ids = equalityKeyIds(tasks);
    return ids.size() == 1 ? ids.get(0) : null;
  }

  /**
   * The columns every equality delete of the scan compares, or an empty list. Deletes over
   * different column sets in one scan are refused.
   */
  static List<Integer> equalityKeyIds(List<FileScanTask> tasks) {
    List<Integer> key = List.of();
    for (FileScanTask task : tasks) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() != FileContent.EQUALITY_DELETES) {
          continue;
        }
        List<Integer> ids = delete.equalityFieldIds();
        if (!key.isEmpty() && !key.equals(ids)) {
          throw new UnsupportedOperationException(
              "Equality deletes over different columns in one scan: " + key + " and " + ids);
        }
        key = List.copyOf(ids);
      }
    }
    return key;
  }

  /** Equality deletes over several columns: loaded on the host as a {@link KeySet}. */
  HostDeletes withKeyIds(List<Integer> ids) {
    for (int id : ids) {
      Types.NestedField field = table.schema().findField(id);
      Type.TypeID type = field.type().typeId();
      if (type != Type.TypeID.INTEGER
          && type != Type.TypeID.LONG
          && type != Type.TypeID.DATE
          && type != Type.TypeID.TIMESTAMP) {
        throw new UnsupportedOperationException(
            "Equality deletes over several columns need integer, date or timestamp keys: " + field);
      }
    }
    this.keyIds = ids;
    return this;
  }

  /** The equality-delete files that apply to a task, as an identity: tasks with equal ids batch. */
  static String equalityId(FileScanTask task) {
    return task.deletes().stream()
        .filter(delete -> delete.content() == FileContent.EQUALITY_DELETES)
        .map(DeleteFile::location)
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.joining(","));
  }

  /**
   * Whether a task reads its whole data file: from at or before its first row group (Iceberg starts
   * a whole Parquet file's split after the 4-byte magic) to the end of the file.
   */
  static boolean wholeFile(FileScanTask task) {
    List<Long> offsets = task.file().splitOffsets();
    long first = offsets == null || offsets.isEmpty() ? 4 : offsets.get(0);
    return task.start() <= first && task.start() + task.length() >= task.file().fileSizeInBytes();
  }

  /** Whether the device reads this position-delete file itself. */
  static boolean readOnDevice(DeleteFile delete, String dataLocation) {
    return delete.content() == FileContent.POSITION_DELETES
        && delete.format() == FileFormat.PARQUET
        && dataLocation.equals(ContentFileUtil.referencedDataFileLocation(delete));
  }

  /** A local path cuDF can open: the file itself, or its cached copy from an object store. */
  String localPath(String location) {
    return LocalFiles.path(location, table.io());
  }

  /**
   * Where the file stores the projected columns, -1 for one it lacks, and how many columns it has:
   * what a batch must share. cuDF reads several files as one source only when they have the same
   * number of columns, and a version-3 table's updated files carry the row-lineage columns too.
   */
  String columnLayout(FileScanTask task) {
    String location = task.file().location();
    MessageType footer = footerSchema(localPath(location));
    return Arrays.toString(layout.parquetColumns(footer, location)) + "/" + footer.getFieldCount();
  }

  /** One data file. */
  FilePrep prepare(FileScanTask task) {
    return prepare(List.of(task));
  }

  /**
   * Several data files read as one device batch: their paths one a line, their rows concatenated,
   * their position bitmaps shifted to their offset in the batch. They share one equality-delete set
   * (batches are formed that way), and their projected columns must sit at the same Parquet
   * indices, since one read resolves them once.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  FilePrep prepare(List<FileScanTask> batch) {
    long start = System.nanoTime();
    FilePrep prep = new FilePrep();
    prep.files = batch.size();
    prep.spec = batch.get(0).spec();
    prep.partition = batch.get(0).file().partition();
    StringBuilder paths = new StringBuilder();
    List<long[]> hostPositions = Lists.newArrayList();
    long offset = 0;
    long hostDeleted = 0;
    for (FileScanTask task : batch) {
      String location = task.file().location();
      String path = localPath(location);
      long rows = task.file().recordCount();
      if (task.file().format() != FileFormat.PARQUET) {
        throw new UnsupportedOperationException("Not a Parquet data file: " + location);
      }
      if (!wholeFile(task)) {
        throw new UnsupportedOperationException("Split data files are not supported: " + location);
      }
      MessageType footer = footerSchema(path);
      int[] columns = layout.parquetColumns(footer, location);
      for (int i = 0; i < footer.getFieldCount(); i++) {
        org.apache.parquet.schema.Type column = footer.getType(i);
        if (column.getId() != null
            && (column.getId().intValue() == MetadataColumns.ROW_ID.fieldId()
                || column.getId().intValue()
                    == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId())) {
          prep.storedLineage = true;
        }
      }
      if (prep.columns == null) {
        prep.columns = columns;
      } else if (!Arrays.equals(columns, prep.columns)) {
        throw new UnsupportedOperationException(
            "Data files of one batch store the projected columns at different positions: "
                + location);
      }
      if (paths.length() > 0) {
        paths.append('\n');
      }
      paths.append(path);
      prep.fileStarts.add((int) offset);
      prep.tasks.add(task);
      prep.firstRowIds.add(task.file().firstRowId());
      prep.sequences.add(task.file().dataSequenceNumber());

      List<DeleteFile> hostDeletes = Lists.newArrayList();
      List<DeleteFile> equalityDeletes = Lists.newArrayList();
      boolean segmentOpen = false;
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() == FileContent.EQUALITY_DELETES) {
          equalityDeletes.add(delete);
        } else if (deviceDeletes && readOnDevice(delete, location)) {
          if (!segmentOpen) {
            prep.segmentStarts.add((int) prep.positionRows);
            prep.segmentOffsets.add((int) offset);
            segmentOpen = true;
          }
          String deletePath = localPath(delete.location());
          if (prep.positionPaths.length() > 0) {
            prep.positionPaths.append('\n');
          }
          prep.positionPaths.append(deletePath);
          prep.positionRows += delete.recordCount();
          int column = column(deletePath, MetadataColumns.DELETE_FILE_POS.fieldId());
          if (prep.positionColumn >= 0 && prep.positionColumn != column) {
            throw new UnsupportedOperationException(
                "Position-delete files of one batch store pos at different positions");
          }
          prep.positionColumn = column;
        } else {
          hostDeletes.add(delete);
        }
      }

      if (!hostDeletes.isEmpty()) {
        PositionDeleteIndex index = deleteLoader.loadPositionDeletes(hostDeletes, location);
        long[] positions = new long[(int) index.cardinality()];
        int[] count = {0};
        long base = offset;
        index.forEach(
            position -> {
              if (position < rows && count[0] < positions.length) {
                positions[count[0]++] = base + position;
              }
            });
        hostPositions.add(Arrays.copyOf(positions, count[0]));
        hostDeleted += count[0];
      }

      if (!equalityDeletes.isEmpty()
          && prep.setPaths == null
          && prep.equality == null
          && prep.keySet == null) {
        applyEquality(prep, equalityDeletes);
      }
      offset += rows;
    }
    prep.path = paths.toString();
    prep.rows = offset;
    if (hostDeleted > 0) {
      byte[] bits = new byte[(int) ((offset + 7) / 8)];
      for (long[] positions : hostPositions) {
        for (long position : positions) {
          bits[(int) (position >> 3)] |= (byte) (1 << (position & 7));
        }
      }
      prep.positionBits = bits;
    }
    prep.hostNanos = System.nanoTime() - start;
    return prep;
  }

  private void applyEquality(FilePrep prep, List<DeleteFile> deletes) {
    if (keyIds.size() > 1) {
      String id =
          deletes.stream()
              .map(DeleteFile::location)
              .sorted(Comparator.naturalOrder())
              .collect(Collectors.joining(","));
      prep.keySet = KEY_SETS.get(id, ignored -> loadKeySet(id, deletes));
      return;
    }
    Types.NestedField key = table.schema().findField(equalityKeyId);
    boolean allParquet = deletes.stream().allMatch(d -> d.format() == FileFormat.PARQUET);
    boolean provided = deletes.stream().anyMatch(d -> isProvided(d.location()));
    if (deviceDeletes && key.isRequired() && allParquet && !provided) {
      // A required key cannot be null in a delete file either, so the device needs no null flag.
      List<DeleteFile> sorted = Lists.newArrayList(deletes);
      sorted.sort(Comparator.comparing(DeleteFile::location));
      int column = -1;
      StringBuilder paths = new StringBuilder();
      long rows = 0;
      for (DeleteFile delete : sorted) {
        String path = localPath(delete.location());
        int own = column(path, equalityKeyId);
        if (column >= 0 && own != column) {
          throw new UnsupportedOperationException(
              "Equality-delete files store the key at different positions");
        }
        column = own;
        if (paths.length() > 0) {
          paths.append('\n');
        }
        paths.append(path);
        rows += delete.recordCount();
      }
      prep.setPaths = paths.toString();
      prep.setRows = rows;
      prep.setColumn = column;
    } else {
      prep.equality = equalitySet(deletes);
    }
  }

  /** The Parquet column index of a field id in a file, from its footer. */
  private int column(String path, int fieldId) {
    return COLUMNS.get(
        path + "#" + fieldId,
        ignored -> {
          MessageType schema = footerSchema(path);
          for (int i = 0; i < schema.getFieldCount(); i++) {
            org.apache.parquet.schema.Type column = schema.getType(i);
            if (column.getId() != null && column.getId().intValue() == fieldId) {
              return i;
            }
          }
          throw new UnsupportedOperationException(
              "Delete file " + path + " has no column with field id " + fieldId);
        });
  }

  private static MessageType footerSchema(String path) {
    return FOOTERS.get(path, HostDeletes::readFooter).schema();
  }

  /**
   * The bytes a string column of a data file decodes to, from its footer's size statistics, or -1
   * when the writer did not record them.
   */
  static long stringBytes(String path, int fieldId) {
    Long bytes = FOOTERS.get(path, HostDeletes::readFooter).stringBytes().get(fieldId);
    return bytes == null ? -1 : bytes;
  }

  /** The smallest and largest value of an INT32 or INT64 column of a data file, or null. */
  static long[] bounds(String path, int fieldId) {
    return FOOTERS.get(path, HostDeletes::readFooter).bounds().get(fieldId);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Footer readFooter(String path) {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(Paths.get(path)))) {
      MessageType schema = reader.getFileMetaData().getSchema();
      Map<Integer, Long> bytes = Maps.newHashMap();
      Map<Integer, long[]> bounds = Maps.newHashMap();
      Set<Integer> unbounded = Sets.newHashSet();
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block :
          reader.getFooter().getBlocks()) {
        for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData chunk : block.getColumns()) {
          if (chunk.getPath().size() != 1) {
            continue;
          }
          org.apache.parquet.schema.Type column = schema.getType(chunk.getPath().toArray()[0]);
          if (column.getId() == null || !column.isPrimitive()) {
            continue;
          }
          org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName physical =
              column.asPrimitiveType().getPrimitiveTypeName();
          if (physical == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32
              || physical == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64) {
            int id = column.getId().intValue();
            org.apache.parquet.column.statistics.Statistics<?> stats = chunk.getStatistics();
            if (stats == null || stats.isEmpty()) {
              unbounded.add(id);
            } else if (stats.hasNonNullValue()) {
              long min = ((Number) stats.genericGetMin()).longValue();
              long max = ((Number) stats.genericGetMax()).longValue();
              long[] seen = bounds.get(id);
              bounds.put(
                  id,
                  seen == null
                      ? new long[] {min, max}
                      : new long[] {Math.min(seen[0], min), Math.max(seen[1], max)});
            }
            continue;
          }
          org.apache.parquet.column.statistics.SizeStatistics size = chunk.getSizeStatistics();
          Optional<Long> unencoded =
              size == null || !size.isValid()
                  ? Optional.empty()
                  : size.getUnencodedByteArrayDataBytes();
          if (column.asPrimitiveType().getPrimitiveTypeName()
              != org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY) {
            continue;
          }
          int id = column.getId().intValue();
          long previous = bytes.getOrDefault(id, 0L);
          bytes.put(id, previous < 0 || unencoded.isEmpty() ? -1 : previous + unencoded.get());
        }
      }
      unbounded.forEach(bounds::remove);
      return new Footer(schema, bytes, bounds);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read the footer of " + path, e);
    }
  }

  /**
   * Reads the key columns of each equality-delete file, values widened to long, and hashes them.
   */
  private KeySet loadKeySet(String id, List<DeleteFile> deletes) {
    int columns = keyIds.size();
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int keyId : keyIds) {
      fields.add(table.schema().findField(keyId));
    }
    Schema keySchema = new Schema(fields);
    long total = deletes.stream().mapToLong(DeleteFile::recordCount).sum();
    if (total * columns > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Equality-delete set too large: " + total);
    }
    int capacity = (int) total;
    long[] values = new long[columns * capacity];
    byte[] nulls = new byte[columns * capacity];
    long[] hashes = new long[capacity];
    int size = 0;
    for (DeleteFile delete : deletes) {
      if (delete.format() != FileFormat.PARQUET) {
        throw new UnsupportedOperationException("Not a Parquet delete file: " + delete.location());
      }
      try (CloseableIterable<Record> records =
          Parquet.read(table.io().newInputFile(delete.location()))
              .project(keySchema)
              .createReaderFunc(
                  fileSchema -> GenericParquetReaders.buildReader(keySchema, fileSchema))
              .build()) {
        for (Record record : records) {
          long hash = DeviceKernels.KEY_HASH_SEED;
          for (int c = 0; c < columns; c++) {
            Object value = record.get(c);
            long widened = 0;
            byte isNull = 1;
            if (value != null) {
              isNull = 0;
              widened =
                  switch (value) {
                    case LocalDate date -> date.toEpochDay();
                    case LocalDateTime time -> DateTimeUtil.microsFromTimestamp(time);
                    case OffsetDateTime time -> DateTimeUtil.microsFromTimestamptz(time);
                    default -> ((Number) value).longValue();
                  };
            }
            values[c * capacity + size] = widened;
            nulls[c * capacity + size] = isNull;
            hash = DeviceKernels.keyHash(hash, widened, isNull);
          }
          hashes[size++] = hash;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    if (size != capacity) {
      // Columns are laid out at the declared count; a short read would misplace them.
      throw new IllegalStateException("Equality-delete files hold " + size + " rows, not " + total);
    }
    return new KeySet(id, columns, size, hashes, values, nulls);
  }

  /**
   * Keys held in memory under a placeholder delete-file location that was never written: what
   * {@code deleteKeys} probes. Read in place of a file, merged with any real equality-delete files
   * that apply too.
   */
  private static final Map<String, long[]> PROVIDED = Maps.newConcurrentMap();

  static final String PROVIDED_PREFIX = "memory://iceberg-tornadovm/keys/";

  static String provide(long[] keys) {
    String location = PROVIDED_PREFIX + UUID.randomUUID();
    PROVIDED.put(location, keys);
    return location;
  }

  static void forget(String location) {
    PROVIDED.remove(location);
    EQUALITY_SETS.asMap().keySet().removeIf(key -> key.contains(location));
  }

  static boolean isProvided(String location) {
    return location.startsWith(PROVIDED_PREFIX);
  }

  private EqualitySet equalitySet(List<DeleteFile> deletes) {
    String id =
        deletes.stream()
            .map(DeleteFile::location)
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.joining(","));
    return EQUALITY_SETS.get(id + "#" + equalityKeyId, ignored -> loadEqualitySet(id, deletes));
  }

  /**
   * Reads the key column of each equality-delete file. Keys are widened to long on the host; the
   * device set has the key column's own width.
   */
  private EqualitySet loadEqualitySet(String id, List<DeleteFile> deletes) {
    Types.NestedField key = table.schema().findField(equalityKeyId);
    Schema keySchema = new Schema(key);
    long[] keys = new long[1024];
    int size = 0;
    boolean hasNull = false;
    for (DeleteFile delete : deletes) {
      long[] held = PROVIDED.get(delete.location());
      if (held != null) {
        if (size + held.length > keys.length) {
          keys = Arrays.copyOf(keys, Math.max(keys.length * 2, size + held.length));
        }
        System.arraycopy(held, 0, keys, size, held.length);
        size += held.length;
        continue;
      }
      if (delete.format() != FileFormat.PARQUET) {
        throw new UnsupportedOperationException("Not a Parquet delete file: " + delete.location());
      }
      try (CloseableIterable<Record> records =
          Parquet.read(table.io().newInputFile(delete.location()))
              .project(keySchema)
              .createReaderFunc(
                  fileSchema -> GenericParquetReaders.buildReader(keySchema, fileSchema))
              .build()) {
        for (Record record : records) {
          Object value = record.get(0);
          if (value == null) {
            hasNull = true;
            continue;
          }
          if (size == keys.length) {
            keys = Arrays.copyOf(keys, keys.length * 2);
          }
          keys[size++] = ScanLayout.deviceLong(key, value);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return new EqualitySet(id, Arrays.copyOf(keys, size), hasNull);
  }
}

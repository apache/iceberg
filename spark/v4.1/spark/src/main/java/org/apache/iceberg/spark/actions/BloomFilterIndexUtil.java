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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal utilities for building and consulting a Bloom-filter-based file-skipping index stored in
 * Puffin statistics files.
 *
 * <p>This is a proof-of-concept implementation intended to demonstrate that Bloom filters can
 * improve query performance by pruning data files. It is intentionally limited in scope:
 *
 * <ul>
 *   <li>Only equality predicates on a single column are supported.
 *   <li>Only un-nested, primitive columns are supported.
 *   <li>Bloom filters are built per data file using Spark and stored in a single statistics file
 *       per snapshot.
 *   <li>The index is best-effort: if anything looks inconsistent or unsupported, callers should
 *       ignore it and fall back to normal planning.
 * </ul>
 */
public class BloomFilterIndexUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BloomFilterIndexUtil.class);

  // Blob type used for Bloom filters inside Puffin statistics files.
  // Kept package-private so both actions and scan-side code can share it.
  static final String BLOOM_FILTER_BLOB_TYPE = "bloom-filter-v1";

  // Property keys on each Bloom blob
  static final String PROP_DATA_FILE = "data-file";
  static final String PROP_COLUMN_NAME = "column-name";
  static final String PROP_FPP = "fpp";
  static final String PROP_NUM_VALUES = "num-values";
  static final String PROP_NUM_BITS = "num-bits";
  static final String PROP_NUM_HASHES = "num-hashes";

  // Heuristic Bloom filter sizing for the POC
  private static final double DEFAULT_FPP = 0.01;
  private static final long DEFAULT_EXPECTED_VALUES_PER_FILE = 100_000L;

  private BloomFilterIndexUtil() {}

  /**
   * Build Bloom-filter blobs for a single column of a snapshot and return them in memory.
   *
   * <p>This uses Spark to read the table at a given snapshot, groups by input file, and collects
   * values for the target column on the driver to build per-file Bloom filters. It is suitable for
   * small/medium demo tables, not for production-scale index building.
   */
  static List<Blob> buildBloomBlobsForColumn(
      SparkSession spark, Table table, Snapshot snapshot, String columnName) {
    Preconditions.checkNotNull(snapshot, "snapshot must not be null");
    Preconditions.checkArgument(
        columnName != null && !columnName.isEmpty(), "columnName must not be null/empty");

    Dataset<Row> df = SparkTableUtil.loadTable(spark, table, snapshot.snapshotId());

    // Attach per-row file path using Spark's built-in function. This relies on the underlying
    // reader exposing file information, which is already true for Iceberg's Spark integration.
    Column fileCol = functions.input_file_name().alias("_file");
    Dataset<Row> fileAndValue =
        df.select(functions.col(columnName), fileCol).na().drop(); // drop nulls for simplicity

    Dataset<Row> perFileValues =
        fileAndValue.groupBy("_file").agg(functions.collect_list(columnName).alias("values"));

    List<Row> rows = perFileValues.collectAsList();
    LOG.info(
        "Building Bloom-filter blobs for column {} in table {} (snapshot {}, {} file group(s))",
        columnName,
        table.name(),
        snapshot.snapshotId(),
        rows.size());

    Schema schema = table.schemas().get(snapshot.schemaId());
    Types.NestedField field = schema.findField(columnName);
    Preconditions.checkArgument(
        field != null, "Cannot find column %s in schema %s", columnName, schema);
    Preconditions.checkArgument(
        supportedFieldType(field.type()),
        "Unsupported Bloom index column type %s for column %s (supported: string, int, long, uuid)",
        field.type(),
        columnName);

    List<Blob> blobs = Lists.newArrayListWithExpectedSize(rows.size());

    for (Row row : rows) {
      String filePath = row.getString(0);
      @SuppressWarnings("unchecked")
      List<Object> values = row.getList(1);

      if (values == null || values.isEmpty()) {
        continue;
      }

      SimpleBloomFilter bloom =
          SimpleBloomFilter.create(
              (int) Math.max(DEFAULT_EXPECTED_VALUES_PER_FILE, values.size()), DEFAULT_FPP);

      long nonNullCount = 0L;
      for (Object value : values) {
        if (value != null) {
          byte[] canonicalBytes = canonicalBytes(value);
          Preconditions.checkArgument(
              canonicalBytes != null,
              "Unsupported Bloom index value type %s for column %s",
              value.getClass().getName(),
              columnName);
          bloom.put(canonicalBytes);
          nonNullCount++;
        }
      }

      if (nonNullCount == 0L) {
        continue;
      }

      ByteBuffer serialized = serializeBloomBits(bloom);

      Map<String, String> properties =
          ImmutableMap.of(
              PROP_DATA_FILE,
              filePath,
              PROP_COLUMN_NAME,
              columnName,
              PROP_FPP,
              String.valueOf(DEFAULT_FPP),
              PROP_NUM_VALUES,
              String.valueOf(nonNullCount),
              PROP_NUM_BITS,
              String.valueOf(bloom.numBits()),
              PROP_NUM_HASHES,
              String.valueOf(bloom.numHashFunctions()));

      Blob blob =
          new Blob(
              BLOOM_FILTER_BLOB_TYPE,
              ImmutableList.of(field.fieldId()),
              snapshot.snapshotId(),
              snapshot.sequenceNumber(),
              serialized,
              PuffinCompressionCodec.ZSTD,
              properties);

      blobs.add(blob);
    }

    return blobs;
  }

  private static ByteBuffer serializeBloomBits(SimpleBloomFilter bloom) {
    return ByteBuffer.wrap(bloom.toBitsetBytes());
  }

  private static byte[] toByteArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  /**
   * Use Bloom-filter blobs stored in statistics files to prune data files for an equality predicate
   * on a single column.
   *
   * <p>The caller is responsible for:
   *
   * <ul>
   *   <li>Ensuring the predicate is an equality of a supported literal type.
   *   <li>Passing the snapshot used for planning (so we can ignore stale stats).
   *   <li>Falling back cleanly if this method returns the original tasks.
   * </ul>
   *
   * @param table the Iceberg table
   * @param snapshot the snapshot being scanned (may be null)
   * @param tasksSupplier supplier that returns the already planned tasks (typically calls
   *     super.tasks())
   * @param columnName the column name used in the equality predicate
   * @param literalValue the literal value used in the equality predicate
   * @return either the original tasks or a filtered subset if Bloom pruning was applied
   */
  public static <T extends PartitionScanTask> List<T> pruneTasksWithBloomIndex(
      Table table,
      Snapshot snapshot,
      Supplier<List<T>> tasksSupplier,
      String columnName,
      Object literalValue) {

    if (snapshot == null) {
      return tasksSupplier.get();
    }

    List<StatisticsFile> statsFilesForSnapshot =
        table.statisticsFiles().stream()
            .filter(sf -> sf.snapshotId() == snapshot.snapshotId())
            .collect(Collectors.toList());

    if (statsFilesForSnapshot.isEmpty()) {
      return tasksSupplier.get();
    }

    byte[] literalBytes = canonicalBytes(literalValue);
    if (literalBytes == null) {
      // Unsupported literal type for this portable encoding; do not prune.
      return tasksSupplier.get();
    }
    String columnNameLower = columnName.toLowerCase(Locale.ROOT);

    Set<String> candidateFiles =
        loadCandidateFilesFromBloom(table, statsFilesForSnapshot, columnNameLower, literalBytes);

    if (candidateFiles == null) {
      // Index missing/unusable; do not change planning.
      return tasksSupplier.get();
    }

    if (candidateFiles.isEmpty()) {
      // Bloom filters have no false negatives. If the index is usable but no files matched, we can
      // safely prune to an empty scan without planning tasks.
      return ImmutableList.of();
    }

    List<T> tasks = tasksSupplier.get();
    List<T> filtered =
        tasks.stream()
            .filter(
                task -> {
                  if (task instanceof ContentScanTask) {
                    ContentScanTask<?> contentTask = (ContentScanTask<?>) task;
                    String path = contentTask.file().path().toString();
                    return candidateFiles.contains(path);
                  }
                  // If we don't know how to interpret the task, keep it for safety.
                  return true;
                })
            .collect(Collectors.toList());

    if (filtered.size() == tasks.size()) {
      // No pruning happened; return the original list to avoid surprising equals/hashCode behavior.
      return tasks;
    }

    LOG.info(
        "Bloom index pruned {} of {} task(s) for table {} on column {} = {}",
        tasks.size() - filtered.size(),
        tasks.size(),
        table.name(),
        columnName,
        literalValue);

    return filtered;
  }

  private static Set<String> loadCandidateFilesFromBloom(
      Table table, List<StatisticsFile> statsFiles, String columnNameLower, byte[] literalBytes) {

    Set<String> candidateFiles = Sets.newHashSet();
    boolean indexFound = false;
    boolean indexUsable = false;

    for (StatisticsFile stats : statsFiles) {
      InputFile inputFile = table.io().newInputFile(stats.path());

      try (PuffinReader reader =
          Puffin.read(inputFile).withFileSize(stats.fileSizeInBytes()).build()) {
        FileMetadata fileMetadata = reader.fileMetadata();

        List<BlobMetadata> bloomBlobs =
            fileMetadata.blobs().stream()
                .filter(
                    bm ->
                        BLOOM_FILTER_BLOB_TYPE.equals(bm.type())
                            && columnMatches(bm, table, columnNameLower))
                .collect(Collectors.toList());

        if (bloomBlobs.isEmpty()) {
          continue;
        }

        indexFound = true;
        Iterable<Pair<BlobMetadata, ByteBuffer>> blobData = reader.readAll(bloomBlobs);

        for (Pair<BlobMetadata, ByteBuffer> pair : blobData) {
          BlobMetadata bm = pair.first();
          ByteBuffer data = pair.second();

          String dataFile = bm.properties().get(PROP_DATA_FILE);
          if (dataFile == null) {
            continue;
          }

          Integer numBits = parsePositiveInt(bm.properties().get(PROP_NUM_BITS));
          Integer numHashes = parsePositiveInt(bm.properties().get(PROP_NUM_HASHES));
          if (numBits == null || numHashes == null) {
            continue;
          }

          indexUsable = true;
          SimpleBloomFilter bloom =
              SimpleBloomFilter.fromBitsetBytes(numBits, numHashes, toByteArray(data.duplicate()));
          if (bloom.mightContain(literalBytes)) {
            candidateFiles.add(dataFile);
          }
        }

      } catch (Exception e) {
        LOG.warn(
            "Failed to read Bloom index from statistics file {} for table {}, skipping it",
            stats.path(),
            table.name(),
            e);
      }
    }

    if (!indexFound || !indexUsable) {
      return null;
    }

    return candidateFiles;
  }

  private static Integer parsePositiveInt(String value) {
    if (value == null) {
      return null;
    }

    try {
      int parsed = Integer.parseInt(value);
      return parsed > 0 ? parsed : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean columnMatches(BlobMetadata bm, Table table, String columnNameLower) {
    List<Integer> fields = bm.inputFields();
    if (fields == null || fields.isEmpty()) {
      return false;
    }

    int fieldId = fields.get(0);
    String colName = table.schema().findColumnName(fieldId);
    return colName != null && colName.toLowerCase(Locale.ROOT).equals(columnNameLower);
  }

  private static boolean supportedFieldType(Type type) {
    return type.typeId() == Type.TypeID.STRING
        || type.typeId() == Type.TypeID.INTEGER
        || type.typeId() == Type.TypeID.LONG
        || type.typeId() == Type.TypeID.UUID;
  }

  /**
   * Canonical bytes for Phase 1 portable encoding:
   *
   * <ul>
   *   <li>string: UTF-8 bytes
   *   <li>int: 4 bytes two's complement big-endian
   *   <li>long: 8 bytes two's complement big-endian
   *   <li>uuid: 16 bytes (MSB 8 bytes big-endian + LSB 8 bytes big-endian)
   * </ul>
   *
   * <p>Returns null if the type is unsupported.
   */
  private static byte[] canonicalBytes(Object value) {
    if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    }

    // Spark may use UTF8String in some paths; treat it as a string value for this encoding.
    if (value instanceof org.apache.spark.unsafe.types.UTF8String) {
      return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    if (value instanceof Integer) {
      int intValue = (Integer) value;
      return new byte[] {
        (byte) (intValue >>> 24), (byte) (intValue >>> 16), (byte) (intValue >>> 8), (byte) intValue
      };
    }

    if (value instanceof Long) {
      long longValue = (Long) value;
      return new byte[] {
        (byte) (longValue >>> 56),
        (byte) (longValue >>> 48),
        (byte) (longValue >>> 40),
        (byte) (longValue >>> 32),
        (byte) (longValue >>> 24),
        (byte) (longValue >>> 16),
        (byte) (longValue >>> 8),
        (byte) longValue
      };
    }

    if (value instanceof UUID) {
      UUID uuid = (UUID) value;
      long msb = uuid.getMostSignificantBits();
      long lsb = uuid.getLeastSignificantBits();
      return new byte[] {
        (byte) (msb >>> 56),
        (byte) (msb >>> 48),
        (byte) (msb >>> 40),
        (byte) (msb >>> 32),
        (byte) (msb >>> 24),
        (byte) (msb >>> 16),
        (byte) (msb >>> 8),
        (byte) msb,
        (byte) (lsb >>> 56),
        (byte) (lsb >>> 48),
        (byte) (lsb >>> 40),
        (byte) (lsb >>> 32),
        (byte) (lsb >>> 24),
        (byte) (lsb >>> 16),
        (byte) (lsb >>> 8),
        (byte) lsb
      };
    }

    return null;
  }

  /**
   * Minimal Bloom filter implementation for the POC using Murmur3 x64 128-bit and standard
   * double-hashing to derive multiple hash functions.
   */
  private static final class SimpleBloomFilter {
    private final int numBits;
    private final int numHashFunctions;
    private final BitSet bits;

    private SimpleBloomFilter(int numBits, int numHashFunctions) {
      this.numBits = numBits;
      this.numHashFunctions = numHashFunctions;
      this.bits = new BitSet(numBits);
    }

    private SimpleBloomFilter(int numBits, int numHashFunctions, BitSet bits) {
      this.numBits = numBits;
      this.numHashFunctions = numHashFunctions;
      this.bits = bits;
    }

    static SimpleBloomFilter create(int expectedInsertions, double fpp) {
      int numBits = Math.max(8 * expectedInsertions, 1); // very rough heuristic, 8 bits/value min
      int numHashFunctions =
          Math.max(2, (int) Math.round(-Math.log(fpp) / Math.log(2))); // ~ ln(1/fpp)/ln(2)
      return new SimpleBloomFilter(numBits, numHashFunctions);
    }

    static SimpleBloomFilter fromBitsetBytes(
        int numBits, int numHashFunctions, byte[] bitsetBytes) {
      int requiredBytes = (numBits + 7) / 8;
      Preconditions.checkArgument(
          bitsetBytes.length == requiredBytes,
          "Invalid Bloom bitset length: expected %s bytes, got %s bytes",
          requiredBytes,
          bitsetBytes.length);
      BitSet bits = BitSet.valueOf(bitsetBytes);
      return new SimpleBloomFilter(numBits, numHashFunctions, bits);
    }

    int numBits() {
      return numBits;
    }

    int numHashFunctions() {
      return numHashFunctions;
    }

    byte[] toBitsetBytes() {
      int requiredBytes = (numBits + 7) / 8;
      byte[] bytes = new byte[requiredBytes];
      byte[] encoded = bits.toByteArray(); // bit 0 is LSB of byte 0
      System.arraycopy(encoded, 0, bytes, 0, Math.min(encoded.length, bytes.length));
      return bytes;
    }

    void put(byte[] valueBytes) {
      long[] hashes = murmur3x64_128(valueBytes);
      long hash1 = hashes[0];
      long hash2 = hashes[1];
      for (int i = 0; i < numHashFunctions; i++) {
        long combined = hash1 + (long) i * hash2;
        int index = (int) Long.remainderUnsigned(combined, (long) numBits);
        bits.set(index);
      }
    }

    boolean mightContain(byte[] valueBytes) {
      long[] hashes = murmur3x64_128(valueBytes);
      long hash1 = hashes[0];
      long hash2 = hashes[1];
      for (int i = 0; i < numHashFunctions; i++) {
        long combined = hash1 + (long) i * hash2;
        int index = (int) Long.remainderUnsigned(combined, (long) numBits);
        if (!bits.get(index)) {
          return false;
        }
      }
      return true;
    }

    // MurmurHash3 x64 128-bit, seed=0.
    // See https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
    private static long[] murmur3x64_128(byte[] data) {
      final int length = data.length;
      final int nblocks = length / 16;

      long h1 = 0L;
      long h2 = 0L;

      final long c1 = 0x87c37b91114253d5L;
      final long c2 = 0x4cf5ad432745937fL;

      // body
      for (int i = 0; i < nblocks; i++) {
        int offset = i * 16;
        long k1 = getLittleEndianLong(data, offset);
        long k2 = getLittleEndianLong(data, offset + 8);

        k1 *= c1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = Long.rotateLeft(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729;

        k2 *= c2;
        k2 = Long.rotateLeft(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = Long.rotateLeft(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5;
      }

      // tail
      long k1 = 0L;
      long k2 = 0L;
      int tailStart = nblocks * 16;
      switch (length & 15) {
        case 15:
          k2 ^= ((long) data[tailStart + 14] & 0xff) << 48;
          // fall through
        case 14:
          k2 ^= ((long) data[tailStart + 13] & 0xff) << 40;
          // fall through
        case 13:
          k2 ^= ((long) data[tailStart + 12] & 0xff) << 32;
          // fall through
        case 12:
          k2 ^= ((long) data[tailStart + 11] & 0xff) << 24;
          // fall through
        case 11:
          k2 ^= ((long) data[tailStart + 10] & 0xff) << 16;
          // fall through
        case 10:
          k2 ^= ((long) data[tailStart + 9] & 0xff) << 8;
          // fall through
        case 9:
          k2 ^= ((long) data[tailStart + 8] & 0xff);
          k2 *= c2;
          k2 = Long.rotateLeft(k2, 33);
          k2 *= c1;
          h2 ^= k2;
          // fall through
        case 8:
          k1 ^= ((long) data[tailStart + 7] & 0xff) << 56;
          // fall through
        case 7:
          k1 ^= ((long) data[tailStart + 6] & 0xff) << 48;
          // fall through
        case 6:
          k1 ^= ((long) data[tailStart + 5] & 0xff) << 40;
          // fall through
        case 5:
          k1 ^= ((long) data[tailStart + 4] & 0xff) << 32;
          // fall through
        case 4:
          k1 ^= ((long) data[tailStart + 3] & 0xff) << 24;
          // fall through
        case 3:
          k1 ^= ((long) data[tailStart + 2] & 0xff) << 16;
          // fall through
        case 2:
          k1 ^= ((long) data[tailStart + 1] & 0xff) << 8;
          // fall through
        case 1:
          k1 ^= ((long) data[tailStart] & 0xff);
          k1 *= c1;
          k1 = Long.rotateLeft(k1, 31);
          k1 *= c2;
          h1 ^= k1;
          // fall through
        default:
          // no tail
      }

      // finalization
      h1 ^= length;
      h2 ^= length;

      h1 += h2;
      h2 += h1;

      h1 = fmix64(h1);
      h2 = fmix64(h2);

      h1 += h2;
      h2 += h1;

      return new long[] {h1, h2};
    }

    private static long getLittleEndianLong(byte[] data, int offset) {
      return ((long) data[offset] & 0xff)
          | (((long) data[offset + 1] & 0xff) << 8)
          | (((long) data[offset + 2] & 0xff) << 16)
          | (((long) data[offset + 3] & 0xff) << 24)
          | (((long) data[offset + 4] & 0xff) << 32)
          | (((long) data[offset + 5] & 0xff) << 40)
          | (((long) data[offset + 6] & 0xff) << 48)
          | (((long) data[offset + 7] & 0xff) << 56);
    }

    private static long fmix64(long value) {
      long result = value;
      result ^= result >>> 33;
      result *= 0xff51afd7ed558ccdL;
      result ^= result >>> 33;
      result *= 0xc4ceb9fe1a85ec53L;
      result ^= result >>> 33;
      return result;
    }
  }
}

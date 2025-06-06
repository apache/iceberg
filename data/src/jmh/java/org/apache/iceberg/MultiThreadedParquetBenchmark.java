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

import static org.apache.iceberg.data.FileAccessFactoryRegistry.MULTI_THREADED;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.data.FileAccessFactoryRegistry;
import org.apache.iceberg.data.GenericObjectModels;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Fork(value = 1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public class MultiThreadedParquetBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedParquetBenchmark.class);

  private static final int SEED = -2;
  private static final int BATCH_SIZE = 10000;
  private static final int DATA_SIZE = 100_000_000;
  private static final String TEST_DIR =
      "/Users/petervary/iceberg-generic-parquet-reader-benchmark/";
  private static final String READ_DIR = "read/";
  private static final String WRITE_DIR = "write/";
  private static final String SOURCE = "source/data.parquet";
  private Schema testSchema;
  private List<List<Integer>> familyIds;
  private int testDataSize;

  @Param({"100", "1000", "10000"})
  private int columns;

  @Param({"0", "1", "2", "5", "10"})
  private int families;

  @Param({"true", "false"})
  private boolean multiThreaded;

  {
    // Only delete the write directory to avoid deleting the read/source directory and losing the
    // pregenerated test records.
    delete(WRITE_DIR);
    Path readDirPath = Path.of(TEST_DIR, READ_DIR);
    Path writeDirPath = Path.of(TEST_DIR, WRITE_DIR);
    try {
      if (!java.nio.file.Files.exists(readDirPath)) {
        java.nio.file.Files.createDirectories(readDirPath);
      }
      if (!java.nio.file.Files.exists(writeDirPath)) {
        java.nio.file.Files.createDirectories(writeDirPath);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create directories", e);
    }
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    System.err.println("Run: " + columns + ", F: " + families + ", MT: " + multiThreaded);
    List<Types.NestedField> fieldList = Lists.newArrayListWithCapacity(columns);
    familyIds = Lists.newArrayListWithCapacity(families);
    for (int i = 0; i < families; ++i) {
      familyIds.add(Lists.newArrayList());
    }

    // Generate the column families and the schema.
    int family = 0;
    for (int i = 0; i < columns; ++i) {
      fieldList.add(optional(i, "col" + i, Types.DoubleType.get()));
      if (families > 0) {
        List<Integer> familyIdsForColumn = familyIds.get(family);
        if (familyIdsForColumn == null) {
          familyIdsForColumn = Lists.newArrayList();
          familyIds.add(familyIdsForColumn);
        }

        familyIdsForColumn.add(i);
        family = (family + 1) % families;
      }
    }

    testSchema = new Schema(fieldList);
    testDataSize = DATA_SIZE / columns;

    initSourceRecords();
    initReaderRecords();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    // To keep the generated files to speed up the tests, we do not delete the files here.
    //   delete(WRITE_DIR);
    //   delete(READ_DIR);
  }

  private static int counter = 0;

  @Benchmark
  @Threads(1)
  public void write() throws IOException {
    write(WRITE_DIR + counter++ + "_write_" + multiThreaded + "_");
  }

  @Benchmark
  @Threads(1)
  public void read() throws IOException {
    long val = 0;

    if (families == 0) {
      if (multiThreaded) {
        // no test here
        return;
      }

      String file = readFileName(0);
      try (CloseableIterable<Record> reader =
          Parquet.read(Files.localInput(file))
              .project(testSchema)
              .createReaderFunc(
                  fileSchema -> GenericParquetReaders.buildReader(testSchema, fileSchema))
              .build()) {
        for (Record record : reader) {
          // access something to ensure the compiler doesn't optimize this away
          if (record.get(0) != null) {
            val ^= ((Double) record.get(0)).longValue();
          }
        }
      }
    } else {
      try (CloseableIterable<Record> reader = reader()) {
        for (Record record : reader) {
          // access something to ensure the compiler doesn't optimize this away
          if (record.get(0) != null) {
            val ^= ((Double) record.get(0)).longValue();
          }
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  private void write(String prefix) throws IOException {
    long val = 0;
    if (families == 0) {
      if (multiThreaded) {
        // no test here
        return;
      }

      String file = TEST_DIR + prefix + columns + "_" + families;
      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(file))
              .schema(testSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        CloseableIterator<Record> iterator = testData().iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          // access something to ensure the compiler doesn't optimize this away
          writer.add(record);
          if (record.get(0) != null) {
            val ^= ((Double) record.get(0)).longValue();
          }
        }
      }
    } else {
      try (FileAppender<Record> writer = writer(prefix)) {
        CloseableIterator<Record> iterator = testData().iterator();
        while (iterator.hasNext()) {
          Record record = iterator.next();
          // access something to ensure the compiler doesn't optimize this away
          writer.add(record);
          if (record.get(0) != null) {
            val ^= ((Double) record.get(0)).longValue();
          }
        }
      }
    }

    LOG.info("XOR val: {}", val);
  }

  private String readFileName(int family) {
    return TEST_DIR + READ_DIR + columns + "_" + (families < 2 ? "0" : families + "_" + family);
  }

  private CloseableIterable<Record> testData() {
    String file = TEST_DIR + SOURCE + "_" + columns + "_" + testDataSize;
    CloseableIterable<Record> iterator =
        Parquet.read(Files.localInput(file))
            .project(testSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(testSchema, fileSchema))
            .build();
    return CloseableIterable.combine(
        () -> new LimitedIterator(iterator.iterator(), testDataSize), iterator);
  }

  private CloseableIterable<Record> reader() {
    List<Pair<InputFile, Integer[]>> files = Lists.newArrayListWithCapacity(familyIds.size());
    for (int i = 0; i < familyIds.size(); ++i) {
      String file = readFileName(i);
      files.add(Pair.of(Files.localInput(file), familyIds.get(i).toArray(new Integer[] {})));
    }

    ReadBuilder<?, Record> builder =
        FileAccessFactoryRegistry.readBuilder(
            FileFormat.PARQUET,
            GenericObjectModels.GENERIC_OBJECT_MODEL,
            files.toArray(new Pair[] {}));
    return builder.project(testSchema).build();
  }

  private FileAppender<Record> writer(String prefix) throws IOException {
    List<Pair<EncryptedOutputFile, Integer[]>> files =
        Lists.newArrayListWithCapacity(familyIds.size());
    for (int i = 0; i < familyIds.size(); ++i) {
      String file = TEST_DIR + prefix + columns + "_" + families + "_" + i;
      files.add(
          Pair.of(
              EncryptionUtil.plainAsEncryptedOutput(Files.localOutput(file)),
              familyIds.get(i).toArray(new Integer[] {})));
    }

    WriteBuilder<?, ?, Record> builder =
        FileAccessFactoryRegistry.writeBuilder(
            FileFormat.PARQUET,
            GenericObjectModels.GENERIC_OBJECT_MODEL,
            files.toArray(new Pair[] {}));
    if (multiThreaded) {
      builder = builder.set(MULTI_THREADED, "true");
    }

    return builder.fileSchema(testSchema).build();
  }

  private void delete(String path) {
    Path pathToBeDeleted = Path.of(TEST_DIR, path);
    try (Stream<Path> paths = java.nio.file.Files.walk(pathToBeDeleted)) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (Exception e) {
      // Ignore exceptions during deletion
    }
  }

  private void initSourceRecords() throws IOException {
    String file = TEST_DIR + SOURCE + "_" + columns + "_" + testDataSize;
    if (!Files.localInput(file).exists()) {
      System.err.println("New writer source file: " + file);
      try (FileAppender<Record> writer =
          Parquet.write(Files.localOutput(file))
              .schema(testSchema)
              .createWriterFunc(GenericParquetWriter::create)
              .build()) {
        for (int i = 0; i < testDataSize; i += BATCH_SIZE) {
          writer.addAll(RandomGenericData.generate(testSchema, BATCH_SIZE, SEED + i));
          System.err.println("Status: " + i);
        }
      }
      System.err.println("New writer source file created: " + file);
    } else {
      System.err.println("Writer source file already exists: " + file);
    }
  }

  private void initReaderRecords() throws IOException {
    String file1 = readFileName(0);
    if (!Files.localInput(file1).exists()) {
      System.err.println("Generating new file for readers: " + file1);
      write(READ_DIR);
    }
  }

  private static class LimitedIterator implements CloseableIterator<Record> {
    private final CloseableIterator<Record> iterator;
    private int remaining;

    public LimitedIterator(CloseableIterator<Record> iterator, int limit) {
      this.iterator = iterator;
      this.remaining = limit;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0 && iterator.hasNext();
    }

    @Override
    public Record next() {
      if (remaining <= 0) {
        throw new IllegalStateException("No more elements available");
      }

      remaining--;
      return iterator.next();
    }

    @Override
    public void close() throws IOException {
      iterator.close();
    }
  }
}

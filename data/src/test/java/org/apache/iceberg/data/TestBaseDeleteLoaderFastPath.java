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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.PositionDeleteIndexReader;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.CharSequenceMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Verifies that {@link BaseDeleteLoader} consults {@link FormatModelRegistry} for a registered
 * {@link PositionDeleteIndexReader} before falling back to the per-row record reader.
 */
public class TestBaseDeleteLoaderFastPath {

  private static final String DATA_FILE_A = "s3://bucket/data/file-a.parquet";

  @TempDir private Path tempDir;

  @BeforeEach
  @AfterEach
  void clearRegisteredFastReaders() {
    // The default classpath for iceberg-data tests does not include iceberg-arrow, so no reader
    // is registered automatically; clear any test-installed reader so each test starts clean.
    // The accessor is package-private @VisibleForTesting; reach it reflectively so we don't need
    // to widen its visibility just to drive cross-package tests.
    readersMap().remove(FileFormat.PARQUET);
  }

  @SuppressWarnings("unchecked")
  private static Map<FileFormat, PositionDeleteIndexReader> readersMap() {
    try {
      Method method = FormatModelRegistry.class.getDeclaredMethod("positionDeleteIndexReaders");
      method.setAccessible(true);
      return (Map<FileFormat, PositionDeleteIndexReader>) method.invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot access FormatModelRegistry.positionDeleteIndexReaders()", e);
    }
  }

  @Test
  void delegatesToRegisteredFastReader() throws IOException {
    File rawFile = writePositionDeleteFile("fast-path.parquet");
    DeleteFile metadata = parquetPositionDeleteMetadata(rawFile, 3L);

    AtomicInteger invocations = new AtomicInteger();
    PositionDeleteIndexReader stub =
        new PositionDeleteIndexReader() {
          @Override
          public PositionDeleteIndex read(
              InputFile file, CharSequence dataLocation, DeleteFile deleteFile) {
            invocations.incrementAndGet();
            assertThat(dataLocation).as("dataLocation forwarded").isEqualTo(DATA_FILE_A);
            PositionDeleteIndex index = PositionDeleteIndex.create(deleteFile);
            index.delete(42L);
            return index;
          }

          @Override
          public CharSequenceMap<PositionDeleteIndex> readAll(
              InputFile file, DeleteFile deleteFile) {
            throw new UnsupportedOperationException("readAll not used in this test");
          }
        };

    FormatModelRegistry.registerPositionDeleteIndexReader(FileFormat.PARQUET, stub);

    BaseDeleteLoader loader = new BaseDeleteLoader(deleteFile -> Files.localInput(rawFile));

    PositionDeleteIndex result =
        loader.loadPositionDeletes(ImmutableList.of(metadata), DATA_FILE_A);

    assertThat(invocations).as("loader should invoke the registered fast reader").hasValue(1);
    assertThat(result.cardinality())
        .as("loader should surface the index produced by the fast reader")
        .isEqualTo(1L);
    assertThat(result.isDeleted(42L)).as("position from the stub must be present").isTrue();
  }

  @Test
  void fallsBackToRecordReaderWhenNoFastReaderRegistered() throws IOException {
    File rawFile = writePositionDeleteFile("fallback.parquet");
    DeleteFile metadata = parquetPositionDeleteMetadata(rawFile, 3L);

    BaseDeleteLoader loader = new BaseDeleteLoader(deleteFile -> Files.localInput(rawFile));

    PositionDeleteIndex result =
        loader.loadPositionDeletes(ImmutableList.of(metadata), DATA_FILE_A);

    assertThat(result.cardinality())
        .as("fallback must read the same positions written to the file")
        .isEqualTo(3L);
    assertThat(result.isDeleted(1L)).as("position 1 should be deleted").isTrue();
    assertThat(result.isDeleted(2L)).as("position 2 should be deleted").isTrue();
    assertThat(result.isDeleted(3L)).as("position 3 should be deleted").isTrue();
  }

  @Test
  void delegatesToRegisteredFastReaderReadAllWhenCachingEnabled() throws IOException {
    // When canCache() is true the loader goes through getOrReadPosDeletes -> readPosDeletes
    // (without filePath), which dispatches to PositionDeleteIndexReader#readAll. This is the
    // path Spark's CachingDeleteLoader uses; a regression in readAll wiring would otherwise
    // be invisible at the loader layer (the existing delegatesToRegisteredFastReader stub
    // throws from readAll).
    File rawFile = writePositionDeleteFile("readall-caching.parquet");
    DeleteFile metadata = parquetPositionDeleteMetadata(rawFile, 3L);

    AtomicInteger readAllInvocations = new AtomicInteger();
    AtomicInteger readInvocations = new AtomicInteger();
    PositionDeleteIndexReader stub =
        new PositionDeleteIndexReader() {
          @Override
          public PositionDeleteIndex read(
              InputFile file, CharSequence dataLocation, DeleteFile deleteFile) {
            readInvocations.incrementAndGet();
            throw new UnsupportedOperationException("filtered read should not be called");
          }

          @Override
          public CharSequenceMap<PositionDeleteIndex> readAll(
              InputFile file, DeleteFile deleteFile) {
            readAllInvocations.incrementAndGet();
            CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
            PositionDeleteIndex index = PositionDeleteIndex.create(deleteFile);
            index.delete(7L);
            index.delete(8L);
            indexes.put(DATA_FILE_A, index);
            return indexes;
          }
        };

    FormatModelRegistry.registerPositionDeleteIndexReader(FileFormat.PARQUET, stub);

    BaseDeleteLoader loader =
        new BaseDeleteLoader(deleteFile -> Files.localInput(rawFile)) {
          @Override
          protected boolean canCache(long size) {
            return true;
          }

          @Override
          protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
            // Caching is exercised by canCache() returning true; the actual cache backing
            // is not under test, so we just invoke the supplier once and return the value.
            return valueSupplier.get();
          }
        };

    PositionDeleteIndex result =
        loader.loadPositionDeletes(ImmutableList.of(metadata), DATA_FILE_A);

    assertThat(readAllInvocations).as("caching path must dispatch to readAll").hasValue(1);
    assertThat(readInvocations)
        .as("filtered read must not be called on the caching path")
        .hasValue(0);
    assertThat(result.cardinality()).as("returned cardinality").isEqualTo(2L);
    assertThat(result.isDeleted(7L)).as("position 7 should be deleted").isTrue();
    assertThat(result.isDeleted(8L)).as("position 8 should be deleted").isTrue();
  }

  private File writePositionDeleteFile(String name) throws IOException {
    File file = tempDir.resolve(name).toFile();
    OutputFile out = Files.localOutput(file);
    PositionDelete<Void> pd = PositionDelete.create();
    try (PositionDeleteWriter<Void> writer =
        Parquet.writeDeletes(out)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter()) {
      pd.set(DATA_FILE_A, 1L, null);
      writer.write(pd);
      pd.set(DATA_FILE_A, 2L, null);
      writer.write(pd);
      pd.set(DATA_FILE_A, 3L, null);
      writer.write(pd);
    }
    return file;
  }

  private static DeleteFile parquetPositionDeleteMetadata(File rawFile, long recordCount) {
    DeleteFile metadata = Mockito.mock(DeleteFile.class);
    Mockito.when(metadata.format()).thenReturn(FileFormat.PARQUET);
    Mockito.when(metadata.content()).thenReturn(FileContent.POSITION_DELETES);
    Mockito.when(metadata.location()).thenReturn(rawFile.getAbsolutePath());
    Mockito.when(metadata.recordCount()).thenReturn(recordCount);
    return metadata;
  }
}

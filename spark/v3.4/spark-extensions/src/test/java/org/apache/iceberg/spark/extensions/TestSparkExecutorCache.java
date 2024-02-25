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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.storage.memory.MemoryStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class TestSparkExecutorCache extends SparkExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            CatalogProperties.FILE_IO_IMPL,
            CustomFileIO.class.getName(),
            "default-namespace",
            "default")
      },
    };
  }

  private static final String UPDATES_VIEW_NAME = "updates";
  private static final AtomicInteger JOB_COUNTER = new AtomicInteger();
  private static final Map<String, CustomInputFile> INPUT_FILES =
      Collections.synchronizedMap(Maps.newHashMap());

  private String targetTableName;
  private TableIdentifier targetTableIdent;

  public TestSparkExecutorCache(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void configureTargetTableName() {
    String name = "target_exec_cache_" + JOB_COUNTER.incrementAndGet();
    this.targetTableName = tableName(name);
    this.targetTableIdent = TableIdentifier.of(Namespace.of("default"), name);
  }

  @After
  public void releaseResources() {
    sql("DROP TABLE IF EXISTS %s", targetTableName);
    sql("DROP TABLE IF EXISTS %s", UPDATES_VIEW_NAME);
    INPUT_FILES.clear();
  }

  @Test
  public void testCopyOnWriteDelete() throws Exception {
    checkDelete(COPY_ON_WRITE);
  }

  @Test
  public void testMergeOnReadDelete() throws Exception {
    checkDelete(MERGE_ON_READ);
  }

  private void checkDelete(RowLevelOperationMode mode) throws Exception {
    List<DeleteFile> deleteFiles = createAndInitTable(TableProperties.DELETE_MODE, mode);

    sql("DELETE FROM %s WHERE id = 1 OR id = 4", targetTableName);

    // there are 2 data files and 2 delete files that apply to both of them
    // in CoW, the target table will be scanned 2 times (main query + runtime filter)
    // the runtime filter may invalidate the cache so check at least some requests were hits
    // in MoR, the target table will be scanned only once
    // so each delete file must be opened once
    int maxRequestCount = mode == COPY_ON_WRITE ? 3 : 1;
    assertThat(deleteFiles).allMatch(deleteFile -> streamCount(deleteFile) <= maxRequestCount);

    // verify the final set of records is correct
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id ASC", targetTableName));
  }

  @Test
  public void testCopyOnWriteUpdate() throws Exception {
    checkUpdate(COPY_ON_WRITE);
  }

  @Test
  public void testMergeOnReadUpdate() throws Exception {
    checkUpdate(MERGE_ON_READ);
  }

  private void checkUpdate(RowLevelOperationMode mode) throws Exception {
    List<DeleteFile> deleteFiles = createAndInitTable(TableProperties.UPDATE_MODE, mode);

    Dataset<Integer> updateDS = spark.createDataset(ImmutableList.of(1, 4), Encoders.INT());
    updateDS.createOrReplaceTempView(UPDATES_VIEW_NAME);

    sql("UPDATE %s SET id = -1 WHERE id IN (SELECT * FROM %s)", targetTableName, UPDATES_VIEW_NAME);

    // there are 2 data files and 2 delete files that apply to both of them
    // in CoW, the target table will be scanned 3 times (2 in main query + runtime filter)
    // the runtime filter may invalidate the cache so check at least some requests were hits
    // in MoR, the target table will be scanned only once
    // so each delete file must be opened once
    int maxRequestCount = mode == COPY_ON_WRITE ? 5 : 1;
    assertThat(deleteFiles).allMatch(deleteFile -> streamCount(deleteFile) <= maxRequestCount);

    // verify the final set of records is correct
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hr"), row(-1, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC", targetTableName));
  }

  @Test
  public void testCopyOnWriteMerge() throws Exception {
    checkMerge(COPY_ON_WRITE);
  }

  @Test
  public void testMergeOnReadMerge() throws Exception {
    checkMerge(MERGE_ON_READ);
  }

  private void checkMerge(RowLevelOperationMode mode) throws Exception {
    List<DeleteFile> deleteFiles = createAndInitTable(TableProperties.MERGE_MODE, mode);

    Dataset<Integer> updateDS = spark.createDataset(ImmutableList.of(1, 4), Encoders.INT());
    updateDS.createOrReplaceTempView(UPDATES_VIEW_NAME);

    sql(
        "MERGE INTO %s t USING %s s "
            + "ON t.id == s.value "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET id = 100 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (id, dep) VALUES (-1, 'unknown')",
        targetTableName, UPDATES_VIEW_NAME);

    // there are 2 data files and 2 delete files that apply to both of them
    // in CoW, the target table will be scanned 2 times (main query + runtime filter)
    // the runtime filter may invalidate the cache so check at least some requests were hits
    // in MoR, the target table will be scanned only once
    // so each delete file must be opened once
    int maxRequestCount = mode == COPY_ON_WRITE ? 3 : 1;
    assertThat(deleteFiles).allMatch(deleteFile -> streamCount(deleteFile) <= maxRequestCount);

    // verify the final set of records is correct
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(100, "hr"), row(100, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC", targetTableName));
  }

  private int streamCount(DeleteFile deleteFile) {
    CustomInputFile inputFile = INPUT_FILES.get(deleteFile.path().toString());
    return inputFile.streamCount();
  }

  private List<DeleteFile> createAndInitTable(String operation, RowLevelOperationMode mode)
      throws Exception {
    sql(
        "CREATE TABLE %s (id INT, dep STRING) "
            + "USING iceberg "
            + "TBLPROPERTIES ('%s' '%s', '%s' '%s', '%s' '%s')",
        targetTableName,
        TableProperties.WRITE_METADATA_LOCATION,
        temp.toString().replaceFirst("file:", ""),
        TableProperties.WRITE_DATA_LOCATION,
        temp.toString().replaceFirst("file:", ""),
        operation,
        mode.modeName());

    append(targetTableName, new Employee(0, "hr"), new Employee(1, "hr"), new Employee(2, "hr"));
    append(targetTableName, new Employee(3, "hr"), new Employee(4, "hr"), new Employee(5, "hr"));

    Table table = validationCatalog.loadTable(targetTableIdent);

    List<Pair<CharSequence, Long>> posDeletes =
        dataFiles(table).stream()
            .map(dataFile -> Pair.of(dataFile.path(), 0L))
            .collect(Collectors.toList());
    Pair<DeleteFile, CharSequenceSet> posDeleteResult = writePosDeletes(table, posDeletes);
    DeleteFile posDeleteFile = posDeleteResult.first();
    CharSequenceSet referencedDataFiles = posDeleteResult.second();

    DeleteFile eqDeleteFile = writeEqDeletes(table, "id", 2, 5);

    table
        .newRowDelta()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .validateDataFilesExist(referencedDataFiles)
        .addDeletes(posDeleteFile)
        .addDeletes(eqDeleteFile)
        .commit();

    sql("REFRESH TABLE %s", targetTableName);

    // invalidate the memory store to destroy all currently live table broadcasts
    SparkEnv sparkEnv = SparkEnv.get();
    MemoryStore memoryStore = sparkEnv.blockManager().memoryStore();
    memoryStore.clear();

    return ImmutableList.of(posDeleteFile, eqDeleteFile);
  }

  private DeleteFile writeEqDeletes(Table table, String col, Object... values) throws IOException {
    Schema deleteSchema = table.schema().select(col);

    Record delete = GenericRecord.create(deleteSchema);
    List<Record> deletes = Lists.newArrayList();
    for (Object value : values) {
      deletes.add(delete.copy(col, value));
    }

    OutputFile out = Files.localOutput(temp.newFile("eq-deletes-" + UUID.randomUUID()));
    return FileHelpers.writeDeleteFile(table, out, null, deletes, deleteSchema);
  }

  private Pair<DeleteFile, CharSequenceSet> writePosDeletes(
      Table table, List<Pair<CharSequence, Long>> deletes) throws IOException {
    OutputFile out = Files.localOutput(temp.newFile("pos-deletes-" + UUID.randomUUID()));
    return FileHelpers.writeDeleteFile(table, out, null, deletes);
  }

  private void append(String target, Employee... employees) throws NoSuchTableException {
    List<Employee> input = Arrays.asList(employees);
    Dataset<Row> inputDF = spark.createDataFrame(input, Employee.class);
    inputDF.coalesce(1).writeTo(target).append();
  }

  private Collection<DataFile> dataFiles(Table table) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return ImmutableList.copyOf(Iterables.transform(tasks, ContentScanTask::file));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static class CustomFileIO implements FileIO {

    public CustomFileIO() {}

    @Override
    public InputFile newInputFile(String path) {
      return INPUT_FILES.computeIfAbsent(path, key -> new CustomInputFile(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      File file = new File(path);
      if (!file.delete()) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    }
  }

  public static class CustomInputFile implements InputFile {
    private final InputFile delegate;
    private final AtomicInteger streamCount;

    public CustomInputFile(String path) {
      this.delegate = Files.localInput(path);
      this.streamCount = new AtomicInteger();
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      streamCount.incrementAndGet();
      return delegate.newStream();
    }

    public int streamCount() {
      return streamCount.get();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }
}

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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.flink.MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class OperatorTestBase {
  private static final int NUMBER_TASK_MANAGERS = 1;
  private static final int SLOTS_PER_TASK_MANAGER = 8;
  private static final Schema SCHEMA_WITH_PRIMARY_KEY =
      new Schema(
          Lists.newArrayList(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get())),
          ImmutableMap.of(),
          ImmutableSet.of(SimpleDataUtil.SCHEMA.columns().get(0).fieldId()));

  protected static final String UID_SUFFIX = "UID-Dummy";
  protected static final String SLOT_SHARING_GROUP = "SlotSharingGroup";
  protected static final TriggerLockFactory LOCK_FACTORY = new MemoryLockFactory();

  public static final String IGNORED_OPERATOR_NAME = "Ignore";

  static final long EVENT_TIME = 10L;
  static final long EVENT_TIME_2 = 11L;
  static final Watermark WATERMARK = new Watermark(EVENT_TIME);
  protected static final String DUMMY_TASK_NAME = "dummyTask";
  protected static final String DUMMY_TABLE_NAME = "dummyTable";

  static final String FILE_NAME_1 = "fileName1";
  static final String FILE_NAME_2 = "fileName2";
  static final Watermark WATERMARK_2 = new Watermark(EVENT_TIME_2);

  @RegisterExtension
  protected static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
              .setConfiguration(config())
              .build());

  @TempDir private Path warehouseDir;

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(TestFixtures.DATABASE, TestFixtures.TABLE);

  @BeforeEach
  void before() {
    LOCK_FACTORY.open();
    LOCK_FACTORY.createLock().unlock();
    LOCK_FACTORY.createRecoveryLock().unlock();
    MetricsReporterFactoryForTests.reset();
  }

  @AfterEach
  void after() throws IOException {
    LOCK_FACTORY.close();
  }

  protected static Table createTable() {
    // only test V2 tables as compaction doesn't support V3 with row lineage
    return createTable("2");
  }

  protected static Table createTable(String formatVersion) {
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SimpleDataUtil.SCHEMA,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                formatVersion,
                "flink.max-continuous-empty-commits",
                "100000"));
  }

  protected static Table createTableWithDelete() {
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SCHEMA_WITH_PRIMARY_KEY,
            PartitionSpec.unpartitioned(),
            null,
            ImmutableMap.of("format-version", "2", "write.upsert.enabled", "true"));
  }

  protected static Table createPartitionedTable() {
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TestFixtures.TABLE_IDENTIFIER,
            SimpleDataUtil.SCHEMA,
            PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build(),
            null,
            ImmutableMap.of("format-version", "2", "flink.max-continuous-empty-commits", "100000"));
  }

  protected void insert(Table table, Integer id, String data) throws IOException {
    new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
        .appendToTable(Lists.newArrayList(SimpleDataUtil.createRecord(id, data)));
    table.refresh();
  }

  protected void insert(Table table, Integer id, String data, String extra) throws IOException {
    new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
        .appendToTable(Lists.newArrayList(SimpleDataUtil.createRecord(id, data, extra)));
    table.refresh();
  }

  /**
   * For the same identifier column id this methods simulate the following row operations: <tr>
   * <li>add an equality delete on oldData
   * <li>insert newData </tr>
   *
   * @param table to modify
   * @param id the identifier column id
   * @param oldData the old data to be deleted
   * @param newData the new data to be inserted
   */
  protected void update(Table table, Integer id, String oldData, String newData)
      throws IOException {
    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
            .writeFile(Lists.newArrayList(SimpleDataUtil.createRecord(id, newData)));
    DeleteFile eqDelete = writeEqualityDelete(table, id, oldData);

    table.newRowDelta().addRows(dataFile).addDeletes(eqDelete).commit();
  }

  /**
   * For the same identifier column id this methods simulate the following row operations: <tr>
   * <li>add an equality delete on oldData
   * <li>insert tempData
   * <li>add a position delete on tempData
   * <li>insert newData </tr>
   *
   * @param table to modify
   * @param id the identifier column id
   * @param oldData the old data to be deleted
   * @param tempData the temp data to be inserted and deleted with a position delete
   * @param newData the new data to be inserted
   */
  protected void update(Table table, Integer id, String oldData, String tempData, String newData)
      throws IOException {
    DataFile dataFile =
        new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
            .writeFile(
                Lists.newArrayList(
                    SimpleDataUtil.createRecord(id, tempData),
                    SimpleDataUtil.createRecord(id, newData)));
    DeleteFile eqDelete = writeEqualityDelete(table, id, oldData);
    DeleteFile posDelete = writePosDelete(table, dataFile.path(), 0, id, tempData);

    table.newRowDelta().addRows(dataFile).addDeletes(eqDelete).addDeletes(posDelete).commit();
  }

  protected void insertPartitioned(Table table, Integer id, String data) throws IOException {
    new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
        .appendToTable(
            TestHelpers.Row.of(data), Lists.newArrayList(SimpleDataUtil.createRecord(id, data)));
    table.refresh();
  }

  protected void insertFullPartitioned(Table table, Integer id, String data) throws IOException {
    new GenericAppenderHelper(table, FileFormat.PARQUET, warehouseDir)
        .appendToTable(
            TestHelpers.Row.of(data, id),
            Lists.newArrayList(SimpleDataUtil.createRecord(id, data)));
    table.refresh();
  }

  protected void dropTable() {
    CATALOG_EXTENSION.catalogLoader().loadCatalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
  }

  protected TableLoader tableLoader() {
    return CATALOG_EXTENSION.tableLoader();
  }

  /**
   * Close the {@link JobClient} and wait for the job closure. If the savepointDir is specified, it
   * stops the job with a savepoint.
   *
   * @param jobClient the job to close
   * @param savepointDir the savepointDir to store the last savepoint. If <code>null</code> then
   *     stop without a savepoint.
   * @return configuration for restarting the job from the savepoint
   */
  protected static Configuration closeJobClient(JobClient jobClient, File savepointDir) {
    Configuration conf = new Configuration();
    if (jobClient != null) {
      if (savepointDir != null) {
        // Stop with savepoint
        jobClient.stopWithSavepoint(false, savepointDir.getPath(), SavepointFormatType.CANONICAL);
        // Wait until the savepoint is created and the job has been stopped
        Awaitility.await().until(() -> savepointDir.listFiles(File::isDirectory).length == 1);
        conf.set(
            SavepointConfigOptions.SAVEPOINT_PATH,
            savepointDir.listFiles(File::isDirectory)[0].getAbsolutePath());
      } else {
        jobClient.cancel();
      }

      // Wait until the job has been stopped
      Awaitility.await().until(() -> jobClient.getJobStatus().get().isTerminalState());
      return conf;
    }

    return null;
  }

  /**
   * Close the {@link JobClient} and wait for the job closure.
   *
   * @param jobClient the job to close
   */
  protected static void closeJobClient(JobClient jobClient) {
    closeJobClient(jobClient, null);
  }

  protected static void checkUidsAreSet(StreamExecutionEnvironment env, String uidSuffix) {
    env.getTransformations().stream()
        .filter(
            t -> !(t instanceof SinkTransformation) && !(t.getName().equals(IGNORED_OPERATOR_NAME)))
        .forEach(
            transformation -> {
              assertThat(transformation.getUid()).isNotNull();
              if (uidSuffix != null) {
                assertThat(transformation.getUid()).contains(UID_SUFFIX);
              }
            });
  }

  protected static void checkSlotSharingGroupsAreSet(StreamExecutionEnvironment env, String name) {
    String nameToCheck = name != null ? name : StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP;

    env.getTransformations().stream()
        .filter(
            t -> !(t instanceof SinkTransformation) && !(t.getName().equals(IGNORED_OPERATOR_NAME)))
        .forEach(
            t -> {
              assertThat(t.getSlotSharingGroup()).isPresent();
              assertThat(t.getSlotSharingGroup().get().getName()).isEqualTo(nameToCheck);
            });
  }

  private static Configuration config() {
    Configuration config = new Configuration(DISABLE_CLASSLOADER_CHECK_CONFIG);
    MetricOptions.forReporter(config, "test_reporter")
        .set(MetricOptions.REPORTER_FACTORY_CLASS, MetricsReporterFactoryForTests.class.getName());
    return config;
  }

  private DeleteFile writeEqualityDelete(Table table, Integer id, String oldData)
      throws IOException {
    File file = File.createTempFile("junit", null, warehouseDir.toFile());
    assertThat(file.delete()).isTrue();
    return FileHelpers.writeDeleteFile(
        table,
        Files.localOutput(file),
        new PartitionData(PartitionSpec.unpartitioned().partitionType()),
        Lists.newArrayList(SimpleDataUtil.createRecord(id, oldData)),
        SCHEMA_WITH_PRIMARY_KEY);
  }

  private DeleteFile writePosDelete(
      Table table, CharSequence path, Integer pos, Integer id, String oldData) throws IOException {
    File file = File.createTempFile("junit", null, warehouseDir.toFile());
    assertThat(file.delete()).isTrue();
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(table.schema());
    nested.set(0, id);
    nested.set(1, oldData);
    posDelete.set(path, pos, nested);
    return FileHelpers.writePosDeleteFile(
        table, Files.localOutput(file), null, Lists.newArrayList(posDelete));
  }

  static void trigger(OneInputStreamOperatorTestHarness<Trigger, ?> harness) throws Exception {
    long time = System.currentTimeMillis();
    harness.processElement(Trigger.create(time, 0), time);
  }

  private static class MemoryLock implements TriggerLockFactory.Lock {
    volatile boolean locked = false;

    @Override
    public boolean tryLock() {
      if (locked) {
        return false;
      } else {
        locked = true;
        return true;
      }
    }

    @Override
    public boolean isHeld() {
      return locked;
    }

    @Override
    public void unlock() {
      locked = false;
    }
  }

  private static class MemoryLockFactory implements TriggerLockFactory {
    private static final TriggerLockFactory.Lock MAINTENANCE_LOCK = new MemoryLock();
    private static final TriggerLockFactory.Lock RECOVERY_LOCK = new MemoryLock();

    @Override
    public void open() {
      // do nothing
    }

    @Override
    public Lock createLock() {
      return MAINTENANCE_LOCK;
    }

    @Override
    public Lock createRecoveryLock() {
      return RECOVERY_LOCK;
    }

    @Override
    public void close() {
      // do nothing
    }
  }
}

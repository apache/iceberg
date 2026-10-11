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

import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestNonIcebergRowLevelOperations {

  private static final String CATALOG_NAME = "mock_non_iceberg";
  private static final String TABLE_NAME = "t";
  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty())
          });

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog." + CATALOG_NAME, MockRowLevelCatalog.class.getName())
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testUpdateOnNonIcebergTableDoesNotThrowMatchError() {
    assertThatCode(
            () ->
                spark.sql(
                    "UPDATE "
                        + CATALOG_NAME
                        + "."
                        + TABLE_NAME
                        + " SET data = 'updated' WHERE id = 1"))
        .doesNotThrowAnyException();
  }

  /** Minimal {@link TableCatalog} whose tables are never Iceberg {@code SparkTable}s. */
  public static class MockRowLevelCatalog implements TableCatalog {

    private final MockRowLevelTable table = new MockRowLevelTable();

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {}

    @Override
    public String name() {
      return CATALOG_NAME;
    }

    @Override
    public Identifier[] listTables(String[] namespace) {
      return new Identifier[] {Identifier.of(namespace, TABLE_NAME)};
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
      return table;
    }

    @Override
    public Table createTable(
        Identifier ident,
        StructType schema,
        Transform[] partitions,
        Map<String, String> properties) {
      throw new UnsupportedOperationException("createTable is not supported");
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
      throw new UnsupportedOperationException("alterTable is not supported");
    }

    @Override
    public boolean dropTable(Identifier ident) {
      throw new UnsupportedOperationException("dropTable is not supported");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) throws NoSuchTableException {
      throw new UnsupportedOperationException("renameTable is not supported");
    }
  }

  /** A non-Iceberg {@link Table} that supports row-level operations. */
  public static class MockRowLevelTable implements Table, SupportsRowLevelOperations {

    @Override
    public String name() {
      return CATALOG_NAME + "." + TABLE_NAME;
    }

    @Override
    public StructType schema() {
      return SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return EnumSet.of(
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.OVERWRITE_DYNAMIC);
    }

    @Override
    public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
      return new MockRowLevelOperationBuilder(info.command());
    }
  }

  private static class MockScanBuilder implements ScanBuilder {
    @Override
    public Scan build() {
      return new MockScan();
    }
  }

  private static class MockScan implements Scan {
    @Override
    public StructType readSchema() {
      return SCHEMA;
    }

    @Override
    public Batch toBatch() {
      return new MockBatch();
    }
  }

  private static class MockBatch implements Batch {
    @Override
    public InputPartition[] planInputPartitions() {
      return new InputPartition[0];
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new MockPartitionReaderFactory();
    }
  }

  private static class MockPartitionReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      return new MockPartitionReader();
    }
  }

  private static class MockPartitionReader implements PartitionReader<InternalRow> {
    @Override
    public boolean next() {
      return false;
    }

    @Override
    public InternalRow get() {
      return null;
    }

    @Override
    public void close() {}
  }

  private static class MockRowLevelOperationBuilder implements RowLevelOperationBuilder {
    private final RowLevelOperation.Command command;

    MockRowLevelOperationBuilder(RowLevelOperation.Command command) {
      this.command = command;
    }

    @Override
    public RowLevelOperation build() {
      return new MockRowLevelOperation(command);
    }
  }

  private static class MockRowLevelOperation implements RowLevelOperation {
    private final RowLevelOperation.Command command;

    MockRowLevelOperation(RowLevelOperation.Command command) {
      this.command = command;
    }

    @Override
    public RowLevelOperation.Command command() {
      return command;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return new MockScanBuilder();
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      return new MockWriteBuilder();
    }
  }

  private static class MockWriteBuilder implements WriteBuilder {
    @Override
    public BatchWrite buildForBatch() {
      return new MockBatchWrite();
    }
  }

  private static class MockBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new MockDataWriterFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {}

    @Override
    public void abort(WriterCommitMessage[] messages) {}
  }

  private static class MockDataWriterFactory implements DataWriterFactory {
    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new MockDataWriter();
    }
  }

  private static class MockDataWriter implements DataWriter<InternalRow> {
    @Override
    public void write(InternalRow record) {}

    @Override
    public WriterCommitMessage commit() {
      return null;
    }

    @Override
    public void abort() {}

    @Override
    public void close() {}
  }
}

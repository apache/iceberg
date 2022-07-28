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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousIcebergEnumerator;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlanner;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlannerImpl;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.reader.ReaderMetricsContext;
import org.apache.iceberg.flink.source.reader.RowDataReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumeratorState> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSource.class);

  private final TableLoader tableLoader;
  private final ScanContext scanContext;
  private final ReaderFunction<T> readerFunction;
  private final SplitAssignerFactory assignerFactory;

  IcebergSource(
      TableLoader tableLoader,
      ScanContext scanContext,
      ReaderFunction<T> readerFunction,
      SplitAssignerFactory assignerFactory) {
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
    this.readerFunction = readerFunction;
    this.assignerFactory = assignerFactory;
  }

  private static Table loadTable(TableLoader tableLoader) {
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      return loader.loadTable();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close table loader", e);
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return scanContext.isStreaming() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<T, IcebergSourceSplit> createReader(SourceReaderContext readerContext) {
    ReaderMetricsContext readerMetrics = new ReaderMetricsContext(readerContext.metricGroup());
    return new IcebergSourceReader<>(readerFunction, readerContext, readerMetrics);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumeratorState enumState) {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
    return IcebergEnumeratorStateSerializer.INSTANCE;
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumeratorState enumState) {
    Table table = loadTable(tableLoader);
    SplitAssigner assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
    } else {
      LOG.info(
          "Iceberg source restored {} splits from state for table {}",
          enumState.pendingSplits().size(),
          table.name());
      assigner = assignerFactory.createAssigner(enumState.pendingSplits());
    }

    if (scanContext.isStreaming()) {
      // Ideally, operatorId should be used as the threadPoolName as Flink guarantees its uniqueness
      // within a job.
      // SplitEnumeratorContext doesn't expose the OperatorCoordinator.Context, which would contain
      // the OperatorID.
      // Need to discuss with Flink community whether it is ok to expose a public API like the
      // protected method
      // "OperatorCoordinator.Context getCoordinatorContext()" from SourceCoordinatorContext
      // implementation.
      // For now, <table name>-<random UUID> is used as the unique thread pool name.
      ContinuousSplitPlanner splitPlanner =
          new ContinuousSplitPlannerImpl(
              table, scanContext, table.name() + "-" + UUID.randomUUID());
      return new ContinuousIcebergEnumerator(
          enumContext, assigner, scanContext, splitPlanner, enumState);
    } else {
      return new StaticIcebergEnumerator(enumContext, assigner, table, scanContext, enumState);
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static Builder<RowData> forRowData() {
    return new Builder<>();
  }

  public static class Builder<T> {

    // required
    private TableLoader tableLoader;
    private SplitAssignerFactory splitAssignerFactory;
    private ReaderFunction<T> readerFunction;
    private ReadableConfig flinkConfig = new Configuration();

    // optional
    private final ScanContext.Builder contextBuilder = ScanContext.builder();

    Builder() {}

    public Builder<T> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T> assignerFactory(SplitAssignerFactory assignerFactory) {
      this.splitAssignerFactory = assignerFactory;
      return this;
    }

    public Builder<T> readerFunction(ReaderFunction<T> newReaderFunction) {
      this.readerFunction = newReaderFunction;
      return this;
    }

    public Builder<T> flinkConfig(ReadableConfig config) {
      this.flinkConfig = config;
      return this;
    }

    public Builder caseSensitive(boolean newCaseSensitive) {
      this.contextBuilder.caseSensitive(newCaseSensitive);
      return this;
    }

    public Builder useSnapshotId(Long newSnapshotId) {
      this.contextBuilder.useSnapshotId(newSnapshotId);
      return this;
    }

    public Builder streamingStartingStrategy(StreamingStartingStrategy newStartingStrategy) {
      this.contextBuilder.startingStrategy(newStartingStrategy);
      return this;
    }

    public Builder startSnapshotTimestamp(Long newStartSnapshotTimestamp) {
      this.contextBuilder.startSnapshotTimestamp(newStartSnapshotTimestamp);
      return this;
    }

    public Builder startSnapshotId(Long newStartSnapshotId) {
      this.contextBuilder.startSnapshotId(newStartSnapshotId);
      return this;
    }

    public Builder endSnapshotId(Long newEndSnapshotId) {
      this.contextBuilder.endSnapshotId(newEndSnapshotId);
      return this;
    }

    public Builder asOfTimestamp(Long newAsOfTimestamp) {
      this.contextBuilder.asOfTimestamp(newAsOfTimestamp);
      return this;
    }

    public Builder splitSize(Long newSplitSize) {
      this.contextBuilder.splitSize(newSplitSize);
      return this;
    }

    public Builder splitLookback(Integer newSplitLookback) {
      this.contextBuilder.splitLookback(newSplitLookback);
      return this;
    }

    public Builder splitOpenFileCost(Long newSplitOpenFileCost) {
      this.contextBuilder.splitOpenFileCost(newSplitOpenFileCost);
      return this;
    }

    public Builder streaming(boolean streaming) {
      this.contextBuilder.streaming(streaming);
      return this;
    }

    public Builder monitorInterval(Duration newMonitorInterval) {
      this.contextBuilder.monitorInterval(newMonitorInterval);
      return this;
    }

    public Builder nameMapping(String newNameMapping) {
      this.contextBuilder.nameMapping(newNameMapping);
      return this;
    }

    public Builder project(Schema newProjectedSchema) {
      this.contextBuilder.project(newProjectedSchema);
      return this;
    }

    public Builder filters(List<Expression> newFilters) {
      this.contextBuilder.filters(newFilters);
      return this;
    }

    public Builder limit(long newLimit) {
      this.contextBuilder.limit(newLimit);
      return this;
    }

    public Builder includeColumnStats(boolean newIncludeColumnStats) {
      this.contextBuilder.includeColumnStats(newIncludeColumnStats);
      return this;
    }

    public Builder planParallelism(int planParallelism) {
      this.contextBuilder.planParallelism(planParallelism);
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      contextBuilder.fromProperties(properties);
      return this;
    }

    public IcebergSource<T> build() {
      ScanContext context = contextBuilder.build();
      if (readerFunction == null) {
        try (TableLoader loader = tableLoader) {
          loader.open();
          Table table = tableLoader.loadTable();
          RowDataReaderFunction rowDataReaderFunction =
              new RowDataReaderFunction(
                  flinkConfig,
                  table.schema(),
                  context.project(),
                  context.nameMapping(),
                  context.caseSensitive(),
                  table.io(),
                  table.encryption());
          this.readerFunction = (ReaderFunction<T>) rowDataReaderFunction;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      checkRequired();
      return new IcebergSource<T>(tableLoader, context, readerFunction, splitAssignerFactory);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
      Preconditions.checkNotNull(splitAssignerFactory, "assignerFactory is required.");
      Preconditions.checkNotNull(readerFunction, "readerFunction is required.");
    }
  }
}

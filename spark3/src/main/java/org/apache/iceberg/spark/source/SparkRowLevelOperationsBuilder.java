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

package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsPushDownRowLevelFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import static org.apache.iceberg.TableProperties.ROW_LEVEL_OPS_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.ROW_LEVEL_OPS_ISOLATION_LEVEL_DEFAULT;

class SparkRowLevelOperationsBuilder implements SupportsPushDownRowLevelFilters {

  private final SparkSession spark;
  private final Table table;
  private final StructType dsSchema;
  private final CaseInsensitiveStringMap options;
  private final boolean caseSensitive;

  private List<Expression> filterExpressions = null;
  private Filter[] pushedFilters = null;

  // lazy variables
  private Long readSnapshotId = null;
  private List<FileScanTask> fileScanTasks;
  private JavaSparkContext lazySparkContext = null;
  private Broadcast<FileIO> io = null;
  private Broadcast<EncryptionManager> encryption = null;

  SparkRowLevelOperationsBuilder(SparkSession spark, Table table, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.dsSchema = info.schema();
    this.options = info.options();
    this.caseSensitive = Boolean.parseBoolean(spark.conf().get("spark.sql.caseSensitive"));
  }

  @Override
  public void pushFilters(Filter[] filters) {
    List<Expression> pushedExpressions = Lists.newArrayListWithExpectedSize(filters.length);
    List<Filter> pushed = Lists.newArrayListWithExpectedSize(filters.length);

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(table.schema().asStruct(), expr, caseSensitive);
          pushedExpressions.add(expr);
          pushed.add(filter);
        } catch (ValidationException e) {
          // TODO: add warning
          // binding to the table schema failed, so this expression cannot be pushed down
        }
      }
    }

    this.filterExpressions = pushedExpressions;
    this.pushedFilters = pushed.toArray(new Filter[0]);
  }

  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public Scan buildScan() {
    return new SparkFileBatchScan(table, files(), io(), encryption(), caseSensitive, options);
  }

  @Override
  public BatchWrite buildBatchWrite() {
    List<DataFile> overwrittenFiles = files().stream().map(FileScanTask::file).collect(Collectors.toList());

    String appId = spark.sparkContext().applicationId();
    String wapId = spark.conf().get("spark.wap.id", null);

    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);

    // TODO: rework isolation handling
    String isolationLevel = table.properties().getOrDefault(
        ROW_LEVEL_OPS_ISOLATION_LEVEL,
        ROW_LEVEL_OPS_ISOLATION_LEVEL_DEFAULT
    );
    if (isolationLevel.equals("serializable")) {
      return new OverwriteFilesBatchWrite(
          table, io(), encryption(), options, appId, wapId, writeSchema,
          dsSchema, overwrittenFiles, readSnapshotId, filterExpression());
    } else {
      return new OverwriteFilesBatchWrite(
          table, io(), encryption(), options, appId, wapId, writeSchema,
          dsSchema, overwrittenFiles);
    }
  }

  // TODO: concurrency?
  private List<FileScanTask> files() {
    if (fileScanTasks == null) {
      this.readSnapshotId = table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;

      TableScan scan = table.newScan().filter(filterExpression()).ignoreResiduals();

      if (readSnapshotId != null) {
        scan = scan.useSnapshot(readSnapshotId);

        try (CloseableIterable<FileScanTask> fileScanTasksIterable = scan.planFiles()) {
          this.fileScanTasks = Lists.newArrayList(fileScanTasksIterable);
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
        }
      } else {
        fileScanTasks = Collections.emptyList();
      }

    }
    return fileScanTasks;
  }

  private Expression filterExpression() {
    if (filterExpressions != null) {
      return filterExpressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }
    return Expressions.alwaysTrue();
  }

  private JavaSparkContext lazySparkContext() {
    if (lazySparkContext == null) {
      this.lazySparkContext = new JavaSparkContext(spark.sparkContext());
    }
    return lazySparkContext;
  }

  private Broadcast<FileIO> io() {
    if (io == null) {
      this.io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    }
    return io;
  }

  private Broadcast<EncryptionManager> encryption() {
    if (encryption == null) {
      this.encryption = lazySparkContext().broadcast(table.encryption());
    }
    return encryption;
  }
}

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.arrow.vectorized.ArrowReader;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BucketUtil;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

/**
 * The CPU baseline: the same merge-on-read algorithm as {@link DeviceMergeOnReadScan}, on all
 * cores, with Iceberg's fastest Java reader.
 *
 * <p>That reader is the vectorized Arrow reader, and it refuses any file that has deletes ({@code
 * ArrowReader}: "Cannot read files that require applying delete files", apache/iceberg#2487). So
 * each file is handed to it without its deletes, and the deletes are applied here, from the same
 * host preparation the device scan uses: the position bitmap from {@code BaseDeleteLoader} and the
 * equality keys, probed through a primitive hash set. One data file a task, one task a thread.
 */
public final class CpuMergeOnReadScan implements MergeOnReadScan {

  private static final int BATCH_ROWS = 65536;

  private final Table table;
  private final int threads;
  private final Expression filter;
  private ScanMetrics metrics = new ScanMetrics();

  private CpuMergeOnReadScan(Table table, int threads, Expression filter) {
    this.table = table;
    this.threads = threads;
    this.filter = filter;
  }

  public static CpuMergeOnReadScan of(Table table, int threads) {
    return new CpuMergeOnReadScan(table, threads, Expressions.alwaysTrue());
  }

  /** Same contract as {@link DeviceMergeOnReadScan#of(Table, Expression)}. */
  public static CpuMergeOnReadScan of(Table table, int threads, Expression filter) {
    return new CpuMergeOnReadScan(table, threads, filter);
  }

  @Override
  public ScanMetrics metrics() {
    return metrics;
  }

  @Override
  public AggregateResult aggregate(String groupColumn, String... sumColumns) {
    return aggregate(groupColumn, 0, sumColumns);
  }

  @Override
  public AggregateResult aggregateByBucket(String column, int buckets, String... sumColumns) {
    return aggregate(column, buckets, sumColumns);
  }

  private AggregateResult aggregate(String groupColumn, int buckets, String... sumColumns) {
    AggregateResult result = new AggregateResult(sumColumns.length);
    List<String> projection = Lists.newArrayList();
    if (groupColumn != null) {
      projection.add(groupColumn);
    }
    projection.addAll(ImmutableList.copyOf(sumColumns));
    run(
        projection,
        (file, columnIndex) -> new GroupSink(groupColumn, buckets, sumColumns, columnIndex),
        sinks -> {
          synchronized (result) {
            for (GroupSink sink : sinks) {
              sink.mergeInto(result);
            }
          }
        });
    return result;
  }

  /** The consumer is called once a data file, after every file has been read. */
  @Override
  public long materialize(List<String> columns, Consumer<LiveRows> consumer) {
    return run(
        columns,
        (file, columnIndex) -> new RowSink(columnIndex, file.recordCount(), table.schema()),
        sinks -> {
          for (RowSink sink : sinks) {
            consumer.accept(sink);
          }
        });
  }

  /**
   * A visitor of the live rows of one data file, on the scan thread that reads it: the CPU
   * counterpart of a {@link GroupedStage}, so that a technique can run on both processors over the
   * same deletes and filters. Rows arrive in file order with every delete already applied.
   */
  public interface RowVisitor {
    void accept(ColumnarBatch batch, int row);
  }

  /**
   * Visits every live row of the scan: {@code perFile} makes a visitor for each data file, on the
   * thread that reads it, given each projected column's index in the batches; {@code finish} sees
   * every visitor once all files have been read. Returns the live row count.
   */
  public <V extends RowVisitor> long visit(
      List<String> columns, Function<Map<String, Integer>, V> perFile, Consumer<List<V>> finish) {
    return run(
        columns,
        (file, columnIndex) -> new VisitorSink<>(perFile.apply(columnIndex)),
        sinks -> {
          List<V> visitors = Lists.newArrayList();
          sinks.forEach(sink -> visitors.add(sink.visitor));
          finish.accept(visitors);
        });
  }

  private static final class VisitorSink<V extends RowVisitor> implements Sink {
    private final V visitor;
    private int rows = 0;

    VisitorSink(V visitor) {
      this.visitor = visitor;
    }

    @Override
    public void accept(ColumnarBatch batch, int row) {
      visitor.accept(batch, row);
      rows++;
    }

    @Override
    public int rows() {
      return rows;
    }
  }

  /**
   * {@link DeviceMergeOnReadScan#convertEqualityDeletes()} on the CPU, for comparison: the same
   * files, read the same way (key columns only, every delete applied), each file's deleted
   * positions written into one deletion vector, and the same {@code RowDelta}.
   */
  public DeviceMergeOnReadScan.ConversionResult convertEqualityDeletes() {
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    List<String> keys = Lists.newArrayList();
    Set<DeleteFile> equality = Sets.newHashSet();
    Set<DeleteFile> replaced = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      for (FileScanTask task : planned) {
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES) {
            equality.add(delete);
            for (int id : delete.equalityFieldIds()) {
              String name = table.schema().findField(id).name();
              if (!keys.contains(name)) {
                keys.add(name);
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    OutputFileFactory files =
        OutputFileFactory.builderFor(table, 1, System.nanoTime())
            .format(org.apache.iceberg.FileFormat.PUFFIN)
            .build();
    BaseDVFileWriter writer = new BaseDVFileWriter(files, path -> null);
    long[] dead = new long[1];
    int[] converted = new int[1];
    run(
        keys,
        (file, columnIndex) -> new DeadSink(file),
        sinks -> {
          // Only the files equality deletes apply to were read.
          for (DeadSink sink : sinks) {
            converted[0]++;
            for (int i = 0; i < sink.count; i++) {
              writer.delete(
                  sink.file.location(),
                  sink.positions[i],
                  table.specs().get(sink.file.specId()),
                  sink.file.partition());
            }
            dead[0] += sink.count;
          }
        },
        task -> {
          boolean hasEquality =
              task.deletes().stream()
                  .anyMatch(d -> d.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES);
          if (hasEquality) {
            task.deletes().stream()
                .filter(d -> d.content() != org.apache.iceberg.FileContent.EQUALITY_DELETES)
                .forEach(replaced::add);
          }
          return hasEquality;
        });
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    List<DeleteFile> vectors = writer.result().deleteFiles();
    org.apache.iceberg.RowDelta delta =
        table
            .newRowDelta()
            .validateFromSnapshot(snapshotId)
            .validateNoConflictingDeleteFiles()
            .validateDeletedFiles()
            .validateDataFilesExist(DeviceMergeOnReadScan.referencedDataFiles(vectors));
    equality.forEach(delta::removeDeletes);
    replaced.forEach(delta::removeDeletes);
    vectors.forEach(delta::addDeletes);
    delta.commit();
    return new DeviceMergeOnReadScan.ConversionResult(
        converted[0], equality.size(), replaced.size(), vectors.size(), dead[0]);
  }

  /** One file's deleted positions, ascending. */
  private static final class DeadSink implements Sink {
    private final DataFile file;
    private long[] positions = new long[1024];
    private int count = 0;

    DeadSink(DataFile file) {
      this.file = file;
    }

    @Override
    public void accept(ColumnarBatch batch, int row) {}

    @Override
    public void deleted(long position) {
      if (count == positions.length) {
        positions = Arrays.copyOf(positions, count * 2);
      }
      positions[count++] = position;
    }

    @Override
    public int rows() {
      return 0;
    }
  }

  /** What one thread accumulates over the live rows of one data file. */
  private interface Sink {
    void accept(ColumnarBatch batch, int row);

    /** A row of the file, by position, that a delete drops. */
    default void deleted(long position) {}

    int rows();
  }

  private interface SinkFactory<S extends Sink> {
    S create(DataFile file, Map<String, Integer> columnIndex);
  }

  private <S extends Sink> long run(
      List<String> projection, SinkFactory<S> factory, Consumer<List<S>> finish) {
    return run(projection, factory, finish, task -> true);
  }

  /** {@code include}: which planned tasks to read. */
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  private <S extends Sink> long run(
      List<String> projection,
      SinkFactory<S> factory,
      Consumer<List<S>> finish,
      Predicate<FileScanTask> include) {
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask task : planned) {
        if (include.test(task)) {
          tasks.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    Integer equalityKeyId = HostDeletes.equalityKeyId(tasks);
    List<Integer> allKeyIds = HostDeletes.equalityKeyIds(tasks);
    List<Integer> keyIds = allKeyIds.size() > 1 ? allKeyIds : List.of();
    List<String> read = Lists.newArrayList(projection);
    for (int keyId : keyIds) {
      String name = table.schema().findField(keyId).name();
      if (!read.contains(name)) {
        read.add(name);
      }
    }
    for (int fieldId : DevicePredicate.fieldIds(filter, table.schema())) {
      String name = table.schema().findField(fieldId).name();
      if (!read.contains(name)) {
        read.add(name);
      }
    }
    boolean filtered = filter.op() != Expression.Operation.TRUE;
    String keyName = null;
    if (equalityKeyId != null) {
      keyName = table.schema().findField(equalityKeyId).name();
      if (!read.contains(keyName)) {
        read.add(0, keyName);
      }
    }
    // The reader returns columns in schema order, so index them by name from its schema.
    org.apache.iceberg.Schema readSchema = table.schema().select(read);
    Map<String, Integer> columnIndex = Maps.newHashMap();
    for (int i = 0; i < readSchema.columns().size(); i++) {
      columnIndex.put(readSchema.columns().get(i).name(), i);
    }
    ScanLayout layout = new ScanLayout(table.schema(), equalityKeyId, null, read);
    HostDeletes host = new HostDeletes(table, layout, equalityKeyId, false).withKeyIds(keyIds);
    int[] keyColumns = new int[keyIds.size()];
    boolean[] keyInts = new boolean[keyIds.size()];
    for (int c = 0; c < keyColumns.length; c++) {
      Types.NestedField field = table.schema().findField(keyIds.get(c));
      keyColumns[c] = columnIndex.get(field.name());
      keyInts[c] = layout.type(field.fieldId()) == CudfType.INT32;
    }
    metrics.plan((System.nanoTime() - start) / 1e6);

    boolean keyIsInt = keyName != null && layout.type(equalityKeyId) == CudfType.INT32;
    Integer keyColumn = keyName == null ? null : columnIndex.get(keyName);
    Map<String, PrimitiveHash.LongSet> sets = Maps.newConcurrentMap();

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    long live = 0;
    try {
      List<Future<S>> futures = Lists.newArrayList();
      for (FileScanTask task : tasks) {
        futures.add(
            pool.submit(
                () -> {
                  HostDeletes.FilePrep prep = host.prepare(task);
                  byte[] bits = prep.positionBits();
                  HostDeletes.EqualitySet equality = prep.equality();
                  PrimitiveHash.LongSet set =
                      equality == null
                          ? null
                          : sets.computeIfAbsent(
                              equality.id(), ignored -> new PrimitiveHash.LongSet(equality.keys()));
                  boolean setHasNull = equality != null && equality.hasNull();
                  HostDeletes.KeySet keySet = prep.keySet();
                  long[] keyValues = new long[keyColumns.length];
                  byte[] keyNulls = new byte[keyColumns.length];
                  S sink = factory.create(task.file(), columnIndex);
                  RowFilter rowFilter =
                      filtered
                          ? new RowFilter(
                              DevicePredicate.compile(task.residual(), table.schema()),
                              table.schema(),
                              columnIndex)
                          : null;
                  try (ArrowReader reader =
                          new ArrowReader(table.newScan().select(read), BATCH_ROWS, true);
                      CloseableIterator<ColumnarBatch> batches =
                          reader.open(
                              CloseableIterable.withNoopClose(
                                  ImmutableList.<CombinedScanTask>of(
                                      new BaseCombinedScanTask(new WithoutDeletes(task)))))) {
                    long position = 0;
                    while (batches.hasNext()) {
                      ColumnarBatch batch = batches.next();
                      ColumnVector key = keyColumn == null ? null : batch.column(keyColumn);
                      int rows = batch.numRows();
                      for (int r = 0; r < rows; r++) {
                        long pos = position + r;
                        if (bits != null && ((bits[(int) (pos >> 3)] >> (pos & 7)) & 1) == 1) {
                          sink.deleted(pos);
                          continue;
                        }
                        if (set != null) {
                          if (key.isNullAt(r)) {
                            if (setHasNull) {
                              sink.deleted(pos);
                              continue;
                            }
                          } else if (set.contains(keyIsInt ? key.getInt(r) : key.getLong(r))) {
                            sink.deleted(pos);
                            continue;
                          }
                        }
                        if (keySet != null) {
                          for (int c = 0; c < keyColumns.length; c++) {
                            ColumnVector column = batch.column(keyColumns[c]);
                            keyNulls[c] = (byte) (column.isNullAt(r) ? 1 : 0);
                            keyValues[c] =
                                keyNulls[c] == 1
                                    ? 0
                                    : keyInts[c] ? column.getInt(r) : column.getLong(r);
                          }
                          if (keySet.contains(keyValues, keyNulls)) {
                            sink.deleted(pos);
                            continue;
                          }
                        }
                        if (rowFilter != null && !rowFilter.test(batch, r)) {
                          continue;
                        }
                        sink.accept(batch, r);
                      }
                      position += rows;
                    }
                  }
                  return sink;
                }));
      }
      List<S> sinks = Lists.newArrayList();
      for (Future<S> future : futures) {
        S sink = future.get();
        sinks.add(sink);
      }
      finish.accept(sinks);
      for (S sink : sinks) {
        live += sink.rows();
      }
      for (FileScanTask task : tasks) {
        metrics.file(task.file().recordCount());
      }
      metrics.live(live);
    } catch (Exception e) {
      throw new IllegalStateException("CPU scan failed", e);
    } finally {
      pool.shutdownNow();
    }
    metrics.total((System.nanoTime() - start) / 1e6);
    return live;
  }

  /**
   * The residual filter as the device evaluates it -- the same compiled terms -- read straight from
   * Arrow vectors without boxing a value per row.
   */
  private static final class RowFilter {
    private final int[] columns;
    private final int[] kinds;
    private final int[] ops;
    private final long[][] longs;
    private final double[][] doubles;

    RowFilter(
        DevicePredicate predicate, org.apache.iceberg.Schema schema, Map<String, Integer> index) {
      List<DevicePredicate.Term> terms = predicate.terms();
      this.columns = new int[terms.size()];
      this.kinds = new int[terms.size()];
      this.ops = new int[terms.size()];
      this.longs = new long[terms.size()][];
      this.doubles = new double[terms.size()][];
      for (int t = 0; t < terms.size(); t++) {
        DevicePredicate.Term term = terms.get(t);
        ops[t] = term.op();
        if (term.op() == DevicePredicate.OP_FALSE) {
          continue;
        }
        columns[t] = index.get(schema.findField(term.fieldId()).name());
        kinds[t] = term.kind().code();
        longs[t] = term.longs();
        doubles[t] = term.doubles();
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    boolean test(ColumnarBatch batch, int row) {
      for (int t = 0; t < ops.length; t++) {
        int op = ops[t];
        if (op == DevicePredicate.OP_FALSE) {
          return false;
        }
        ColumnVector vector = batch.column(columns[t]);
        boolean isNull = vector.isNullAt(row);
        if (op == DevicePredicate.OP_IS_NULL) {
          if (!isNull) {
            return false;
          }
        } else if (op == DevicePredicate.OP_NOT_NULL) {
          if (isNull) {
            return false;
          }
        } else if (isNull) {
          // Iceberg's evaluator orders nulls first, as the device kernel does.
          if (op != DevicePredicate.OP_LT
              && op != DevicePredicate.OP_LT_EQ
              && op != DevicePredicate.OP_NOT_EQ) {
            return false;
          }
        } else if (kinds[t] == CudfType.FLOAT64.code()) {
          if (!compare(vector.getDouble(row), op, doubles[t])) {
            return false;
          }
        } else {
          long value = kinds[t] == CudfType.INT64.code() ? vector.getLong(row) : vector.getInt(row);
          if (!compare(value, op, longs[t])) {
            return false;
          }
        }
      }
      return true;
    }

    private static boolean compare(double value, int op, double[] constants) {
      if (op == DevicePredicate.OP_IN) {
        for (double constant : constants) {
          if (value == constant) {
            return true;
          }
        }
        return false;
      }
      double constant = constants[0];
      return switch (op) {
        case DevicePredicate.OP_LT -> value < constant;
        case DevicePredicate.OP_LT_EQ -> value <= constant;
        case DevicePredicate.OP_GT -> value > constant;
        case DevicePredicate.OP_GT_EQ -> value >= constant;
        case DevicePredicate.OP_EQ -> value == constant;
        default -> value != constant;
      };
    }

    private static boolean compare(long value, int op, long[] constants) {
      if (op == DevicePredicate.OP_IN) {
        for (long constant : constants) {
          if (value == constant) {
            return true;
          }
        }
        return false;
      }
      long constant = constants[0];
      return switch (op) {
        case DevicePredicate.OP_LT -> value < constant;
        case DevicePredicate.OP_LT_EQ -> value <= constant;
        case DevicePredicate.OP_GT -> value > constant;
        case DevicePredicate.OP_GT_EQ -> value >= constant;
        case DevicePredicate.OP_EQ -> value == constant;
        default -> value != constant;
      };
    }
  }

  /** Grouped COUNT and SUMs over one file's live rows, with SQL's null-skipping SUM. */
  private static final class GroupSink implements Sink {
    private final Integer groupColumn;
    private final int buckets;
    private final int[] sumColumns;
    private final PrimitiveHash.IntIndex groups = new PrimitiveHash.IntIndex();
    private long[] counts = new long[64];
    private double[] sums;
    private long rows = 0;

    GroupSink(String group, int buckets, String[] sumNames, Map<String, Integer> columnIndex) {
      this.groupColumn = group == null ? null : columnIndex.get(group);
      this.buckets = buckets;
      this.sumColumns = new int[sumNames.length];
      for (int c = 0; c < sumNames.length; c++) {
        sumColumns[c] = columnIndex.get(sumNames[c]);
      }
      this.sums = new double[64 * sumNames.length];
    }

    @Override
    public void accept(ColumnarBatch batch, int row) {
      int key;
      if (groupColumn == null) {
        key = 0;
      } else if (buckets > 0) {
        long value = batch.column(groupColumn).getLong(row);
        key = (BucketUtil.hash(value) & Integer.MAX_VALUE) % buckets;
      } else {
        key = batch.column(groupColumn).getInt(row);
      }
      int groupIndex = groups.indexOf(key);
      if (groupIndex == counts.length) {
        counts = Arrays.copyOf(counts, counts.length * 2);
        sums = Arrays.copyOf(sums, sums.length * 2);
      }
      counts[groupIndex]++;
      for (int c = 0; c < sumColumns.length; c++) {
        ColumnVector vector = batch.column(sumColumns[c]);
        if (!vector.isNullAt(row)) {
          sums[groupIndex * sumColumns.length + c] += vector.getDouble(row);
        }
      }
      rows++;
    }

    void mergeInto(AggregateResult result) {
      int[] keys = groups.keysByIndex();
      double[] values = new double[sumColumns.length];
      for (int g = 0; g < keys.length; g++) {
        System.arraycopy(sums, g * sumColumns.length, values, 0, sumColumns.length);
        result.add(keys[g], counts[g], values);
      }
    }

    @Override
    public int rows() {
      return (int) rows;
    }
  }

  /** One file's live rows, copied out of Arrow into arrays, as the device scan copies them back. */
  private static final class RowSink implements Sink, LiveRows {
    private final Map<String, Integer> columnIndex;
    private final Object[] columns;
    private final boolean[][] nulls;
    private int rows = 0;

    RowSink(Map<String, Integer> columnIndex, long capacity, org.apache.iceberg.Schema schema) {
      this.columnIndex = columnIndex;
      this.columns = new Object[columnIndex.size()];
      this.nulls = new boolean[columnIndex.size()][];
      this.capacityHint = (int) capacity;
      this.types = new CudfType[columnIndex.size()];
      columnIndex.forEach(
          (name, index) -> types[index] = ScanLayout.cudfType(schema.findField(name)));
    }

    private final CudfType[] types;

    private final int capacityHint;

    @Override
    public void accept(ColumnarBatch batch, int row) {
      for (int c = 0; c < columns.length; c++) {
        ColumnVector vector = batch.column(c);
        if (columns[c] == null) {
          columns[c] = allocate(types[c], capacityHint);
          nulls[c] = new boolean[capacityHint];
        }
        boolean isNull = vector.isNullAt(row);
        nulls[c][rows] = isNull;
        if (isNull) {
          continue;
        }
        Object column = columns[c];
        if (column instanceof long[] longs) {
          longs[rows] = vector.getLong(row);
        } else if (column instanceof int[] ints) {
          ints[rows] = vector.getInt(row);
        } else {
          ((double[]) column)[rows] = vector.getDouble(row);
        }
      }
      rows++;
    }

    /** The column's array, by its Iceberg type: the Arrow vector class depends on the encoding. */
    private static Object allocate(CudfType type, int capacity) {
      return switch (type) {
        case INT64 -> new long[capacity];
        case INT32 -> new int[capacity];
        case FLOAT64, FLOAT32 -> new double[capacity];
        case DECIMAL64 ->
            throw new UnsupportedOperationException("The CPU baseline reads no decimal columns");
      };
    }

    @Override
    public int rows() {
      return rows;
    }

    @Override
    public int column(String name) {
      return columnIndex.get(name);
    }

    @Override
    public boolean isNull(int column, int row) {
      return nulls[column][row];
    }

    @Override
    public int getInt(int column, int row) {
      return ((int[]) columns[column])[row];
    }

    @Override
    public long getLong(int column, int row) {
      return ((long[]) columns[column])[row];
    }

    @Override
    public double getDouble(int column, int row) {
      return ((double[]) columns[column])[row];
    }

    @Override
    public String getString(int column, int row) {
      throw new UnsupportedOperationException("The CPU baseline reads no string columns");
    }
  }

  /** A data-file task with its delete files removed, so the vectorized reader will read it. */
  private static final class WithoutDeletes implements FileScanTask {
    private final FileScanTask task;

    WithoutDeletes(FileScanTask task) {
      this.task = task;
    }

    @Override
    public DataFile file() {
      return task.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return ImmutableList.of();
    }

    @Override
    public PartitionSpec spec() {
      return task.spec();
    }

    @Override
    public long start() {
      return task.start();
    }

    @Override
    public long length() {
      return task.length();
    }

    @Override
    public Expression residual() {
      return task.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this);
    }
  }
}

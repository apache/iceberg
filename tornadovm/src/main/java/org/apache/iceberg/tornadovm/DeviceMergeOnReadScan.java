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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.tornado.api.DataRange;
import uk.ac.manchester.tornado.api.GridScheduler;
import uk.ac.manchester.tornado.api.KernelContext;
import uk.ac.manchester.tornado.api.TaskGraph;
import uk.ac.manchester.tornado.api.TornadoExecutionPlan;
import uk.ac.manchester.tornado.api.TornadoExecutionResult;
import uk.ac.manchester.tornado.api.WorkerGrid;
import uk.ac.manchester.tornado.api.WorkerGrid1D;
import uk.ac.manchester.tornado.api.enums.DataTransferMode;
import uk.ac.manchester.tornado.api.exceptions.TornadoExecutionPlanException;
import uk.ac.manchester.tornado.api.types.arrays.ByteArray;
import uk.ac.manchester.tornado.api.types.arrays.DoubleArray;
import uk.ac.manchester.tornado.api.types.arrays.IntArray;
import uk.ac.manchester.tornado.api.types.arrays.LongArray;
import uk.ac.manchester.tornado.api.types.arrays.TornadoNativeArray;
import uk.ac.manchester.tornado.cudf.Cudf;
import uk.ac.manchester.tornado.cudf.enums.CudfAggregation;
import uk.ac.manchester.tornado.cudf.enums.CudfType;
import uk.ac.manchester.tornado.cudf.enums.ParquetColumnType;

/**
 * A merge-on-read scan of an Iceberg table on an NVIDIA GPU.
 *
 * <p>Planning stays on the CPU and is Iceberg's own: {@code planFiles()} decides which delete files
 * apply to which data file. Consecutive data files that share their equality deletes are grouped
 * into batches of up to {@code iceberg.tornadovm.batch-rows} rows, and each batch goes through one
 * TornadoVM execution plan:
 *
 * <ol>
 *   <li>cuDF decodes the projected columns of all the batch's Parquet files, in one call, straight
 *       into TornadoVM's device buffers;
 *   <li>cuDF reads the {@code pos} column of their file-scoped position-delete files, and the key
 *       column of their equality-delete files, into device buffers too;
 *   <li>cuDF probes the equality-delete set with the key column;
 *   <li>a TornadoVM kernel folds the probe, deletion vectors (packed into a bitmap on the host) and
 *       Iceberg's null semantics into one live byte a row, and a second scatters the position
 *       deletes into it;
 *   <li>the consumer: a cuDF group-by of which only the groups come back, or cuDF compaction and
 *       TornadoVM gathers after which only the live rows come back.
 * </ol>
 *
 * <p>A host pool reads footers, deletion vectors and anything the device path does not cover while
 * the device works on earlier batches. The device buffers and the compiled plan of each query shape
 * are kept by this object and reused by its later queries; {@link #close()} releases them.
 *
 * <p>Supported: local Parquet data files, INT/DATE, LONG/TIMESTAMP/TIME and DOUBLE columns
 * (required or optional), position deletes, deletion vectors, and equality deletes on one INT or
 * LONG column. Anything else is refused with {@link UnsupportedOperationException} rather than
 * answered wrongly.
 */
public final class DeviceMergeOnReadScan implements MergeOnReadScan, AutoCloseable {

  private static final int HOST_THREADS = 8;

  /** The longest string the device packs into an INT64. */
  static final int STRING_BYTES = 8;

  private static final Logger LOG = LoggerFactory.getLogger(DeviceMergeOnReadScan.class);

  static {
    TornadoStart.ensureStarted();
  }

  private static final boolean DEBUG = Boolean.getBoolean("iceberg.tornadovm.debug");

  /**
   * How equality deletes are probed: {@code sorted} (the default) sorts the delete keys on the
   * device once a set and binary-searches them from a kernel on every batch; {@code hash} uses
   * cuDF's contains, which hashes the set again on every batch.
   */
  private static final boolean SORTED_PROBE =
      !"hash".equals(System.getProperty("iceberg.tornadovm.probe", "sorted"));

  /** Characters of paths one batch may name in one holder: data files, delete files. */
  private static final int PATH_CAPACITY = 1 << 16;

  /**
   * Rows a device batch may hold. Files are grouped up to this many rows into one read, because a
   * read of one small file is mostly latency; a file larger than this is a batch on its own. The
   * bigger the read, the cheaper cuDF's ZSTD decompression of it: for 100M rows in 1.56M-row files,
   * 1, 2 and 5 files a read cost 435, 233 and 137 ms of decompression a scan, so the default fits
   * five such files.
   */
  private static final long BATCH_ROWS = Long.getLong("iceberg.tornadovm.batch-rows", 8_000_000L);

  /**
   * Batches in flight at once, each in its own region -- its own buffers, plan and CUDA stream --
   * so that one batch's file reads and host-to-device copies overlap another's decoding.
   */
  private static final int STREAMS = Integer.getInteger("iceberg.tornadovm.streams", 4);

  /**
   * The bytes of the one device buffer a batch's string columns share; tests lower it to batch
   * files apart, or to have the device refuse a file, without writing gigabytes.
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static long stringBuffer =
      Long.getLong("iceberg.tornadovm.string-buffer", Integer.MAX_VALUE);

  /**
   * Streams a rewrite of a caller's file group runs on: each holds a batch's read, gathered and
   * encoded columns at once, on the device and in cuDF's pool.
   */
  private static final int REWRITE_STREAMS =
      Integer.getInteger("iceberg.tornadovm.rewriteStreams", STREAMS);

  /** The decoded bytes a compaction batch holds, at most, on each stream. */
  private static final long REWRITE_BATCH_BYTES =
      Long.getLong("iceberg.tornadovm.rewriteBatchBytes", 768L << 20);

  private final Table table;
  private final Expression filter;
  // Concurrent: another scan's aggregate may evict a shape from it (keepShape).
  private final Map<String, List<Region>> regions = Maps.newConcurrentMap();
  private ScanMetrics metrics = new ScanMetrics();

  private DeviceMergeOnReadScan(Table table, Expression filter) {
    this.table = table;
    this.filter = filter;
  }

  public static DeviceMergeOnReadScan of(Table table) {
    return new DeviceMergeOnReadScan(table, Expressions.alwaysTrue());
  }

  /**
   * A scan of the rows that match {@code filter}. Iceberg prunes files with it while planning, and
   * the device applies each file's residual -- what statistics could not decide -- to every row.
   * See {@link DevicePredicate} for what a residual may contain.
   */
  public static DeviceMergeOnReadScan of(Table table, Expression filter) {
    return new DeviceMergeOnReadScan(table, filter);
  }

  /**
   * Drops the local copies of object-store files ({@code s3://} and the like) that scans fetched,
   * so the next scan reads from the store again -- for measuring cold reads.
   */
  public static void clearObjectStoreCache() {
    LocalFiles.clear();
  }

  /**
   * Whether this JVM can run device scans: TornadoVM with a cuDF shim that has the typed reader.
   */
  public static boolean isAvailable() {
    try {
      return Cudf.isParquetAvailable() && Cudf.isColumnsAvailable();
    } catch (LinkageError e) {
      return false;
    }
  }

  @Override
  public ScanMetrics metrics() {
    return metrics;
  }

  private volatile boolean closed;

  /** Whether {@link #close} has run: a caller sharing scans re-opens one instead of using it. */
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
    LAST_REWRITER.compareAndSet(this, null);
    regions.values().forEach(DeviceMergeOnReadScan::forget);
    regions.values().forEach(list -> list.forEach(Region::close));
    regions.clear();
    sharedPools.values().forEach(SharedPool::close);
    sharedPools.clear();
  }

  @Override
  public AggregateResult aggregate(String groupColumn, String... sumColumns) {
    return aggregate(groupColumn, 0, sumColumns);
  }

  @Override
  public AggregateResult aggregateByBucket(String column, int buckets, String... sumColumns) {
    Types.NestedField field = table.schema().findField(column);
    if (field.isOptional() || field.type().typeId() != org.apache.iceberg.types.Type.TypeID.LONG) {
      throw new UnsupportedOperationException(
          "The bucket column must be a required LONG: " + field);
    }
    if (buckets <= 0) {
      throw new IllegalArgumentException("Invalid bucket count: " + buckets);
    }
    return aggregate(column, buckets, sumColumns);
  }

  /** With {@code buckets > 0}, groups by {@code bucket(buckets, groupColumn)} of a LONG column. */
  private AggregateResult aggregate(String groupColumn, int buckets, String... sumColumns) {
    checkAggregate(groupColumn, buckets, sumColumns);
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    if (groupColumn != null && buckets == 0 && !Ablation.off("dense")) {
      // The grouped path groups a key whose footer bounds span few values without a hash table
      // (q2 on 100M rows 4-11% faster, q5 27-48%), and goes to cuDF's hash group-by otherwise.
      return byKey(groupColumn, sumColumns, start);
    }
    Planned planned = new Planned(groupColumn, ImmutableList.copyOf(sumColumns));
    // The group-by reads the first sumColumns slots of the FP64 buffer, so the sum columns have to
    // be those slots, in order; columns only the filter reads come after them.
    for (int c = 0; c < sumColumns.length; c++) {
      if (planned.layout.slot(table.schema().findField(sumColumns[c]).fieldId()) != c) {
        throw new UnsupportedOperationException("The sum columns must be distinct DOUBLE columns");
      }
    }
    String key = "aggregate:" + groupColumn + ":" + buckets + ":" + String.join(",", sumColumns);
    List<Region> streams =
        reuse(key, planned, () -> new AggregateRegion(planned, groupColumn, buckets, sumColumns));
    metrics.plan((System.nanoTime() - start) / 1e6);

    AggregateResult result = new AggregateResult(sumColumns.length);
    run(
        streams,
        planned,
        (region, executed) -> ((AggregateRegion) region).collect(executed, result));
    metrics.total((System.nanoTime() - start) / 1e6);
    return result;
  }

  private void checkAggregate(String groupColumn, int buckets, String[] sumColumns) {
    if (sumColumns.length == 0) {
      throw new IllegalArgumentException("aggregate needs at least one sum column");
    }
    if (groupColumn != null && buckets == 0) {
      Types.NestedField key = table.schema().findField(groupColumn);
      if (key.isOptional() || ScanLayout.cudfType(key) != CudfType.INT32) {
        throw new UnsupportedOperationException("The group column must be a required INT: " + key);
      }
    }
    for (String column : sumColumns) {
      if (ScanLayout.cudfType(table.schema().findField(column)) != CudfType.FLOAT64) {
        throw new UnsupportedOperationException("Sum columns must be DOUBLE: " + column);
      }
    }
  }

  private AggregateResult byKey(String groupColumn, String[] sumColumns, long start) {
    List<DeviceValue> values = Lists.newArrayListWithCapacity(sumColumns.length);
    for (String column : sumColumns) {
      values.add(DeviceValue.of(column));
    }
    GroupedAggregate grouped = aggregate(ImmutableList.of(groupColumn), values);
    AggregateResult result = new AggregateResult(sumColumns.length);
    double[] sums = new double[sumColumns.length];
    for (Map.Entry<List<Object>, double[]> group : grouped.groups().entrySet()) {
      double[] totals = group.getValue();
      System.arraycopy(totals, 0, sums, 0, sums.length);
      result.add(((Number) group.getKey().get(0)).intValue(), (long) totals[sums.length], sums);
    }
    metrics.total((System.nanoTime() - start) / 1e6);
    return result;
  }

  /**
   * Sums of {@link DeviceValue}s and the row count per group of up to four columns -- integer,
   * long, date, timestamp or string (of up to 8 bytes) -- over the live rows that pass the scan's
   * filter: the shape of TPC-H's Q1 and, with no group columns, Q6.
   *
   * <p>One kernel builds the group keys as INT64 columns (a string as its packed value), another
   * evaluates every value of every row, and cuDF groups by all keys at once
   * (Cudf.groupAggregateLongs); only the groups come back to the host.
   */
  public GroupedAggregate aggregate(List<String> groupColumns, List<DeviceValue> values) {
    return aggregate(groupColumns, values, null);
  }

  /**
   * {@link #aggregate(List, List)} with a {@link GroupedStage} (or null) running its kernels among
   * the aggregate's: the results hold the aggregate's values, then the stage's. The scan's device
   * buffers are kept per aggregate and stage shape, so later runs of a shape reuse them.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public GroupedAggregate aggregate(
      List<String> groupColumns, List<DeviceValue> values, GroupedStage stage) {
    if (groupColumns.size() > 4 || (values.isEmpty() && (stage == null || stage.values() == 0))) {
      throw new IllegalArgumentException("Up to 4 group columns and at least one value");
    }
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    List<String> projection = Lists.newArrayList(groupColumns);
    if (stage != null) {
      stage.columns().stream().filter(c -> !projection.contains(c)).forEach(projection::add);
    }
    for (String column : groupColumns) {
      org.apache.iceberg.types.Type.TypeID type = column(column).type().typeId();
      switch (type) {
        case INTEGER, LONG, DATE, TIMESTAMP, STRING, DECIMAL -> {}
        default -> throw new UnsupportedOperationException("Cannot group by " + column(column));
      }
    }
    for (DeviceValue value : values) {
      for (DeviceValue.Factor factor : value.factors()) {
        org.apache.iceberg.types.Type.TypeID type = column(factor.column()).type().typeId();
        // A decimal sums as its unscaled value.
        if (type != org.apache.iceberg.types.Type.TypeID.INTEGER
            && type != org.apache.iceberg.types.Type.TypeID.LONG
            && type != org.apache.iceberg.types.Type.TypeID.DECIMAL
            && type != org.apache.iceberg.types.Type.TypeID.DOUBLE) {
          throw new UnsupportedOperationException("Not a numeric column: " + factor.column());
        }
        if (!projection.contains(factor.column())) {
          projection.add(factor.column());
        }
      }
    }
    Planned planned = new Planned(null, projection);
    String key = GROUPED + groupColumns + ":" + values + (stage == null ? "" : ":" + stage.shape());
    List<Region> streams =
        reuse(key, planned, () -> new GroupedRegion(planned, groupColumns, values, stage));
    if (stage != null) {
      streams.forEach(region -> ((GroupedRegion) region).bind(stage));
    }
    metrics.plan((System.nanoTime() - start) / 1e6);
    GroupedAggregate result =
        new GroupedAggregate(values.size() + (stage == null ? 0 : stage.values()));
    // Another scan's aggregate, on another thread, must not evict these regions while they run.
    busy(streams, true);
    try {
      run(
          streams,
          planned,
          (region, executed) -> ((GroupedRegion) region).collect(executed, result));
      if (stage != null) {
        stage.finish(result);
      }
    } finally {
      busy(streams, false);
    }
    metrics.total((System.nanoTime() - start) / 1e6);
    return result;
  }

  /** The table's current schema. */
  public org.apache.iceberg.Schema schema() {
    return table.schema();
  }

  private Types.NestedField column(String name) {
    Types.NestedField field = table.schema().findField(name);
    if (field == null) {
      throw new IllegalArgumentException("No column " + name);
    }
    return field;
  }

  /** Whether the device scan can read this task: a whole data file, as it reads files. */
  public static boolean readsWholeFile(FileScanTask task) {
    return HostDeletes.wholeFile(task);
  }

  /**
   * {@link #materialize(List, Consumer)} over file scan tasks someone else planned -- a Spark
   * task's -- rather than the table's: their data files read with their delete files applied.
   * Concurrent calls share the device: their batches go into one queue that every stream takes
   * from, so the streams stay busy while each caller waits only for its own batches.
   */
  public long materialize(
      List<FileScanTask> tasks, List<String> columns, Consumer<LiveRows> consumer) {
    if (tasks.isEmpty()) {
      return 0;
    }
    Planned planned = new Planned(null, columns, tasks);
    SharedPool pool = sharedPools.computeIfAbsent(planned.signature(), ignored -> new SharedPool());
    return pool.run(
        planned, (region, executed) -> ((MaterializeRegion) region).collect(executed, consumer));
  }

  private final Map<String, SharedPool> sharedPools = Maps.newConcurrentMap();

  /**
   * Every pool in this JVM that holds device buffers, least recently used first. A query engine
   * reads many tables and projections, each a pool of its own; past {@link #MAX_POOLS} the idle
   * ones give their device memory back.
   */
  /**
   * Orders freeing execution plans against running them, JVM-wide: a batch runs under the read lock
   * and a plan is closed under the write lock. An idle pool evicted to make room for another
   * projection is closed on its own thread while other pools' batches run; TPC-DS with equality
   * deletes crashed in a host-to-device copy (SIGSEGV at address -1) when the two overlapped.
   * Closing is rare (eviction, rebuild, out-of-memory release), so batches almost never wait.
   */
  private static final ReentrantReadWriteLock DEVICE_PLANS = new ReentrantReadWriteLock();

  private static final Map<SharedPool, Boolean> LIVE_POOLS = new LinkedHashMap<>(16, 0.75f, true);

  private static final int MAX_POOLS = Integer.getInteger("iceberg.tornadovm.sharedPools", 6);

  /** Frees the least recently used idle pools, other than {@code except}, down to {@code keep}. */
  private static void evictIdle(SharedPool except, int keep) {
    List<SharedPool> pools;
    synchronized (LIVE_POOLS) {
      pools = Lists.newArrayList(LIVE_POOLS.keySet());
    }
    int live = pools.size();
    for (SharedPool pool : pools) {
      if (live <= keep) {
        return;
      }
      if (pool != except && pool.releaseIfIdle()) {
        live--;
      }
    }
  }

  /** Whether a failure, or one of its causes, is the device (TornadoVM or cuDF) out of memory. */
  public static boolean isOutOfDeviceMemory(Throwable failure) {
    return outOfDeviceMemory(failure);
  }

  private static boolean outOfDeviceMemory(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof uk.ac.manchester.tornado.api.exceptions.TornadoOutOfMemoryException) {
        return true;
      }
      // cuDF's allocations fail inside the shim and arrive as a runtime error with RMM's message.
      String message = cause.getMessage();
      if (message != null
          && (message.contains("out_of_memory") || message.contains("cudaErrorMemoryAllocation"))) {
        return true;
      }
    }
    return false;
  }

  /** Regions that take batches from any caller's request, for one projection. */
  private final class SharedPool {
    // The current regions' queue. Each generation of regions has its own: a stopped region's stop
    // marker must reach that region, and a new batch must never reach a stopped region (one sharing
    // a queue with its successors could take a new batch into its freed buffers).
    private volatile LinkedBlockingQueue<Work> queue = new LinkedBlockingQueue<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ExecutorService hostPool =
        Executors.newFixedThreadPool(
            HOST_THREADS,
            runnable -> {
              Thread thread = new Thread(runnable, "tornadovm-host");
              thread.setDaemon(true);
              return thread;
            });
    private List<Region> regions;

    /** A batch of a request; a null planned stops the region that takes it. */
    private record Work(
        Planned planned,
        int index,
        CompletableFuture<HostDeletes.FilePrep> prep,
        ToLongBiFunction<Region, TornadoExecutionResult> perBatch,
        ScanMetrics metrics,
        CompletableFuture<Long> done) {}

    long run(Planned planned, ToLongBiFunction<Region, TornadoExecutionResult> perBatch) {
      try {
        return runOnce(planned, perBatch);
      } catch (RuntimeException e) {
        if (!outOfDeviceMemory(e)) {
          throw e;
        }
        // Every other idle pool gives its memory back, and this one rebuilds once.
        evictIdle(this, 0);
        releaseIfIdle();
        return runOnce(planned, perBatch);
      }
    }

    private long runOnce(
        Planned planned, ToLongBiFunction<Region, TornadoExecutionResult> perBatch) {
      // Another request may rebuild the regions between ensureFits and the read lock: check again
      // under the lock, which keeps them until this request's batches are done.
      while (true) {
        ensureFits(planned);
        lock.readLock().lock();
        if (regions != null && regions.get(0).fits(planned)) {
          break;
        }
        lock.readLock().unlock();
      }
      synchronized (LIVE_POOLS) {
        LIVE_POOLS.put(this, Boolean.TRUE);
      }
      try {
        ScanMetrics batchMetrics = new ScanMetrics();
        List<CompletableFuture<Long>> done = Lists.newArrayList();
        for (int i = 0; i < planned.batches.size(); i++) {
          List<FileScanTask> batch = planned.batches.get(i);
          CompletableFuture<HostDeletes.FilePrep> prep =
              CompletableFuture.supplyAsync(() -> planned.host.prepare(batch), hostPool);
          CompletableFuture<Long> finished = new CompletableFuture<>();
          done.add(finished);
          queue.add(new Work(planned, i, prep, perBatch, batchMetrics, finished));
        }
        // Every batch must be done, failed or not, before the read lock goes: a failed request that
        // returned early would let a rebuild or an out-of-memory release free the regions while its
        // other batches still run in them (a native crash in a host-to-device copy).
        long rows = 0;
        RuntimeException failed = null;
        for (CompletableFuture<Long> finished : done) {
          try {
            rows += finished.join();
          } catch (CompletionException e) {
            if (failed == null) {
              failed =
                  e.getCause() instanceof RuntimeException runtime
                      ? runtime
                      : new IllegalStateException("Device scan failed", e.getCause());
            }
          }
        }
        if (failed != null) {
          throw failed;
        }
        metrics = batchMetrics;
        return rows;
      } finally {
        lock.readLock().unlock();
      }
    }

    /** Builds the regions, or rebuilds them bigger once no request is in flight. */
    private void ensureFits(Planned planned) {
      lock.readLock().lock();
      try {
        if (regions != null && regions.get(0).fits(planned)) {
          return;
        }
      } finally {
        lock.readLock().unlock();
      }
      lock.writeLock().lock();
      try {
        if (regions != null && regions.get(0).fits(planned)) {
          return;
        }
        if (regions != null) {
          stopRegions();
        }
        evictIdle(this, MAX_POOLS - 1);
        LinkedBlockingQueue<Work> generation = new LinkedBlockingQueue<>();
        List<Region> created = Lists.newArrayList();
        for (int r = 0; r < STREAMS; r++) {
          Region region = new MaterializeRegion(planned);
          created.add(region);
          region.thread.execute(() -> loop(region, generation));
        }
        queue = generation;
        regions = created;
      } finally {
        lock.writeLock().unlock();
      }
    }

    /** Stops the region threads and frees their device buffers; the write lock is held. */
    private void stopRegions() {
      for (int r = 0; r < regions.size(); r++) {
        queue.add(new Work(null, -1, null, null, null, null));
      }
      regions.forEach(Region::close);
      regions = null;
      synchronized (LIVE_POOLS) {
        LIVE_POOLS.remove(this);
      }
    }

    /** Frees the device buffers unless a request is in flight; the next request rebuilds them. */
    boolean releaseIfIdle() {
      if (!lock.writeLock().tryLock()) {
        return false;
      }
      try {
        if (regions == null) {
          return false;
        }
        stopRegions();
        return true;
      } finally {
        lock.writeLock().unlock();
      }
    }

    void close() {
      lock.writeLock().lock();
      try {
        if (regions != null) {
          stopRegions();
        }
        hostPool.shutdown();
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void loop(Region region, LinkedBlockingQueue<Work> workQueue) {
      TornadoExecutionPlan executionPlan;
      RuntimeException broken = null;
      try {
        executionPlan = region.plan();
      } catch (RuntimeException e) {
        // Out of device memory, say: this region fails the work it takes rather than leave it.
        executionPlan = null;
        broken = e;
      }
      while (true) {
        Work work;
        try {
          work = workQueue.take();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        if (work.planned() == null) {
          return;
        }
        if (broken != null) {
          work.done().completeExceptionally(broken);
          continue;
        }
        try {
          work.done()
              .complete(
                  executeBatch(
                      region,
                      executionPlan,
                      work.planned(),
                      work.index(),
                      work.prep().join(),
                      work.perBatch(),
                      work.metrics()));
        } catch (Throwable failure) {
          work.done().completeExceptionally(failure);
        }
      }
    }
  }

  @Override
  public long materialize(List<String> columns, Consumer<LiveRows> consumer) {
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    Planned planned = new Planned(null, columns);
    String key = "materialize:" + String.join(",", columns);
    List<Region> streams = reuse(key, planned, () -> new MaterializeRegion(planned));
    metrics.plan((System.nanoTime() - start) / 1e6);
    long live =
        run(
            streams,
            planned,
            (region, executed) -> ((MaterializeRegion) region).collect(executed, consumer));
    metrics.total((System.nanoTime() - start) / 1e6);
    return live;
  }

  /** What {@link #rewriteDataFiles()} did. */
  public static final class RewriteResult {
    private final int dataFilesRemoved;
    private final int deleteFilesRemoved;
    private final int dataFilesAdded;
    private final long rows;

    RewriteResult(int dataFilesRemoved, int deleteFilesRemoved, int dataFilesAdded, long rows) {
      this.dataFilesRemoved = dataFilesRemoved;
      this.deleteFilesRemoved = deleteFilesRemoved;
      this.dataFilesAdded = dataFilesAdded;
      this.rows = rows;
    }

    public int dataFilesRemoved() {
      return dataFilesRemoved;
    }

    public int deleteFilesRemoved() {
      return deleteFilesRemoved;
    }

    public int dataFilesAdded() {
      return dataFilesAdded;
    }

    public long rows() {
      return rows;
    }
  }

  /**
   * Compacts the table on the device: merge-on-read into copy-on-write.
   *
   * <p>Every data file is read with its deletes applied, as a scan reads it; the live rows of each
   * batch are compacted and written, still on the device, to a new ZSTD Parquet file by libcudf --
   * with Iceberg's field ids, so Iceberg reads it back by id. The files' metrics come from the
   * footers libcudf wrote. One {@code RewriteFiles} commit then swaps every old data file and every
   * delete file for the new files, validated against the snapshot that was planned, so a concurrent
   * commit fails it rather than being lost.
   *
   * <p>A v3 table's rewrite also writes each row's lineage -- {@code _row_id} and {@code
   * _last_updated_sequence_number}, which the old files left to inheritance -- as columns of the
   * new files, so rows keep them. Data files that already store lineage columns are refused.
   */
  public RewriteResult rewriteDataFiles() {
    return rewrite(RowOrder.NONE);
  }

  /**
   * {@link #rewriteDataFiles()}, writing each new file's rows in Iceberg's Z-order of the given
   * columns -- the order {@code rewrite_data_files(strategy => 'sort', sort_order =>
   * 'zorder(...)')} writes -- so that files cluster on all of them at once.
   *
   * <p>A kernel computes every row's Z-order value as Iceberg's {@code ZOrderByteUtils} does, cuDF
   * sorts the rows of a batch by it, and the live rows are gathered in that order and written. Each
   * output file is sorted; files are not sorted against each other (a batch is the unit, as a file
   * group is Spark's). Up to {@value DeviceKernels#MAX_ZORDER_COLUMNS} integer, long, date,
   * timestamp or double columns.
   */
  public RewriteResult rewriteDataFilesZOrder(String... zorderColumns) {
    if (zorderColumns.length == 0 || zorderColumns.length > DeviceKernels.MAX_ZORDER_COLUMNS) {
      throw new IllegalArgumentException(
          "Z-order takes 1 to " + DeviceKernels.MAX_ZORDER_COLUMNS + " columns");
    }
    return rewrite(new RowOrder(List.of(zorderColumns), new int[zorderColumns.length], 0, true));
  }

  /**
   * {@link #rewriteDataFiles()}, writing each new file's rows in the table's sort order -- what
   * {@code rewrite_data_files(strategy => 'sort')} writes when no sort order is given -- and
   * recording the order's id on each new file, as Spark's writes do ({@code sort_order_id}), so
   * that readers and later rewrites know the file is sorted.
   *
   * <p>A kernel turns each row's sort fields into words that compare as the order says (direction,
   * null order, doubles as {@code Double.compare}); cuDF sorts the rows of a batch by them. Each
   * output file is sorted; files are not sorted against each other, as Spark's file groups are not.
   * Up to {@value DeviceKernels#MAX_ZORDER_COLUMNS} identity fields of integer, long, date,
   * timestamp or double columns.
   */
  public RewriteResult rewriteDataFilesSorted() {
    return rewrite(sortedRowOrder());
  }

  /** The table's sort order as a {@link RowOrder}; refused when the device cannot sort by it. */
  private RowOrder sortedRowOrder() {
    org.apache.iceberg.SortOrder order = table.sortOrder();
    if (order.isUnsorted()) {
      throw new IllegalStateException("The table has no sort order");
    }
    if (order.fields().size() > DeviceKernels.MAX_ZORDER_COLUMNS) {
      throw new UnsupportedOperationException(
          "Sort orders of more than " + DeviceKernels.MAX_ZORDER_COLUMNS + " fields");
    }
    List<String> columns = Lists.newArrayList();
    int[] flags = new int[order.fields().size()];
    for (int f = 0; f < flags.length; f++) {
      org.apache.iceberg.SortField field = order.fields().get(f);
      if (!field.transform().isIdentity()) {
        throw new UnsupportedOperationException("Sort field " + field + " is not an identity");
      }
      columns.add(table.schema().findColumnName(field.sourceId()));
      flags[f] =
          (field.direction() == org.apache.iceberg.SortDirection.DESC ? 1 : 0)
              | (field.nullOrder() == org.apache.iceberg.NullOrder.NULLS_FIRST ? 2 : 0);
    }
    return new RowOrder(columns, flags, order.orderId(), false);
  }

  /**
   * How a rewrite orders each file's rows: by the Z-order of {@code columns}, or by them in turn
   * with {@code flags} (bit 0 descending, bit 1 nulls first) for sort order {@code sortOrderId}.
   */
  private record RowOrder(List<String> columns, int[] flags, int sortOrderId, boolean zorder) {
    static final RowOrder NONE = new RowOrder(List.of(), new int[0], 0, true);

    int words() {
      return zorder ? columns.size() + 1 : 1 + 2 * columns.size();
    }

    String key() {
      return (zorder ? "-z" : "-sorted" + sortOrderId) + columns + Arrays.toString(flags);
    }
  }

  private RewriteResult rewrite(RowOrder order) {
    if (filter.op() != Expression.Operation.TRUE) {
      throw new UnsupportedOperationException("A rewrite of part of a table is not supported");
    }
    long start = System.nanoTime();
    table.refresh();
    Long snapshotId = table.currentSnapshot() == null ? null : table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
      planned.forEach(tasks::add);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    // As a file group: batches sized by the rows' decoded width, halved on running out of device
    // memory, so a wide table (TPC-DS store_sales: 23 columns) fits as a narrow one does.
    List<DataFile> written =
        rewriteGroup(
            tasks,
            PropertyUtil.propertyAsLong(
                table.properties(),
                org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT),
            order);
    long rows = written.stream().mapToLong(DataFile::recordCount).sum();

    Set<String> deletesSeen = Sets.newHashSet();
    RewriteFiles rewrite = table.newRewrite();
    if (snapshotId != null) {
      rewrite.validateFromSnapshot(snapshotId);
    }
    for (FileScanTask task : tasks) {
      rewrite.deleteFile(task.file());
      for (DeleteFile delete : task.deletes()) {
        if (deletesSeen.add(delete.location())) {
          rewrite.deleteFile(delete);
        }
      }
    }
    written.forEach(rewrite::addFile);
    rewrite.commit();
    metrics.total((System.nanoTime() - start) / 1e6);
    return new RewriteResult(tasks.size(), deletesSeen.size(), written.size(), rows);
  }

  /** What {@link #convertEqualityDeletes()} did. */
  public record ConversionResult(
      int dataFiles,
      int equalityDeletesRemoved,
      int positionDeletesReplaced,
      int vectorsAdded,
      long deletedRows) {}

  /**
   * Turns a v3 table's equality deletes into deletion vectors, without rewriting a data file.
   *
   * <p>Every data file that equality deletes apply to is read -- its key columns only -- with all
   * of its deletes applied, as a scan reads it. A kernel marks the rows merge-on-read drops, cuDF
   * compacts their positions, and only those come back. Each file gets one deletion vector holding
   * all of its deleted rows: the equality hits and its earlier deletion vector or file-scoped
   * position deletes, which the same {@code RowDelta} replaces. The equality-delete files are
   * removed with them: every data file they apply to was planned, so none still needs them.
   * Validated against the snapshot that was planned; a concurrent delete fails the commit.
   */
  public ConversionResult convertEqualityDeletes() {
    if (filter.op() != Expression.Operation.TRUE) {
      throw new UnsupportedOperationException("A conversion of part of a table is not supported");
    }
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    table.refresh();
    if (table.currentSnapshot() == null) {
      return new ConversionResult(0, 0, 0, 0, 0);
    }
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask task : planned) {
        if (task.deletes().stream().anyMatch(d -> d.content() == FileContent.EQUALITY_DELETES)) {
          tasks.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (tasks.isEmpty()) {
      return new ConversionResult(0, 0, 0, 0, 0);
    }
    Set<DeleteFile> equality = Sets.newHashSet();
    for (FileScanTask task : tasks) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() == FileContent.EQUALITY_DELETES) {
          equality.add(delete);
        }
      }
    }
    return writeVectors(snapshotId, start, tasks, equality, delta -> {});
  }

  /**
   * Deletes the rows whose {@code column} holds one of the keys, as deletion vectors: an SQL {@code
   * DELETE ... WHERE column IN (keys)} for a v3 table, without writing an equality-delete file for
   * readers to apply. What a CDC writer does for keys it has seen before, with their positions
   * found on the device instead of left to every reader.
   *
   * <p>Iceberg prunes the data files by the column's bounds; the device reads the column of the
   * files that remain, with every delete already applying to them, and each such file gets one
   * deletion vector holding its earlier deletes and the new ones. One {@code RowDelta} adds the
   * vectors and replaces the files' earlier vectors or file-scoped position deletes. It validates
   * against the planned snapshot, fails on concurrent deletes and on data files added since that
   * could hold one of the keys. Equality-delete files already in the table stay as they are.
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public ConversionResult deleteKeys(String column, Collection<?> keys) {
    if (filter.op() != Expression.Operation.TRUE) {
      throw new UnsupportedOperationException("A delete on a filtered scan is not supported");
    }
    Types.NestedField field = table.schema().findField(column);
    if (field == null) {
      throw new IllegalArgumentException("No column " + column);
    }
    boolean intKey = field.type().typeId() == org.apache.iceberg.types.Type.TypeID.INTEGER;
    if (!intKey && field.type().typeId() != org.apache.iceberg.types.Type.TypeID.LONG) {
      throw new UnsupportedOperationException(
          "Keys of type " + field.type() + " are not supported");
    }
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    table.refresh();
    if (table.currentSnapshot() == null || keys.isEmpty()) {
      return new ConversionResult(0, 0, 0, 0, 0);
    }
    long snapshotId = table.currentSnapshot().snapshotId();
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(intKey ? (Object) ((Number) key).intValue() : (Object) ((Number) key).longValue());
    }
    // Up to IN_KEYS keys prune by each one; more by their range, which costs Iceberg's metrics
    // evaluation nothing per key (an IN of 500,000 literals took most of a 0.8 s delete) and prunes
    // the same files when the keys spread over the column's values, as upserts' do.
    Expression matching;
    if (values.size() <= IN_KEYS) {
      matching = Expressions.in(column, values);
    } else {
      long low = Long.MAX_VALUE;
      long high = Long.MIN_VALUE;
      for (Object value : values) {
        long key = ((Number) value).longValue();
        low = Math.min(low, key);
        high = Math.max(high, key);
      }
      matching =
          intKey
              ? Expressions.and(
                  Expressions.greaterThanOrEqual(column, (int) low),
                  Expressions.lessThanOrEqual(column, (int) high))
              : Expressions.and(
                  Expressions.greaterThanOrEqual(column, low),
                  Expressions.lessThanOrEqual(column, high));
    }
    // Files that may hold a key, by their bounds; their tasks come from a plan without the filter,
    // so that no residual drops the rows that do not match.
    Set<String> candidates = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> pruned =
        table.newScan().useSnapshot(snapshotId).filter(matching).planFiles()) {
      pruned.forEach(task -> candidates.add(task.file().location()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    if (candidates.isEmpty()) {
      return new ConversionResult(0, 0, 0, 0, 0);
    }
    long[] held = new long[values.size()];
    for (int i = 0; i < held.length; i++) {
      held[i] = ((Number) values.get(i)).longValue();
    }
    DeleteFile keyFile = keyFile(field, HostDeletes.provide(held), held.length);
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> planned =
        table.newScan().useSnapshot(snapshotId).planFiles()) {
      for (FileScanTask task : planned) {
        if (candidates.contains(task.file().location())) {
          tasks.add(new WithKeys(task, keyFile));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    try {
      return writeVectors(
          snapshotId,
          start,
          tasks,
          Set.of(),
          delta -> delta.conflictDetectionFilter(matching).validateNoConflictingDataFiles());
    } finally {
      HostDeletes.forget(keyFile.location());
    }
  }

  private static final int IN_KEYS =
      Integer.getInteger("iceberg.tornadovm.deleteKeys.inLimit", 1000);

  /**
   * An equality-delete file that names keys held in memory ({@link HostDeletes#provide}): never
   * written, never committed, only attached to the tasks the device runs.
   */
  private static DeleteFile keyFile(Types.NestedField field, String location, long count) {
    return org.apache.iceberg.FileMetadata.deleteFileBuilder(
            org.apache.iceberg.PartitionSpec.unpartitioned())
        .ofEqualityDeletes(field.fieldId())
        .withPath(location)
        .withFormat(org.apache.iceberg.FileFormat.PARQUET)
        .withFileSizeInBytes(0)
        .withRecordCount(count)
        .build();
  }

  /** A planned task with one more delete file, for the device only. */
  private static final class WithKeys implements FileScanTask {
    private final FileScanTask task;
    private final List<DeleteFile> deletes;

    WithKeys(FileScanTask task, DeleteFile keys) {
      this.task = task;
      List<DeleteFile> all = Lists.newArrayList(task.deletes());
      all.add(keys);
      this.deletes = List.copyOf(all);
    }

    @Override
    public DataFile file() {
      return task.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return deletes;
    }

    @Override
    public org.apache.iceberg.PartitionSpec spec() {
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
      return List.of(this);
    }
  }

  /**
   * The shared tail of {@link #convertEqualityDeletes()} and {@link #deleteKeys}: every row the
   * tasks' deletes drop goes into one deletion vector per data file (v3), or one file-scoped
   * position-delete file per data file (v2), which replaces the file's earlier vector or
   * file-scoped position deletes; {@code removed} are further delete files the commit removes, and
   * {@code validate} adds the operation's own checks.
   */
  private ConversionResult writeVectors(
      long snapshotId,
      long start,
      List<FileScanTask> tasks,
      Set<DeleteFile> removed,
      Consumer<org.apache.iceberg.RowDelta> validate) {
    Set<DeleteFile> replaced = Sets.newHashSet();
    for (FileScanTask task : tasks) {
      for (DeleteFile delete : task.deletes()) {
        if (delete.content() == FileContent.EQUALITY_DELETES) {
          // Their hits are in the vectors written here; the caller names the files in removed.
        } else if (ContentFileUtil.isDV(delete)
            || ContentFileUtil.referencedDataFile(delete) != null) {
          replaced.add(delete);
        } else {
          throw new UnsupportedOperationException(
              "A partition-scoped position delete file applies: " + delete.location());
        }
      }
    }
    List<String> keys = Lists.newArrayList();
    for (int keyId : HostDeletes.equalityKeyIds(tasks)) {
      keys.add(table.schema().findField(keyId).name());
    }
    Planned planned = new Planned(null, keys, tasks, BATCH_ROWS, false, false);
    PositionSink writer =
        org.apache.iceberg.TableUtil.formatVersion(table) >= 3
            ? PositionSink.vectors(table, start)
            : PositionSink.positionFiles(table, start);
    List<Region> streams =
        reuse("dead:" + String.join(",", keys), planned, () -> new DeadRegion(planned));
    metrics.plan((System.nanoTime() - start) / 1e6);
    long runStart = System.nanoTime();
    long dead =
        run(
            streams,
            planned,
            (region, executed) -> ((DeadRegion) region).collect(executed, writer));
    metrics.run((System.nanoTime() - runStart) / 1e6);
    long writeStart = System.nanoTime();
    List<DeleteFile> vectors;
    try {
      vectors = writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    metrics.write((System.nanoTime() - writeStart) / 1e6);
    org.apache.iceberg.RowDelta delta =
        table
            .newRowDelta()
            .validateFromSnapshot(snapshotId)
            .validateNoConflictingDeleteFiles()
            .validateDeletedFiles()
            .validateDataFilesExist(referencedDataFiles(vectors));
    validate.accept(delta);
    removed.forEach(delta::removeDeletes);
    replaced.forEach(delta::removeDeletes);
    vectors.forEach(delta::addDeletes);
    long commitStart = System.nanoTime();
    delta.commit();
    metrics.commit((System.nanoTime() - commitStart) / 1e6);
    metrics.total((System.nanoTime() - start) / 1e6);
    return new ConversionResult(
        tasks.size(), removed.size(), replaced.size(), vectors.size(), dead);
  }

  /** The data files the new deletes point at, which a concurrent rewrite must not have removed. */
  static List<CharSequence> referencedDataFiles(List<DeleteFile> deletes) {
    List<CharSequence> files = Lists.newArrayList();
    for (DeleteFile delete : deletes) {
      CharSequence file = ContentFileUtil.referencedDataFile(delete);
      if (file != null) {
        files.add(file);
      }
    }
    return files;
  }

  /**
   * Nominal entries of the theta sketches: what Iceberg's compute_table_stats builds by default.
   */
  private static final int SKETCH_ENTRIES = 4096;

  /**
   * Iceberg's {@code compute_table_stats} on the device: for each column, the number of distinct
   * values as an Apache DataSketches theta sketch, written to a Puffin statistics file and set on
   * the current snapshot -- the blob type ({@code apache-datasketches-theta-v1}), {@code ndv}
   * property and commit the Spark action writes, so planners that read Iceberg's NDV statistics use
   * these as they are.
   *
   * <p>The device hashes every live value as DataSketches does (MurmurHash3, seed 9001, of the
   * value's Iceberg serialization), sorts each batch's hashes, and keeps the smallest {@value
   * #SKETCH_ENTRIES} + 1 distinct ones; only those come back. The host merges the batches and
   * writes the compact sketch: the {@value #SKETCH_ENTRIES} smallest distinct hashes and, past
   * them, theta. Columns of int, long, date, timestamp and double. Returns each column's estimate.
   */
  public Map<String, Long> computeTableStats(String... columns) {
    if (filter.op() != Expression.Operation.TRUE) {
      throw new UnsupportedOperationException("Statistics of part of a table are not supported");
    }
    table.refresh();
    org.apache.iceberg.Snapshot snapshot = table.currentSnapshot();
    if (snapshot == null) {
      return Map.of();
    }
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    Planned planned = new Planned(null, List.of(columns));
    List<Types.NestedField> fields = Lists.newArrayList();
    for (String column : columns) {
      Types.NestedField field = table.schema().findField(column);
      if (field == null) {
        throw new IllegalArgumentException("No column " + column);
      }
      switch (field.type().typeId()) {
        case INTEGER, DATE, LONG, TIMESTAMP, DOUBLE -> fields.add(field);
        default ->
            throw new UnsupportedOperationException(
                "Statistics of " + field.type() + " columns are not supported on the device");
      }
    }
    List<NavigableSet<Long>> smallest = Lists.newArrayList();
    fields.forEach(field -> smallest.add(new TreeSet<>()));
    List<Region> streams =
        reuse(
            "stats:" + String.join(",", columns),
            planned,
            () -> new StatsRegion(planned, fields, SKETCH_ENTRIES + 1));
    metrics.plan((System.nanoTime() - start) / 1e6);
    long runStart = System.nanoTime();
    run(
        streams,
        planned,
        (region, executed) ->
            ((StatsRegion) region).collect(executed, smallest, SKETCH_ENTRIES + 1));
    metrics.run((System.nanoTime() - runStart) / 1e6);

    long writeStart = System.nanoTime();
    Map<String, Long> estimates = new LinkedHashMap<>();
    String location =
        table.locationProvider().newDataLocation("stats-" + UUID.randomUUID() + ".puffin");
    OutputFile output = table.io().newOutputFile(location);
    org.apache.iceberg.StatisticsFile statistics;
    try (PuffinWriter writer = Puffin.write(output).createdBy("iceberg-tornadovm").build()) {
      for (int c = 0; c < fields.size(); c++) {
        byte[] sketch = ThetaSketches.compact(smallest.get(c), SKETCH_ENTRIES);
        long estimate =
            (long)
                org.apache.datasketches.theta.CompactSketch.heapify(
                        org.apache.datasketches.memory.Memory.wrap(sketch))
                    .getEstimate();
        estimates.put(fields.get(c).name(), estimate);
        writer.add(
            new Blob(
                StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                List.of(fields.get(c).fieldId()),
                snapshot.snapshotId(),
                snapshot.sequenceNumber(),
                ByteBuffer.wrap(sketch),
                PuffinCompressionCodec.ZSTD,
                Map.of("ndv", String.valueOf(estimate))));
      }
      writer.finish();
      statistics =
          new org.apache.iceberg.GenericStatisticsFile(
              snapshot.snapshotId(),
              output.location(),
              writer.fileSize(),
              writer.footerSize(),
              org.apache.iceberg.GenericBlobMetadata.from(writer.writtenBlobsMetadata()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    metrics.write((System.nanoTime() - writeStart) / 1e6);
    long commitStart = System.nanoTime();
    table.updateStatistics().setStatistics(statistics).commit();
    metrics.commit((System.nanoTime() - commitStart) / 1e6);
    metrics.total((System.nanoTime() - start) / 1e6);
    return estimates;
  }

  /**
   * The live rows of file scan tasks someone else planned -- a file group of Spark's {@code
   * rewrite_data_files}, say -- written into new data files on the device, as {@link
   * #rewriteDataFiles()} writes them: one file a batch, of about {@code targetFileBytes}. Nothing
   * is committed: the caller swaps the files. One group at a time; the streams share its batches.
   */
  public List<DataFile> rewriteGroup(List<FileScanTask> tasks, long targetFileBytes) {
    return rewriteGroup(tasks, targetFileBytes, RowOrder.NONE);
  }

  /**
   * {@link #rewriteGroup}, each new file written in the table's sort order with the order's id set,
   * as {@link #rewriteDataFilesSorted()} writes them: a sort-strategy file group, chosen by
   * whichever planner picks the files (a {@code where}, a delete-file threshold).
   */
  public List<DataFile> rewriteGroupSorted(List<FileScanTask> tasks, long targetFileBytes) {
    return rewriteGroup(tasks, targetFileBytes, sortedRowOrder());
  }

  private List<DataFile> rewriteGroup(
      List<FileScanTask> tasks, long targetFileBytes, RowOrder order) {
    // Before taking this scan's lock: another rewriter frees its regions under its own.
    DeviceMergeOnReadScan previous = LAST_REWRITER.getAndSet(this);
    if (previous != null && previous != this) {
      previous.releaseRewriteRegions();
    }
    synchronized (this) {
      return rewriteGroupLocked(tasks, targetFileBytes, order);
    }
  }

  private List<DataFile> rewriteGroupLocked(
      List<FileScanTask> tasks, long targetFileBytes, RowOrder order) {
    metrics = new ScanMetrics();
    long start = System.nanoTime();
    List<DataFile> written = Collections.synchronizedList(Lists.newArrayList());
    if (!tasks.isEmpty()) {
      // A file's worth of rows a batch, at the inputs' bytes a row: files the size Spark's writer
      // would make, and device buffers a file's rows deep however large the group.
      long bytes = tasks.stream().mapToLong(t -> t.file().fileSizeInBytes()).sum();
      long rows = Math.max(1, tasks.stream().mapToLong(t -> t.file().recordCount()).sum());
      long fileRows =
          Math.max(1, Math.min(BATCH_ROWS, targetFileBytes / Math.max(1, bytes / rows)));
      // ... but no more than a batch's worth of decoded rows: a table that compresses to a few
      // bytes a row (TPC-H lineitem: ~3) would otherwise make batches of millions of wide rows,
      // strings included, on every stream at once.
      fileRows = Math.max(1024, Math.min(fileRows, REWRITE_BATCH_BYTES / decodedRowBytes(tasks)));
      // A rewrite needs room: idle readers give their device memory back first, as the last
      // other table rewritten has. This one keeps its regions -- and its compiled kernels -- for
      // its next group.
      evictIdle(null, 0);
      Planned planned = new Planned(null, rewriteColumns(), tasks, fileRows, false, true);
      if (DEBUG) {
        LOG.info(
            "[mor] rewrite group: {} files, {} rows, target {} bytes -> {} rows a file; capacity {},"
                + " {} batches, positions {}, set {}, {} fields",
            tasks.size(),
            rows,
            targetFileBytes,
            fileRows,
            planned.capacity,
            planned.batches.size(),
            planned.positionCapacity,
            planned.equalityCapacity,
            planned.layout.fields().size());
      }
      try {
        write(planned, order, written, start, REWRITE_STREAMS);
      } catch (RuntimeException e) {
        if (!outOfDeviceMemory(e)) {
          throw e;
        }
        // Out of device memory -- wide string rows next to cuDF's own buffers, say. Drop what
        // the group wrote, free everything idle, and write it again in halves on one stream.
        discard(written);
        releaseRewriteRegions();
        evictIdle(null, 0);
        Planned smaller =
            new Planned(null, rewriteColumns(), tasks, Math.max(1, fileRows / 2), false, true);
        write(smaller, order, written, System.nanoTime(), 1);
      }
    }
    return Lists.newArrayList(written);
  }

  /** Decoded bytes a row of the table, numeric columns and strings (from the files' footers). */
  private long decodedRowBytes(List<FileScanTask> tasks) {
    long bytes = 0;
    for (Types.NestedField field : table.schema().columns()) {
      if (field.type().typeId() != org.apache.iceberg.types.Type.TypeID.STRING) {
        bytes += Long.BYTES + 1;
        continue;
      }
      long chars = 0;
      long rows = 0;
      for (FileScanTask task : tasks) {
        long fileBytes =
            HostDeletes.stringBytes(
                LocalFiles.path(task.file().location(), table.io()), field.fieldId());
        if (fileBytes < 0) {
          chars = -1;
          break;
        }
        chars += fileBytes;
        rows += task.file().recordCount();
      }
      // Offsets and validity, plus the bytes; 16 a row where a writer recorded no sizes.
      bytes += Integer.BYTES + 1 + (chars < 0 || rows == 0 ? 16 : chars / rows + 1);
    }
    return Math.max(1, bytes);
  }

  /** Deletes the files a failed attempt wrote, which nothing will reference. */
  private void discard(List<DataFile> written) {
    synchronized (written) {
      for (DataFile file : written) {
        table.io().deleteFile(file.location());
      }
      written.clear();
    }
  }

  /** The scan whose rewrite regions are kept: one rewriter's at a time hold device memory. */
  private static final AtomicReference<DeviceMergeOnReadScan> LAST_REWRITER =
      new AtomicReference<>();

  private synchronized void releaseRewriteRegions() {
    regions
        .entrySet()
        .removeIf(
            entry -> {
              if (entry.getKey().startsWith("rewrite") && entry.getKey().endsWith("-given")) {
                entry.getValue().forEach(Region::close);
                return true;
              }
              return false;
            });
  }

  /** Every column of the table, refusing what the device cannot read and write. */
  private List<String> rewriteColumns() {
    List<String> columns = Lists.newArrayList();
    for (Types.NestedField field : table.schema().columns()) {
      org.apache.iceberg.types.Type.TypeID type = field.type().typeId();
      if (type == org.apache.iceberg.types.Type.TypeID.TIME) {
        throw new UnsupportedOperationException(
            field.type() + " columns cannot be written on the device");
      }
      ScanLayout.cudfType(field); // refuses what the device cannot read
      columns.add(field.name());
    }
    return columns;
  }

  /** Writes the planned tasks' live rows into new files, added to {@code written}; their rows. */
  private long write(
      Planned planned, RowOrder order, List<DataFile> written, long start, int maxStreams) {
    int formatVersion =
        ((org.apache.iceberg.HasTableOperations) table).operations().current().formatVersion();
    List<Region> streams =
        reuse(
            "rewrite"
                + (formatVersion >= 3 ? "-lineage" : "")
                + order.key()
                + (planned.given ? "-given" : ""),
            planned,
            () -> new RewriteRegion(planned, formatVersion >= 3, order),
            maxStreams);
    metrics.plan((System.nanoTime() - start) / 1e6);
    return run(
        streams,
        planned,
        (region, executed) -> ((RewriteRegion) region).collect(executed, written));
  }

  /** The regions kept for a query shape, one a stream, rebuilt when this scan outgrows them. */
  private List<Region> reuse(String key, Planned planned, Supplier<Region> create) {
    return reuse(key, planned, create, STREAMS);
  }

  private List<Region> reuse(String key, Planned planned, Supplier<Region> create, int maxStreams) {
    List<Region> kept = regions.get(key);
    if (kept != null && kept.get(0).fits(planned)) {
      if (key.startsWith(GROUPED)) {
        synchronized (AGGREGATE_SHAPES) {
          AGGREGATE_SHAPES.get(kept);
        }
      }
      return kept;
    }
    if (kept != null) {
      forget(kept);
      kept.forEach(Region::close);
    }
    // Planned tasks someone else will send more of get every stream from the start.
    int streams =
        Math.max(1, planned.given ? maxStreams : Math.min(maxStreams, planned.batches.size()));
    List<Region> created = Lists.newArrayList();
    for (int i = 0; i < streams; i++) {
      created.add(create.get());
    }
    regions.put(key, created);
    if (key.startsWith(GROUPED)) {
      keepShape(created, () -> regions.remove(key, created));
    }
    return created;
  }

  private static final String GROUPED = "grouped:";

  /**
   * The aggregate shapes' regions in this JVM, least recently used first, with how to drop each
   * from its scan. Every query shape -- group columns, values, a star's joins -- has regions of its
   * own, host and device buffers sized to whole batches; past {@link #MAX_AGGREGATE_SHAPES} the
   * least recently used are freed, so a workload of many queries over a few cached scans does not
   * keep one set a query. A set an aggregate is running on is busy and never evicted.
   */
  private static final Map<List<Region>, Runnable> AGGREGATE_SHAPES =
      new LinkedHashMap<>(16, 0.75f, true);

  private static final int MAX_AGGREGATE_SHAPES =
      Integer.getInteger("iceberg.tornadovm.aggregateShapes", 6);

  // Region sets an aggregate is running on (by identity): never evicted.
  private static final Set<List<Region>> BUSY = Collections.newSetFromMap(new IdentityHashMap<>());

  private static void busy(List<Region> set, boolean running) {
    synchronized (AGGREGATE_SHAPES) {
      if (running) {
        BUSY.add(set);
      } else {
        BUSY.remove(set);
      }
    }
  }

  private static void keepShape(List<Region> created, Runnable drop) {
    List<List<Region>> evicted = Lists.newArrayList();
    synchronized (AGGREGATE_SHAPES) {
      AGGREGATE_SHAPES.put(created, drop);
      Iterator<Map.Entry<List<Region>, Runnable>> eldest = AGGREGATE_SHAPES.entrySet().iterator();
      int over = AGGREGATE_SHAPES.size() - MAX_AGGREGATE_SHAPES;
      while (over > 0 && eldest.hasNext()) {
        Map.Entry<List<Region>, Runnable> entry = eldest.next();
        if (entry.getKey() == created || BUSY.contains(entry.getKey())) {
          continue;
        }
        entry.getValue().run();
        evicted.add(entry.getKey());
        eldest.remove();
        over--;
      }
    }
    evicted.forEach(set -> set.forEach(Region::close));
  }

  private static void forget(List<Region> set) {
    synchronized (AGGREGATE_SHAPES) {
      AGGREGATE_SHAPES.remove(set);
    }
  }

  /**
   * Runs every batch of a query on the given regions, one thread a region taking the next batch as
   * it finishes one; {@code perBatch} returns the live rows a batch had. Host preparation of every
   * batch is started up front on its own pool.
   */
  /** Per field of the layout, whether every file of the batch counts no null in it. */
  private static boolean[] nullFree(ScanLayout layout, List<FileScanTask> batch) {
    List<Types.NestedField> fields = layout.fields();
    boolean[] free = new boolean[fields.size()];
    for (int c = 0; c < free.length; c++) {
      Types.NestedField field = fields.get(c);
      boolean none = true;
      for (FileScanTask task : batch) {
        Map<Integer, Long> counts = task.file().nullValueCounts();
        Long count = counts == null ? null : counts.get(field.fieldId());
        if (count == null || count != 0) {
          none = false;
          break;
        }
      }
      free[c] = field.isRequired() || (none && !Ablation.off("nullcounts"));
    }
    return free;
  }

  /** One batch on one region: stage it, run the plan, hand the result over. */
  private long executeBatch(
      Region region,
      TornadoExecutionPlan executionPlan,
      Planned planned,
      int batchIndex,
      HostDeletes.FilePrep prep,
      ToLongBiFunction<Region, TornadoExecutionResult> perBatch,
      ScanMetrics into) {
    DEVICE_PLANS.readLock().lock();
    try {
      return executeBatchLocked(region, executionPlan, planned, batchIndex, prep, perBatch, into);
    } finally {
      DEVICE_PLANS.readLock().unlock();
    }
  }

  private long executeBatchLocked(
      Region region,
      TornadoExecutionPlan executionPlan,
      Planned planned,
      int batchIndex,
      HostDeletes.FilePrep prep,
      ToLongBiFunction<Region, TornadoExecutionResult> perBatch,
      ScanMetrics into) {
    into.deleteLoad(prep.hostNanos() / 1e6);
    region.nullFree = nullFree(region.layout, planned.batches.get(batchIndex));
    region.stage(executionPlan, prep, planned.keyNullable);
    if (region.filtered) {
      planned
          .batchFilters
          .get(batchIndex)
          .encode(
              region.layout,
              table.schema(),
              region.capacity,
              region.filterTerms,
              region.filterLongs,
              region.filterDoubles);
    }
    long deviceStart = System.nanoTime();
    TornadoExecutionResult executed = executionPlan.execute();
    region.checkStrings(executed);
    long copyStart = System.nanoTime();
    long rows = perBatch.applyAsLong(region, executed);
    if (DEBUG) {
      LOG.info(
          "[mor] batch {} region {} files {} rows {} live {}",
          batchIndex,
          System.identityHashCode(region),
          prep.files(),
          prep.rows(),
          rows);
    }
    long end = System.nanoTime();
    into.device((copyStart - deviceStart) / 1e6);
    into.copyBack((end - copyStart) / 1e6);
    into.batch(prep.rows(), prep.files());
    into.live(rows);
    return rows;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private long run(
      List<Region> streams,
      Planned planned,
      ToLongBiFunction<Region, TornadoExecutionResult> perBatch) {
    ExecutorService hostPool = Executors.newFixedThreadPool(HOST_THREADS);
    AtomicInteger next = new AtomicInteger();
    AtomicLong liveRows = new AtomicLong();
    // A failed batch stops the other regions taking new ones; the run returns only once every
    // region
    // is done, so that a caller that frees the regions next (an out-of-memory retry) frees idle
    // ones.
    AtomicBoolean stop = new AtomicBoolean();
    try {
      List<CompletableFuture<HostDeletes.FilePrep>> preps = Lists.newArrayList();
      for (List<FileScanTask> batch : planned.batches) {
        preps.add(CompletableFuture.supplyAsync(() -> planned.host.prepare(batch), hostPool));
      }
      List<Future<?>> workers = Lists.newArrayList();
      for (Region region : streams) {
        workers.add(
            region.thread.submit(
                () -> {
                  try {
                    TornadoExecutionPlan executionPlan = region.plan();
                    for (int i = next.getAndIncrement();
                        i < preps.size() && !stop.get();
                        i = next.getAndIncrement()) {
                      long rows =
                          executeBatch(
                              region,
                              executionPlan,
                              planned,
                              i,
                              preps.get(i).join(),
                              perBatch,
                              metrics);
                      liveRows.addAndGet(rows);
                    }
                  } catch (RuntimeException | Error failure) {
                    stop.set(true);
                    throw failure;
                  }
                }));
      }
      ExecutionException first = null;
      for (Future<?> worker : workers) {
        try {
          worker.get();
        } catch (ExecutionException e) {
          first = first == null ? e : first;
        }
      }
      if (first != null) {
        throw first;
      }
    } catch (ExecutionException e) {
      for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
        if (cause instanceof UnsupportedOperationException unsupported) {
          throw unsupported;
        }
        if (String.valueOf(cause.getMessage()).contains("a column was sized for")) {
          // The string read found more bytes than 8 a row: some value is longer than that.
          throw new UnsupportedOperationException(
              "A string column holds values longer than "
                  + STRING_BYTES
                  + " bytes, which the device does not support",
              e.getCause());
        }
      }
      throw new IllegalStateException("Device scan failed", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } finally {
      hostPool.shutdownNow();
    }
    return liveRows.get();
  }

  /** One query's planning: Iceberg's file tasks, grouped into device batches, and their sizes. */
  private final class Planned {
    private final List<List<FileScanTask>> batches = Lists.newArrayList();
    private final List<FileScanTask> tasks;
    private final ScanLayout layout;
    private final HostDeletes host;
    private final Integer equalityKeyId;
    private final List<Integer> keyIds;
    private final CudfType equalityKeyType;
    private final boolean keyNullable;
    private final int capacity;
    private final int equalityCapacity;
    private final int positionCapacity;
    private final int maxFiles;
    private final boolean filtered;
    private final List<DevicePredicate> batchFilters = Lists.newArrayList();
    // Bytes a string column of a batch may decode to, from the footers' size statistics.
    private final long stringStride;
    // Columns whose values the device compares -- filters and keys: a string among them must pack.
    private final Set<Integer> keyedIds = Sets.newHashSet();
    private final boolean given;

    Planned(String groupColumn, List<String> projection) {
      this(groupColumn, projection, null);
    }

    /**
     * A given task's residual as a device filter, or null when the device cannot run it: an
     * operation it lacks, or a string column, whose values may not pack into a key.
     */
    private DevicePredicate deviceResidual(Expression residual) {
      if (residual.op() == Expression.Operation.TRUE) {
        return null;
      }
      for (int fieldId : DevicePredicate.fieldIds(residual, table.schema())) {
        if (table.schema().findField(fieldId).type().typeId()
            == org.apache.iceberg.types.Type.TypeID.STRING) {
          return null;
        }
      }
      try {
        return DevicePredicate.compile(residual, table.schema());
      } catch (UnsupportedOperationException e) {
        return null;
      }
    }

    /**
     * {@code given}: file scan tasks someone else planned (a Spark task's, say) instead of the
     * table's. Their sizes are rounded up, so that the next group of tasks most likely fits the
     * same device buffers and plans.
     */
    Planned(String groupColumn, List<String> projection, List<FileScanTask> given) {
      this(groupColumn, projection, given, BATCH_ROWS, true, false);
    }

    /**
     * {@code givenBatchRows}: the most rows a batch of given tasks holds. {@code padGiven}: whether
     * their sizes are rounded up for the next caller's tasks -- a reader's; a rewrite plans its
     * whole group up front and sizes the device to it exactly.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
    Planned(
        String groupColumn,
        List<String> projection,
        List<FileScanTask> given,
        long givenBatchRows,
        boolean padGiven,
        boolean rewrite) {
      this.given = given != null;
      if (given != null) {
        this.tasks = Lists.newArrayList(given);
      } else {
        try (CloseableIterable<FileScanTask> planned = table.newScan().filter(filter).planFiles()) {
          this.tasks = Lists.newArrayList(planned);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      this.equalityKeyId = HostDeletes.equalityKeyId(tasks);
      if (equalityKeyId != null
          && table.schema().findField(equalityKeyId).type().typeId()
              == org.apache.iceberg.types.Type.TypeID.STRING) {
        throw new UnsupportedOperationException(
            "Equality deletes on a string column are not supported on the device");
      }
      List<Integer> allKeyIds = HostDeletes.equalityKeyIds(tasks);
      this.keyIds = allKeyIds.size() > 1 ? allKeyIds : List.of();
      // Given tasks carry the caller's filter as their residuals. The caller applies it again
      // (Spark does), so the device only narrows what comes back, and a residual it cannot run
      // simply filters nothing.
      Map<Expression, DevicePredicate> residuals = Maps.newHashMap();
      Set<Integer> filterIds = Sets.newHashSet(DevicePredicate.fieldIds(filter, table.schema()));
      if (given != null) {
        for (FileScanTask task : tasks) {
          residuals.computeIfAbsent(
              task.residual(),
              residual -> {
                DevicePredicate compiled = deviceResidual(residual);
                if (compiled != null) {
                  filterIds.addAll(DevicePredicate.fieldIds(residual, table.schema()));
                }
                return compiled == null ? DevicePredicate.alwaysTrue() : compiled;
              });
        }
      }
      this.filtered =
          filter.op() != Expression.Operation.TRUE
              || residuals.keySet().stream()
                  .anyMatch(r -> r.op() != Expression.Operation.TRUE && deviceResidual(r) != null);
      List<String> read = Lists.newArrayList(projection);
      for (int keyId : keyIds) {
        String name = table.schema().findField(keyId).name();
        if (!read.contains(name)) {
          read.add(name);
        }
      }
      for (int fieldId : filterIds) {
        String name = table.schema().findField(fieldId).name();
        if (!read.contains(name)) {
          read.add(name);
        }
      }
      this.layout = new ScanLayout(table.schema(), equalityKeyId, groupColumn, read);
      this.host = new HostDeletes(table, layout, equalityKeyId, true).withKeyIds(keyIds);
      // Files on an object store are fetched to the local cache together, before their footers
      // are read below.
      // Data files bring only the columns the device reads (a rewrite reads them all); delete
      // files come whole.
      List<String> dataLocations = Lists.newArrayList();
      List<String> deleteLocations = Lists.newArrayList();
      for (FileScanTask task : tasks) {
        dataLocations.add(task.file().location());
        for (DeleteFile delete : task.deletes()) {
          if (!HostDeletes.isProvided(delete.location())) {
            deleteLocations.add(delete.location());
          }
        }
      }
      Set<Integer> readIds = Sets.newHashSet();
      layout.fields().forEach(field -> readIds.add(field.fieldId()));
      if (rewrite) {
        LocalFiles.prefetch(dataLocations, table.io());
      } else {
        LocalFiles.prefetch(dataLocations, table.io(), readIds);
      }
      LocalFiles.prefetch(deleteLocations, table.io());
      this.equalityKeyType = equalityKeyId == null ? null : layout.type(equalityKeyId);
      this.keyNullable =
          equalityKeyId != null && table.schema().findField(equalityKeyId).isOptional();

      // Big batches decompress cheaply, but a small table cut into a few of them leaves streams
      // idle:
      // a batch holds at most an even share of the table a stream, up to BATCH_ROWS.
      long totalRows = tasks.stream().mapToLong(t -> t.file().recordCount()).sum();
      // Given tasks share the streams with other callers' requests, so they are not cut smaller.
      long batchRows =
          given != null
              ? givenBatchRows
              : Math.max(1, Math.min(BATCH_ROWS, (totalRows + STREAMS - 1) / STREAMS));
      List<FileScanTask> current = Lists.newArrayList();
      long currentRows = 0;
      String currentSet = null;
      // A batch's string columns share one device buffer (stringStride): files batch together only
      // while every string column's decoded bytes fit their share of it. ClickBench's hits has
      // dozens
      // of string columns, and SELECT * over a batch of a few files needed more (q24 failed).
      int strings = layout.stringCount();
      long stringBudget = strings == 0 ? Long.MAX_VALUE : stringBuffer / strings - 2048;
      long[] currentStrings = new long[strings];
      for (FileScanTask task : tasks) {
        long rows = task.file().recordCount();
        long[] taskStrings = stringBytes(task, layout, rows);
        // Files batch together only if they share their equality deletes and their residual.
        // ... and store the projected columns at the same positions (schema evolution moves them),
        // and, for a rewrite, belong to the same partition: an output file is one partition's.
        String set =
            HostDeletes.equalityId(task)
                + "|"
                + task.residual()
                + "|"
                + host.columnLayout(task)
                + (rewrite ? "|" + task.spec().specId() + "/" + task.file().partition() : "")
                + (Ablation.off("batching") ? "|" + task.file().location() : "");
        boolean stringsFit = true;
        for (int c = 0; c < strings; c++) {
          stringsFit &= currentStrings[c] + taskStrings[c] <= stringBudget;
        }
        if (!current.isEmpty()
            && (currentRows + rows > batchRows || !set.equals(currentSet) || !stringsFit)) {
          batches.add(current);
          current = Lists.newArrayList();
          currentRows = 0;
          Arrays.fill(currentStrings, 0);
        }
        current.add(task);
        currentRows += rows;
        currentSet = set;
        for (int c = 0; c < strings; c++) {
          currentStrings[c] += taskStrings[c];
        }
      }
      if (!current.isEmpty()) {
        batches.add(current);
      }
      for (List<FileScanTask> batch : batches) {
        Expression residual = batch.get(0).residual();
        batchFilters.add(
            !filtered
                ? DevicePredicate.alwaysTrue()
                : given != null
                    ? residuals.get(residual)
                    : DevicePredicate.compile(residual, table.schema()));
      }

      long maxRows = 1;
      long maxSet = 1;
      long maxPositions = 1;
      int files = 1;
      for (List<FileScanTask> batch : batches) {
        maxRows = Math.max(maxRows, batch.stream().mapToLong(t -> t.file().recordCount()).sum());
        files = Math.max(files, batch.size());
        long positionRows = 0;
        for (FileScanTask task : batch) {
          for (DeleteFile delete : task.deletes()) {
            if (HostDeletes.readOnDevice(delete, task.file().location())) {
              positionRows += delete.recordCount();
            }
          }
        }
        maxPositions = Math.max(maxPositions, positionRows);
        long setRows = 0;
        for (DeleteFile delete : batch.get(0).deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            setRows += delete.recordCount();
          }
        }
        maxSet = Math.max(maxSet, setRows);
      }
      if (maxRows > Integer.MAX_VALUE / 8
          || maxSet > Integer.MAX_VALUE
          || maxPositions > Integer.MAX_VALUE) {
        throw new UnsupportedOperationException("Batch too large for one device read: " + maxRows);
      }
      // Rounded up to whole thread blocks: TornadoVM's CUDA scheduler picks a block size that
      // divides
      // the loop bound exactly, and a bound with no such divisor (a prime row count, say) gets
      // one-thread blocks -- 28x slower kernels for a batch of 7,987,813 rows. The padding rows are
      // never live.
      this.capacity =
          (int)
              Math.min(
                  Integer.MAX_VALUE / 8,
                  (Math.max(
                              maxRows,
                              given != null && padGiven
                                  ? Math.min(BATCH_ROWS, roundUp(maxRows))
                                  : 0)
                          + 1023)
                      / 1024
                      * 1024);
      keyedIds.addAll(DevicePredicate.fieldIds(filter, table.schema()));
      if (equalityKeyId != null) {
        keyedIds.add(equalityKeyId);
      }
      keyedIds.addAll(keyIds);
      long stride = stringStride(batches, layout, capacity);
      this.stringStride =
          given != null && stride > 1
              ? Math.min(roundUp(stride), stringBuffer / Math.max(1, layout.stringCount()))
              : stride;
      boolean padded = given != null && padGiven;
      this.equalityCapacity = (int) (padded ? roundUp(maxSet) : maxSet);
      this.positionCapacity = (int) (padded ? roundUp(maxPositions) : maxPositions);
      this.maxFiles = padded ? Math.max(64, files) : files;
    }

    /** Each string column's decoded bytes in one file, as {@link #stringStride} counts them. */
    private long[] stringBytes(FileScanTask task, ScanLayout scanLayout, long rows) {
      long[] bytes = new long[scanLayout.stringCount()];
      for (int s = 0; s < bytes.length; s++) {
        int fieldId = scanLayout.fields().get(scanLayout.numericCount() + s).fieldId();
        long file =
            HostDeletes.stringBytes(LocalFiles.path(task.file().location(), table.io()), fieldId);
        bytes[s] = file < 0 ? 16L * rows : file;
      }
      return bytes;
    }

    /**
     * The bytes any string column of any batch decodes to, from the data files' footers
     * (parquet-java records them); a writer that left them out gets 16 bytes a row, and a batch
     * that needs more is refused rather than truncated.
     */
    private long stringStride(
        List<List<FileScanTask>> fileBatches, ScanLayout scanLayout, int rows) {
      if (scanLayout.stringCount() == 0) {
        return 1;
      }
      long most = 0;
      for (List<FileScanTask> batch : fileBatches) {
        for (int s = 0; s < scanLayout.stringCount(); s++) {
          int fieldId = scanLayout.fields().get(scanLayout.numericCount() + s).fieldId();
          long bytes = 0;
          for (FileScanTask task : batch) {
            long file =
                HostDeletes.stringBytes(
                    LocalFiles.path(task.file().location(), table.io()), fieldId);
            if (file < 0) {
              bytes = 16L * rows;
              break;
            }
            bytes += file;
          }
          most = Math.max(most, bytes);
        }
      }
      long stride = (most + 1023) / 1024 * 1024 + 1024;
      if (stride * scanLayout.stringCount() > stringBuffer) {
        throw new UnsupportedOperationException(
            "String columns of " + stride + " bytes a batch are beyond one device buffer");
      }
      return stride;
    }

    /** The next power of two, at least 1024. */
    private static long roundUp(long value) {
      return Math.max(1024, Long.highestOneBit(Math.max(1, value - 1)) << 1);
    }

    /** What decides the shape of the graph, apart from sizes. */
    String signature() {
      StringBuilder signature = new StringBuilder();
      for (int c = 0; c < layout.fields().size(); c++) {
        Types.NestedField field = layout.fields().get(c);
        signature.append(field.fieldId()).append(field.isOptional() ? "?" : "").append(',');
      }
      return signature
          .append(equalityKeyId)
          .append(keyIds)
          .append(keyNullable)
          .append(filtered)
          .toString();
    }
  }

  /**
   * The device buffers, host holders and execution plan of one query shape: read, delete
   * application and live-row kernel, which every shape shares, plus what the shape adds.
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class Region implements AutoCloseable {
    /**
     * Where this region's host buffers live, freed when the region closes. TornadoVM's own arrays
     * come from automatic arenas, which give their memory back only when the garbage collector
     * finds them unreachable: a query engine opening and closing regions for many tables would hold
     * gigabytes of dead buffers until an old-generation collection.
     */
    private final Arena arena = Arena.ofShared();

    private MemorySegment hostSegment(long elements, int bytes) {
      long size =
          uk.ac.manchester.tornado.api.types.arrays.TornadoNativeArray.ARRAY_HEADER
              + elements * bytes;
      return size >= LazyZeroes.THRESHOLD && LazyZeroes.AVAILABLE
          ? LazyZeroes.allocate(size, arena)
          : arena.allocate(size, Long.BYTES);
    }

    final IntArray hostInts(long elements) {
      return IntArray.fromSegmentShallow(hostSegment(elements, Integer.BYTES));
    }

    final LongArray hostLongs(long elements) {
      return LongArray.fromSegmentShallow(hostSegment(elements, Long.BYTES));
    }

    final DoubleArray hostDoubles(long elements) {
      return DoubleArray.fromSegmentShallow(hostSegment(elements, Double.BYTES));
    }

    final ByteArray hostBytes(long elements) {
      return ByteArray.fromSegmentShallow(hostSegment(elements, Byte.BYTES));
    }

    /**
     * Per field of the layout, whether no file of the current batch holds a null there (by its
     * metrics), so that its validity bytes need not come back.
     */
    boolean[] nullFree = new boolean[0];

    final String signature;
    final ScanLayout layout;
    final Integer equalityKeyId;
    final CudfType equalityKeyType;
    final int capacity;
    final int equalityCapacity;
    final int positionCapacity;
    final int maxFiles;

    // The path holders are handed to TornadoVM, which serializes a holder object the first time it
    // transfers it and sizes that buffer then; a holder whose array later grows overflows it. So
    // each holder gets a fixed capacity up front, and a longer path list is refused.
    final StringBuilder path = new StringBuilder(PATH_CAPACITY);
    final long[] rowsHolder = new long[1];
    final int[] columns;
    final int[] probeSizes = new int[2];
    final IntArray meta = hostInts(DeviceKernels.META_SIZE);
    final ByteArray positionBits;
    final StringBuilder positionPath = new StringBuilder(PATH_CAPACITY);
    final long[] positionRowsHolder = new long[1];
    final int[] positionColumn = new int[1];
    final LongArray positions;
    final IntArray segmentStarts;
    final IntArray segmentOffsets;
    final StringBuilder setPath = new StringBuilder(PATH_CAPACITY);
    final long[] setRowsHolder = new long[1];
    final int[] setColumn = new int[1];
    final boolean filtered;
    final IntArray filterTerms =
        hostInts(1 + DevicePredicate.TERM_INTS * DevicePredicate.MAX_TERMS);
    final LongArray filterLongs = hostLongs(DevicePredicate.MAX_CONSTANTS);
    final DoubleArray filterDoubles = hostDoubles(DevicePredicate.MAX_CONSTANTS);
    final ByteArray inSet;
    final IntArray setInts;
    final LongArray setLongs;
    final IntArray sortedInts;
    final LongArray sortedLongs;
    final int[] sortSize = new int[1];
    final IntArray setSize = hostInts(1);
    // Equality deletes over several columns: the set as loaded, then in hash order.
    final int keyColumns;
    final IntArray keySpec;
    final LongArray rawHashes;
    final LongArray rawKeys;
    final ByteArray rawNulls;
    final IntArray keyOrder;
    final int[] keySortSize = {0, 1};
    final IntArray keySetChanged = hostInts(1);
    final LongArray keyHashes;
    final LongArray keyValues;
    final ByteArray keyNulls;
    final IntArray ints;
    final LongArray longs;
    final DoubleArray doubles;
    final ByteArray valid;
    final ByteArray live;
    final int[] stringColumns;
    final IntArray stringOffsets;
    final ByteArray stringChars;
    final IntArray stringErrors;
    final long stringStride;
    final Set<Integer> keyedIds;

    /**
     * Refuses a batch whose filter or key column held a string longer than {@link #STRING_BYTES}:
     * those are compared packed, and a longer value would compare wrongly. Strings only returned to
     * the host can be of any length.
     */
    void checkStrings(TornadoExecutionResult executed) {
      if (stringColumns.length > 0) {
        executed.transferToHost(stringErrors);
        for (int s = 0; s < stringColumns.length; s++) {
          Types.NestedField field = layout.fields().get(layout.numericCount() + s);
          if (stringErrors.get(s) != 0 && keyedIds.contains(field.fieldId())) {
            throw new UnsupportedOperationException(
                "String column "
                    + field.name()
                    + " holds a value longer than "
                    + STRING_BYTES
                    + " bytes; filters and keys on such columns are not supported on the device");
          }
        }
      }
    }

    private TornadoExecutionPlan plan;
    final KernelContext context = new KernelContext();
    private String setOnDevice;

    /**
     * The one thread that builds, runs and closes this region's plan. TornadoVM keys its CUDA
     * streams by plan and thread, but caches a library's context -- and with it the stream cuDF
     * runs on -- by plan alone; a plan run from a second thread would put its copies and kernels on
     * one stream and its cuDF calls on another, with nothing ordering the two.
     */
    final ExecutorService thread =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread worker = new Thread(runnable, "tornadovm-region");
              worker.setDaemon(true);
              return worker;
            });

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Region(Planned planned) {
      this.signature = planned.signature();
      this.filtered = planned.filtered;
      this.layout = planned.layout;
      this.equalityKeyId = planned.equalityKeyId;
      this.equalityKeyType = planned.equalityKeyType;
      this.capacity = planned.capacity;
      this.equalityCapacity = planned.equalityCapacity;
      this.positionCapacity = planned.positionCapacity;
      this.maxFiles = planned.maxFiles;
      if (layout.numericCount() == 0) {
        throw new UnsupportedOperationException("A device scan reads at least one numeric column");
      }
      this.columns = new int[layout.numericCount()];
      this.stringColumns = new int[layout.stringCount()];
      long strings = layout.stringCount();
      this.stringOffsets = hostInts(strings == 0 ? 1 : (int) (strings * (capacity + 1L)));
      this.stringStride = planned.stringStride;
      this.stringChars = hostBytes(strings == 0 ? 1L : strings * stringStride);
      this.stringErrors = hostInts(Math.max(1, strings));
      this.keyedIds = Sets.newHashSet(planned.keyedIds);
      this.probeSizes[0] = capacity;

      boolean equality = equalityKeyId != null;
      this.keyColumns = planned.keyIds.size();
      boolean composite = keyColumns > 1;
      long keyCells = composite ? (long) keyColumns * equalityCapacity : 1L;
      this.keySpec = hostInts(Math.max(1, 3 * keyColumns));
      for (int c = 0; c < keyColumns; c++) {
        int keyId = planned.keyIds.get(c);
        keySpec.set(3 * c, layout.type(keyId) == CudfType.INT32 ? 0 : 1);
        keySpec.set(3 * c + 1, layout.slot(keyId));
        keySpec.set(
            3 * c + 2,
            layout.fields().get(layout.readIndex(keyId)).isOptional()
                ? layout.readIndex(keyId)
                : -1);
      }
      this.rawHashes = hostLongs(composite ? equalityCapacity : 1);
      this.rawKeys = hostLongs(keyCells);
      this.rawNulls = hostBytes(keyCells);
      this.keyOrder = hostInts(composite ? equalityCapacity : 1);
      this.keyHashes = hostLongs(composite ? equalityCapacity : 1);
      this.keyValues = hostLongs(keyCells);
      this.keyNulls = hostBytes(keyCells);
      this.positionBits = hostBytes((capacity + 7) / 8);
      this.positions = hostLongs(positionCapacity);
      this.segmentStarts = hostInts(maxFiles);
      this.segmentOffsets = hostInts(maxFiles);
      this.inSet = hostBytes(equality || composite ? capacity : 1);
      this.setInts = hostInts(equality && equalityKeyType == CudfType.INT32 ? equalityCapacity : 1);
      this.setLongs =
          hostLongs(equality && equalityKeyType == CudfType.INT64 ? equalityCapacity : 1);
      boolean sortedInt = equality && SORTED_PROBE && equalityKeyType == CudfType.INT32;
      boolean sortedLong = equality && SORTED_PROBE && equalityKeyType == CudfType.INT64;
      this.sortedInts = hostInts(sortedInt ? equalityCapacity : 1);
      this.sortedLongs = hostLongs(sortedLong ? equalityCapacity : 1);
      this.ints = hostInts(Math.max(1L, (long) layout.slots(CudfType.INT32) * capacity));
      this.longs = hostLongs(Math.max(1L, (long) layout.slots(CudfType.INT64) * capacity));
      this.doubles = hostDoubles(Math.max(1L, (long) layout.slots(CudfType.FLOAT64) * capacity));
      // The string read writes validity for its columns whether or not they are optional.
      this.valid =
          hostBytes(
              layout.anyOptional() || layout.stringCount() > 0
                  ? (long) layout.fields().size() * capacity
                  : 1L);
      this.live = hostBytes(capacity);
    }

    boolean fits(Planned planned) {
      return signature.equals(planned.signature())
          && capacity >= planned.capacity
          && equalityCapacity >= planned.equalityCapacity
          && positionCapacity >= planned.positionCapacity
          && maxFiles >= planned.maxFiles
          && stringStride >= planned.stringStride;
    }

    /** Threads a block for every kernel of the scan. */
    static final int BLOCK = 256;

    /**
     * The launch shape of every kernel, set here rather than left to TornadoVM's scheduler: each
     * kernel takes a KernelContext and guards its index, and runs on whole blocks of {@link #BLOCK}
     * threads over its own bound -- the batch, the position-delete entries, or the equality-delete
     * set. Names a shape does not use are never looked up.
     */
    private GridScheduler grids() {
      GridScheduler scheduler = new GridScheduler();
      String[] perRow = {
        "packStrings",
        "probe",
        "keyProbe",
        "live",
        "filter",
        "buckets",
        "groups",
        "nulls",
        "gatherInts",
        "gatherLongs",
        "gatherDoubles",
        "gatherValid",
        "groupKeys",
        "values",
        "zorder",
        "sortkeys",
        "lineage"
      };
      for (String task : perRow) {
        scheduler.addWorkerGrid("mor." + task, grid(capacity));
      }
      for (String task : extraTasks()) {
        scheduler.addWorkerGrid("mor." + task, grid(capacity));
      }
      scheduler.addWorkerGrid("mor.deletePositions", grid(positionCapacity));
      scheduler.addWorkerGrid("mor.keyGather", grid(equalityCapacity));
      return scheduler;
    }

    private static WorkerGrid grid(long bound) {
      WorkerGrid grid = new WorkerGrid1D((int) ((Math.max(1, bound) + BLOCK - 1) / BLOCK * BLOCK));
      grid.setLocalWork(BLOCK, 1, 1);
      return grid;
    }

    /** What this shape adds after the live-row kernel. */
    abstract void consumer(TaskGraph graph);

    /** The names of kernels a shape adds beyond the scan's own, each run over the batch's rows. */
    List<String> extraTasks() {
      return List.of();
    }

    TornadoExecutionPlan plan() {
      if (plan == null) {
        TaskGraph graph = shared();
        consumer(graph);
        plan = new TornadoExecutionPlan(graph.snapshot()).withGridScheduler(grids());
      }
      return plan;
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private TaskGraph shared() {
      boolean equality = equalityKeyId != null;
      TaskGraph graph =
          new TaskGraph("mor")
              .transferToDevice(
                  DataTransferMode.EVERY_EXECUTION,
                  meta,
                  positionBits,
                  segmentStarts,
                  segmentOffsets);
      if (equality) {
        graph.transferToDevice(
            DataTransferMode.FIRST_EXECUTION,
            equalityKeyType == CudfType.INT32 ? setInts : setLongs);
      }
      graph.libraryTask(
          "read",
          Cudf::readParquetColumns,
          path,
          0,
          0,
          columns,
          layout.types(),
          rowsHolder,
          capacity,
          ints,
          longs,
          doubles,
          valid,
          layout.anyOptional());
      if (layout.stringCount() > 0) {
        // STRING columns of the same files: cuDF decodes them into offsets and bytes, and a kernel
        // packs each value -- up to 8 bytes -- into its INT64 slot, where filters, group keys and
        // the rest treat it as a number that orders as the string does.
        graph
            .transferToDevice(DataTransferMode.EVERY_EXECUTION, stringErrors)
            .libraryTask(
                "readStrings",
                Cudf::readParquetStringColumns,
                path,
                stringColumns,
                rowsHolder,
                capacity,
                stringOffsets,
                stringChars,
                stringStride,
                valid,
                layout.numericCount())
            .task(
                "packStrings",
                DeviceKernels::packStrings,
                context,
                meta,
                stringOffsets,
                stringChars,
                layout.stringCount(),
                layout.firstStringSlot(),
                capacity,
                (int) stringStride,
                longs,
                stringErrors)
            .transferToHost(DataTransferMode.UNDER_DEMAND, stringErrors);
      }
      // Position-delete files the device reads itself: their pos columns, concatenated.
      graph.libraryTask(
          "readPositions",
          Cudf::readParquetColumns,
          positionPath,
          0,
          0,
          positionColumn,
          new CudfType[] {CudfType.INT64},
          positionRowsHolder,
          positionCapacity,
          hostInts(1),
          positions,
          hostDoubles(1),
          hostBytes(1),
          false);
      int keyValidOffset = 0;
      if (equality) {
        // Equality-delete files the device reads itself, straight into the set; an execution whose
        // set is unchanged reads nothing and probes the copy already there.
        boolean intKey = equalityKeyType == CudfType.INT32;
        graph.libraryTask(
            "readSet",
            Cudf::readParquetColumns,
            setPath,
            0,
            0,
            setColumn,
            // As the file holds it: a decimal key is read unscaled (CudfType.DECIMAL64).
            new CudfType[] {layout.readType(equalityKeyId)},
            setRowsHolder,
            equalityCapacity,
            intKey ? setInts : hostInts(1),
            intKey ? hostLongs(1) : setLongs,
            hostDoubles(1),
            hostBytes(1),
            false);
        // The key is slot 0 of its type, so the probe reads it from offset 0.
        if (SORTED_PROBE) {
          // Sorted once a set (sortSize is 0 when the set has not changed), then a binary search
          // from a kernel for every batch: no hash table rebuilt per batch.
          graph.transferToDevice(DataTransferMode.EVERY_EXECUTION, setSize);
          if (intKey) {
            graph
                .libraryTask("sortSet", Cudf::sortKeys, sortSize, setInts, sortedInts)
                .task(
                    "probe",
                    DeviceKernels::probeSortedInts,
                    context,
                    sortedInts,
                    setSize,
                    ints,
                    inSet);
          } else {
            graph
                .libraryTask("sortSet", Cudf::sortKeys, sortSize, setLongs, sortedLongs)
                .task(
                    "probe",
                    DeviceKernels::probeSortedLongs,
                    context,
                    sortedLongs,
                    setSize,
                    longs,
                    inSet);
          }
        } else if (intKey) {
          graph.libraryTask("probe", Cudf::containedIn, probeSizes, ints, setInts, inSet);
        } else {
          graph.libraryTask("probe", Cudf::containedIn, probeSizes, longs, setLongs, inSet);
        }
        keyValidOffset = layout.readIndex(equalityKeyId) * capacity;
      }
      if (keyColumns > 1) {
        // Several key columns: the set, loaded on the host, is put in hash order on the device when
        // it changes; every batch then binary-searches it by hash and compares the columns.
        graph
            .transferToDevice(
                DataTransferMode.FIRST_EXECUTION, rawHashes, rawKeys, rawNulls, keySpec)
            .transferToDevice(DataTransferMode.EVERY_EXECUTION, keySetChanged, setSize)
            .libraryTask(
                "keySort",
                Cudf::sortedOrderLongs,
                keySortSize,
                rawHashes,
                equalityCapacity,
                keyOrder)
            .task(
                "keyGather",
                DeviceKernels::gatherKeySet,
                context,
                keySetChanged,
                setSize,
                keyOrder,
                rawHashes,
                rawKeys,
                rawNulls,
                keyColumns,
                equalityCapacity,
                keyHashes,
                keyValues,
                keyNulls)
            .task(
                "keyProbe",
                DeviceKernels::probeKeySet,
                context,
                keySpec,
                keyColumns,
                keyHashes,
                keyValues,
                keyNulls,
                setSize,
                equalityCapacity,
                ints,
                longs,
                valid,
                capacity,
                inSet);
      }
      graph
          .task(
              "live",
              DeviceKernels::liveRows,
              context,
              meta,
              positionBits,
              inSet,
              valid,
              keyValidOffset,
              live)
          .task(
              "deletePositions",
              DeviceKernels::deletePositions,
              context,
              positions,
              meta,
              segmentStarts,
              segmentOffsets,
              live);
      if (filtered) {
        graph
            .transferToDevice(
                DataTransferMode.EVERY_EXECUTION, filterTerms, filterLongs, filterDoubles)
            .task(
                "filter",
                DeviceKernels::filterRows,
                context,
                filterTerms,
                filterLongs,
                filterDoubles,
                ints,
                longs,
                doubles,
                valid,
                capacity,
                live);
      }
      return graph;
    }

    /** What a shape sets up per batch, before the shared staging; nothing by default. */
    void stageOutput(HostDeletes.FilePrep prep) {}

    /** Points the plan's holders and small inputs at one batch. */
    void stage(TornadoExecutionPlan executionPlan, HostDeletes.FilePrep prep, boolean keyNullable) {
      stageOutput(prep);
      path.setLength(0);
      hold(path, prep.path());
      rowsHolder[0] = prep.rows();
      System.arraycopy(prep.columns(), 0, columns, 0, columns.length);
      System.arraycopy(prep.columns(), columns.length, stringColumns, 0, stringColumns.length);
      for (int s = 0; s < stringColumns.length; s++) {
        stringErrors.set(s, 0);
      }
      meta.set(DeviceKernels.META_ROWS, (int) prep.rows());

      byte[] bits = prep.positionBits();
      meta.set(DeviceKernels.META_HAS_POSITION_DELETES, bits == null ? 0 : 1);
      if (bits != null) {
        MemorySegment.copy(
            MemorySegment.ofArray(bits), 0, positionBits.getSegment(), 0, bits.length);
      }

      positionPath.setLength(0);
      hold(positionPath, prep.positionPaths());
      positionRowsHolder[0] = prep.positionRows();
      positionColumn[0] = Math.max(0, prep.positionColumn());
      meta.set(DeviceKernels.META_POSITION_ROWS, (int) prep.positionRows());
      meta.set(DeviceKernels.META_POSITION_SEGMENTS, prep.segmentStarts().size());
      for (int g = 0; g < prep.segmentStarts().size(); g++) {
        segmentStarts.set(g, prep.segmentStarts().get(g));
        segmentOffsets.set(g, prep.segmentOffsets().get(g));
      }

      String equalityId = prep.equalityId();
      meta.set(DeviceKernels.META_HAS_EQUALITY_DELETES, equalityId.isEmpty() ? 0 : 1);
      meta.set(DeviceKernels.META_KEY_NULLABLE, keyNullable ? 1 : 0);
      setPath.setLength(0);
      setRowsHolder[0] = 0;
      sortSize[0] = 0;
      keySortSize[0] = 0;
      keySetChanged.set(0, 0);
      if (!equalityId.isEmpty() && !equalityId.equals(setOnDevice)) {
        if (prep.keySet() != null) {
          uploadKeySet(executionPlan, prep.keySet());
          meta.set(DeviceKernels.META_EQUALITY_SET_HAS_NULL, 0);
          meta.set(DeviceKernels.META_KEY_NULLABLE, 0);
        } else if (prep.setPaths() != null) {
          hold(setPath, prep.setPaths());
          setRowsHolder[0] = prep.setRows();
          setColumn[0] = prep.setColumn();
          probeSizes[1] = (int) prep.setRows();
          meta.set(DeviceKernels.META_EQUALITY_SET_HAS_NULL, 0);
          sortSize[0] = probeSizes[1];
          setSize.set(0, probeSizes[1]);
        } else {
          upload(executionPlan, prep.equality());
          meta.set(DeviceKernels.META_EQUALITY_SET_HAS_NULL, prep.equality().hasNull() ? 1 : 0);
          sortSize[0] = probeSizes[1];
          setSize.set(0, probeSizes[1]);
        }
        setOnDevice = equalityId;
      }
    }

    private static void hold(StringBuilder holder, String paths) {
      if (paths.length() >= PATH_CAPACITY) {
        throw new UnsupportedOperationException(
            "A batch names "
                + paths.length()
                + " characters of paths; the limit is "
                + PATH_CAPACITY);
      }
      holder.append(paths);
    }

    /** Puts a host-loaded multi-column set on the device, for this execution to sort. */
    private void uploadKeySet(TornadoExecutionPlan executionPlan, HostDeletes.KeySet set) {
      int entries = set.size();
      MemorySegment.copy(
          MemorySegment.ofArray(set.hashes()),
          0,
          rawHashes.getSegment(),
          0,
          (long) entries * Long.BYTES);
      for (int c = 0; c < keyColumns; c++) {
        MemorySegment.copy(
            MemorySegment.ofArray(set.keys()),
            (long) c * entries * Long.BYTES,
            rawKeys.getSegment(),
            (long) c * equalityCapacity * Long.BYTES,
            (long) entries * Long.BYTES);
        MemorySegment.copy(
            MemorySegment.ofArray(set.nulls()),
            (long) c * entries,
            rawNulls.getSegment(),
            (long) c * equalityCapacity,
            entries);
      }
      executionPlan.transferToDevice(rawHashes, rawKeys, rawNulls);
      keySortSize[0] = entries;
      keySetChanged.set(0, 1);
      setSize.set(0, entries);
    }

    /** Puts a host-loaded equality-delete set on the device. */
    private void upload(TornadoExecutionPlan executionPlan, HostDeletes.EqualitySet set) {
      long[] keys = set.keys();
      probeSizes[1] = keys.length;
      if (equalityKeyType == CudfType.INT32) {
        for (int i = 0; i < keys.length; i++) {
          setInts.set(i, (int) keys[i]);
        }
        executionPlan.transferToDevice(setInts);
      } else {
        MemorySegment.copy(
            MemorySegment.ofArray(keys),
            0,
            setLongs.getSegment(),
            0,
            (long) keys.length * ValueLayout.JAVA_LONG.byteSize());
        executionPlan.transferToDevice(setLongs);
      }
    }

    @Override
    public void close() {
      try {
        thread
            .submit(
                () -> {
                  if (plan != null) {
                    DEVICE_PLANS.writeLock().lock();
                    try {
                      plan.close();
                    } catch (TornadoExecutionPlanException e) {
                      throw new IllegalStateException(e);
                    } finally {
                      DEVICE_PLANS.writeLock().unlock();
                    }
                    plan = null;
                  }
                  // The device copies are gone with the plan; the host buffers go now, not at
                  // the next collection.
                  arena.close();
                })
            .get();
      } catch (ExecutionException e) {
        throw new IllegalStateException("Cannot release the device plan", e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } finally {
        thread.shutdown();
      }
    }
  }

  /** GROUP BY on the device: only each batch's groups come back. */
  private final class AggregateRegion extends Region {
    private final int sumColumns;
    private final int groupOffset;
    private final int buckets;
    private final IntArray groupKeys;
    private final IntArray sumKeys;
    private final DoubleArray sums;
    private final IntArray sumGroups = hostInts(1);
    private final IntArray countKeys;
    private final DoubleArray counts;
    private final IntArray countGroups = hostInts(1);
    private final IntArray validColumn;

    AggregateRegion(Planned planned, String groupColumn, int buckets, String[] sumNames) {
      super(planned);
      this.sumColumns = sumNames.length;
      this.buckets = buckets;
      this.groupOffset =
          groupColumn == null
              ? -1
              : layout.slot(table.schema().findField(groupColumn).fieldId()) * capacity;
      int[] validColumns = new int[sumNames.length];
      for (int c = 0; c < sumNames.length; c++) {
        Types.NestedField field = table.schema().findField(sumNames[c]);
        validColumns[c] = field.isOptional() ? layout.readIndex(field.fieldId()) : -1;
      }
      this.validColumn = IntArray.fromArray(validColumns);
      this.groupKeys = hostInts(capacity);
      this.sumKeys = hostInts(capacity);
      this.sums = hostDoubles((long) sumColumns * capacity);
      this.countKeys = hostInts(capacity);
      this.counts = hostDoubles(capacity);
    }

    @Override
    void consumer(TaskGraph graph) {
      if (buckets > 0) {
        graph.task(
            "buckets",
            DeviceKernels::bucketGroupKeys,
            context,
            live,
            longs,
            groupOffset,
            buckets,
            groupKeys);
      } else {
        graph.task("groups", DeviceKernels::groupKeys, context, live, ints, groupOffset, groupKeys);
      }
      if (layout.anyOptional()) {
        graph
            .transferToDevice(DataTransferMode.FIRST_EXECUTION, validColumn)
            .task(
                "nulls",
                DeviceKernels::zeroNulls,
                context,
                doubles,
                valid,
                validColumn,
                sumColumns,
                capacity);
      }
      graph
          .libraryTask(
              "sum",
              Cudf::groupAggregate,
              capacity,
              sumColumns,
              CudfAggregation.SUM.code(),
              groupKeys,
              doubles,
              sumKeys,
              sums,
              sumGroups)
          .libraryTask(
              "count",
              Cudf::groupAggregate,
              capacity,
              1,
              CudfAggregation.COUNT.code(),
              groupKeys,
              doubles,
              countKeys,
              counts,
              countGroups)
          .transferToHost(
              DataTransferMode.UNDER_DEMAND,
              sumKeys,
              sums,
              sumGroups,
              countKeys,
              counts,
              countGroups);
    }

    /** Copies back one batch's groups and merges them as partial aggregates. */
    long collect(TornadoExecutionResult executed, AggregateResult result) {
      executed.transferToHost(sumGroups, countGroups);
      int groups = sumGroups.get(0);
      int counted = countGroups.get(0);
      if (groups == 0) {
        return 0L;
      }
      executed.transferToHost(new DataRange(sumKeys).withSize(groups));
      executed.transferToHost(new DataRange(countKeys).withSize(counted));
      executed.transferToHost(new DataRange(counts).withSize(counted));
      for (int c = 0; c < sumColumns; c++) {
        executed.transferToHost(
            new DataRange(sums).withOffset((long) c * capacity).withSize(groups));
      }
      Map<Integer, Long> rowsByKey = Maps.newHashMap();
      for (int g = 0; g < counted; g++) {
        rowsByKey.put(countKeys.get(g), (long) counts.get(g));
      }
      long liveRows = 0;
      double[] values = new double[sumColumns];
      for (int g = 0; g < groups; g++) {
        int key = sumKeys.get(g);
        if (key == DeviceKernels.DEAD_GROUP) {
          continue;
        }
        for (int c = 0; c < sumColumns; c++) {
          values[c] = sums.get(c * capacity + g);
        }
        long rows = rowsByKey.get(key);
        result.add(key, rows, values);
        liveRows += rows;
      }
      return liveRows;
    }
  }

  /** Projection on the device: live rows are compacted, and only they come back. */
  private static class MaterializeRegion extends Region {
    private final int intSlots;
    private final int longSlots;
    private final int doubleSlots;
    private final int validSlots;
    private final IntArray compacted;
    private final IntArray liveCount = hostInts(1);
    private final IntArray outInts;
    private final LongArray outLongs;
    private final DoubleArray outDoubles;
    private final ByteArray outValid;
    private final Map<String, Integer> idByName = Maps.newHashMap();

    IntArray liveCount() {
      return liveCount;
    }

    /** The rows the gathers take, in the order they write them: the live rows, by default. */
    IntArray positions() {
      return compacted;
    }

    /** What a shape adds between the compaction and the gathers; nothing by default. */
    void beforeGather(TaskGraph graph) {}

    IntArray outInts() {
      return outInts;
    }

    LongArray outLongs() {
      return outLongs;
    }

    DoubleArray outDoubles() {
      return outDoubles;
    }

    ByteArray outValid() {
      return outValid;
    }

    MaterializeRegion(Planned planned) {
      this(planned, 0);
    }

    /** {@code extraLongSlots} more INT64 output slots, after the gathered ones, for a subclass. */
    MaterializeRegion(Planned planned, int extraLongSlots) {
      super(planned);
      this.intSlots = layout.slots(CudfType.INT32);
      this.longSlots = layout.slots(CudfType.INT64);
      this.doubleSlots = layout.slots(CudfType.FLOAT64);
      this.validSlots = layout.anyOptional() ? layout.fields().size() : 0;
      this.compacted = hostInts(capacity);
      this.outInts = hostInts(Math.max(1L, (long) intSlots * capacity));
      this.outLongs = hostLongs(Math.max(1L, (long) (longSlots + extraLongSlots) * capacity));
      this.outDoubles = hostDoubles(Math.max(1L, (long) doubleSlots * capacity));
      this.outValid = hostBytes(Math.max(1L, (long) validSlots * capacity));
      for (Types.NestedField field : layout.fields()) {
        idByName.put(field.name(), field.fieldId());
      }
    }

    @Override
    void consumer(TaskGraph graph) {
      graph.libraryTask(
          "compact", Cudf::selectedIndices, capacity, capacity, live, compacted, liveCount);
      beforeGather(graph);
      if (intSlots > 0) {
        graph.task(
            "gatherInts",
            DeviceKernels::gatherInts,
            context,
            positions(),
            liveCount,
            ints,
            intSlots,
            capacity,
            outInts);
      }
      if (longSlots > 0) {
        graph.task(
            "gatherLongs",
            DeviceKernels::gatherLongs,
            context,
            positions(),
            liveCount,
            longs,
            longSlots,
            capacity,
            outLongs);
      }
      if (doubleSlots > 0) {
        graph.task(
            "gatherDoubles",
            DeviceKernels::gatherDoubles,
            context,
            positions(),
            liveCount,
            doubles,
            doubleSlots,
            capacity,
            outDoubles);
      }
      if (validSlots > 0) {
        graph.task(
            "gatherValid",
            DeviceKernels::gatherBytes,
            context,
            positions(),
            liveCount,
            valid,
            validSlots,
            capacity,
            outValid);
      }
      graph.transferToHost(
          DataTransferMode.UNDER_DEMAND, liveCount, outInts, outLongs, outDoubles, outValid);
      if (stringColumns.length > 0) {
        // Strings are not gathered on the device: the live rows' indices, offsets and bytes come
        // back, and the host reads each live value where the read left it.
        graph.transferToHost(DataTransferMode.UNDER_DEMAND, compacted, stringOffsets, stringChars);
      }
    }

    /** Copies back one batch's live rows and hands them to the consumer. */
    long collect(TornadoExecutionResult executed, Consumer<LiveRows> consumer) {
      executed.transferToHost(liveCount);
      int rows = liveCount.get(0);
      if (rows > 0) {
        copySlots(executed, outInts, 0, intSlots, rows);
        // A string's INT64 slot holds its packed key, which the host never reads: it takes the
        // string from its offsets and chars.
        copySlots(executed, outLongs, 0, layout.firstStringSlot(), rows);
        copySlots(
            executed, outLongs, layout.firstStringSlot() + layout.stringCount(), longSlots, rows);
        copySlots(executed, outDoubles, 0, doubleSlots, rows);
        for (int c = 0; c < validSlots; c++) {
          if (!nullFree[c]) {
            executed.transferToHost(
                new DataRange(outValid).withOffset((long) c * capacity).withSize(rows));
          }
        }
      }
      consumer.accept(
          new HostBatch(
              layout,
              idByName,
              rows,
              capacity,
              outInts,
              outLongs,
              outDoubles,
              outValid,
              nullFree.clone(),
              rows > 0 ? copyStrings(executed, rows) : null));
      return rows;
    }

    /** A batch's string columns, for the rows the read produced, and the live rows' indices. */
    private HostBatch.StringColumns copyStrings(TornadoExecutionResult executed, int rows) {
      if (stringColumns.length == 0) {
        return null;
      }
      int read = meta.get(DeviceKernels.META_ROWS);
      executed.transferToHost(new DataRange(compacted).withSize(rows));
      byte[][] chars = new byte[stringColumns.length][];
      for (int s = 0; s < stringColumns.length; s++) {
        long offsetsBase = (long) s * (capacity + 1);
        executed.transferToHost(
            new DataRange(stringOffsets).withOffset(offsetsBase).withSize(read + 1L));
        int bytes = stringOffsets.get((int) (offsetsBase + read));
        if (bytes > 0) {
          executed.transferToHost(
              new DataRange(stringChars).withOffset(s * stringStride).withSize(bytes));
        }
        chars[s] =
            stringChars
                .getSegment()
                .asSlice(s * stringStride, bytes)
                .toArray(ValueLayout.JAVA_BYTE);
      }
      return new HostBatch.StringColumns(compacted, stringOffsets, chars, layout.numericCount());
    }

    private void copySlots(
        TornadoExecutionResult executed, TornadoNativeArray array, int from, int to, int rows) {
      for (int s = from; s < to; s++) {
        executed.transferToHost(
            new DataRange(array).withOffset((long) s * capacity).withSize(rows));
      }
    }
  }

  /**
   * Sums of expressions per group of several columns -- the TPC-H Q1 and Q6 shape -- with an
   * optional {@link GroupedStage}'s kernels among the aggregate's.
   */
  private final class GroupedRegion extends Region {
    private final int keys;
    private final int groupKeyColumns;
    // The aggregate's values, then the stage's.
    private final int values;
    // A value's non-null count column after the row count, or -1 when it cannot be null.
    private final int[] countColumn;
    private final int counts;
    private final boolean[] stringKey;
    private final boolean[] nullableKey;
    private final IntArray groupSpec;
    private final IntArray valueSpec;
    private final DoubleArray valueConstants;
    private final LongArray keyColumnsData;
    private final DoubleArray valueData;
    private final LongArray outKeys;
    private final DoubleArray outSums;
    private final IntArray outGroups = hostInts(1);
    private final int[] size;
    private final GroupedStage.Instance stage;
    // The dense group-by (DeviceKernels.denseGroups): the key's field id, or -1 when its type rules
    // it out; a batch takes it when its files' footers bound the key to DENSE_SLOTS values.
    private final int denseKeyId;
    private final LongArray dense = hostLongs(3);
    private final DoubleArray copies;
    private final IntArray misses = hostInts(1);

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    GroupedRegion(
        Planned planned, List<String> groupColumns, List<DeviceValue> sums, GroupedStage stage) {
      super(planned);
      this.keys = groupColumns.size();
      this.groupKeyColumns = Math.max(1, keys);
      this.values = sums.size() + (stage == null ? 0 : stage.values());
      this.stringKey = new boolean[keys];
      this.nullableKey = new boolean[keys];
      this.groupSpec = hostInts(Math.max(1, 3 * keys));
      for (int k = 0; k < keys; k++) {
        Types.NestedField field = column(groupColumns.get(k));
        stringKey[k] = layout.isString(field.fieldId());
        keyedIds.add(field.fieldId()); // grouped on the device: a string key has to pack
        nullableKey[k] = field.isOptional();
        groupSpec.set(3 * k, layout.type(field.fieldId()) == CudfType.INT32 ? 0 : 1);
        groupSpec.set(3 * k + 1, layout.slot(field.fieldId()));
        groupSpec.set(3 * k + 2, field.isOptional() ? layout.readIndex(field.fieldId()) : -1);
      }
      this.valueSpec = hostInts((long) values * DeviceKernels.VALUE_SPEC_INTS);
      this.valueConstants = hostDoubles(3L * values * DeviceValue.MAX_FACTORS);
      this.countColumn = new int[values];
      int nullable = 0;
      for (int v = 0; v < values; v++) {
        // A stage's value has no factors here (its kernels fill it) and always a count column.
        List<DeviceValue.Factor> factors = v < sums.size() ? sums.get(v).factors() : List.of();
        int base = v * DeviceKernels.VALUE_SPEC_INTS;
        valueSpec.set(base, factors.size());
        boolean canBeNull =
            v >= sums.size() || factors.stream().anyMatch(f -> column(f.column()).isOptional());
        countColumn[v] = canBeNull ? nullable++ : -1;
        valueSpec.set(base + DeviceKernels.VALUE_SPEC_INTS - 1, countColumn[v]);
        valueSpec.set(
            base + DeviceKernels.VALUE_SPEC_INTS - 2, v < sums.size() ? sums.get(v).part() : 0);
        for (int f = 0; f < factors.size(); f++) {
          DeviceValue.Factor factor = factors.get(f);
          Types.NestedField field = column(factor.column());
          CudfType type = layout.type(field.fieldId());
          valueSpec.set(
              base + 1 + 3 * f, type == CudfType.INT32 ? 0 : type == CudfType.INT64 ? 1 : 2);
          valueSpec.set(base + 2 + 3 * f, layout.slot(field.fieldId()));
          valueSpec.set(
              base + 3 + 3 * f, field.isOptional() ? layout.readIndex(field.fieldId()) : -1);
          int factorBase = 3 * (v * DeviceValue.MAX_FACTORS + f);
          valueConstants.set(factorBase, factor.offset());
          valueConstants.set(factorBase + 1, factor.scale());
          valueConstants.set(factorBase + 2, factor.coefficient());
        }
      }
      this.counts = nullable;
      this.keyColumnsData = hostLongs((long) groupKeyColumns * capacity);
      this.valueData = hostDoubles((long) (values + 1 + counts) * capacity);
      this.outKeys = hostLongs((long) groupKeyColumns * capacity);
      this.outSums = hostDoubles((long) (values + 1 + counts) * capacity);
      this.size = new int[] {capacity, groupKeyColumns, values + 1 + counts};
      this.stage = stage == null ? null : stage.open(new StageContext());
      Types.NestedField key = keys == 1 ? column(groupColumns.get(0)) : null;
      this.denseKeyId =
          stage == null
                  && key != null
                  && (key.type().typeId() == org.apache.iceberg.types.Type.TypeID.INTEGER
                      || key.type().typeId() == org.apache.iceberg.types.Type.TypeID.LONG)
                  && !Ablation.off("dense")
              ? key.fieldId()
              : -1;
      this.copies =
          hostDoubles(
              denseKeyId < 0
                  ? 1
                  : (long) DeviceKernels.DENSE_COPIES * (DeviceKernels.DENSE_SLOTS + 1) * size[2]);
    }

    /** What the stage sees of this region. */
    private final class StageContext implements GroupedStage.Context {
      @Override
      public KernelContext kernel() {
        return context;
      }

      @Override
      public int capacity() {
        return capacity;
      }

      @Override
      public IntArray ints(long elements) {
        return hostInts(elements);
      }

      @Override
      public LongArray longs(long elements) {
        return hostLongs(elements);
      }

      @Override
      public DoubleArray doubles(long elements) {
        return hostDoubles(elements);
      }

      @Override
      public ByteArray live() {
        return live;
      }

      @Override
      public IntArray intColumns() {
        return ints;
      }

      @Override
      public LongArray longColumns() {
        return longs;
      }

      @Override
      public DoubleArray doubleColumns() {
        return doubles;
      }

      @Override
      public ByteArray validity() {
        return valid;
      }

      @Override
      public LongArray keys() {
        return keyColumnsData;
      }

      @Override
      public DoubleArray values() {
        return valueData;
      }

      @Override
      public int groupKeys() {
        return keys;
      }

      @Override
      public int valueCount() {
        return values;
      }

      @Override
      public int countColumn(int value) {
        return countColumn[value];
      }

      @Override
      public GroupedStage.Column column(String name) {
        Types.NestedField field = DeviceMergeOnReadScan.this.column(name);
        CudfType type = layout.type(field.fieldId());
        return new GroupedStage.Column(
            type == CudfType.INT32 ? 0 : type == CudfType.INT64 ? 1 : 2,
            layout.slot(field.fieldId()),
            field.isOptional() ? layout.readIndex(field.fieldId()) : -1);
      }
    }

    /** Takes a run's stage data, for the next batch. */
    void bind(GroupedStage run) {
      stage.bind(run);
    }

    @Override
    List<String> extraTasks() {
      return stage != null
          ? stage.tasks()
          : denseKeyId >= 0 ? List.of("dense", "denseReduce") : List.of();
    }

    @Override
    void stage(TornadoExecutionPlan executionPlan, HostDeletes.FilePrep prep, boolean keyNullable) {
      super.stage(executionPlan, prep, keyNullable);
      if (stage != null) {
        stage.stage(executionPlan);
      }
      if (denseKeyId >= 0) {
        long[] range = denseRange(prep.tasks());
        dense.set(0, range == null ? 0 : 1);
        dense.set(1, range == null ? 0 : range[0]);
        dense.set(2, range == null ? 0 : range[1] - range[0] + 2);
        misses.set(0, 0);
        size[0] = range == null ? capacity : 0;
      }
    }

    /** The key's smallest and largest value over a batch's files, if few enough to go dense. */
    private long[] denseRange(List<FileScanTask> tasks) {
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (FileScanTask task : tasks) {
        long[] bounds =
            HostDeletes.bounds(LocalFiles.path(task.file().location(), table.io()), denseKeyId);
        if (bounds == null) {
          return null;
        }
        min = Math.min(min, bounds[0]);
        max = Math.max(max, bounds[1]);
      }
      long spread = max - min;
      return min <= max && spread >= 0 && spread < DeviceKernels.DENSE_SLOTS
          ? new long[] {min, max}
          : null;
    }

    @Override
    void consumer(TaskGraph graph) {
      graph.transferToDevice(
          DataTransferMode.FIRST_EXECUTION, groupSpec, valueSpec, valueConstants);
      if (stage != null) {
        stage.afterFilter(graph);
      }
      graph.task(
          "groupKeys",
          DeviceKernels::groupKeyColumns,
          context,
          groupSpec,
          keys,
          live,
          ints,
          longs,
          valid,
          capacity,
          keyColumnsData);
      if (stage != null) {
        stage.afterKeys(graph);
      }
      graph.task(
          "values",
          DeviceKernels::valueColumns,
          context,
          valueSpec,
          valueConstants,
          values,
          live,
          ints,
          longs,
          doubles,
          valid,
          capacity,
          valueData);
      if (stage != null) {
        stage.afterValues(graph);
        if (stage.groups()) {
          return;
        }
      }
      if (denseKeyId >= 0) {
        graph
            .transferToDevice(DataTransferMode.EVERY_EXECUTION, dense, misses)
            .transferToDevice(DataTransferMode.FIRST_EXECUTION, copies)
            .task(
                "dense",
                DeviceKernels::denseGroups,
                context,
                dense,
                groupSpec,
                live,
                ints,
                longs,
                valid,
                capacity,
                valueData,
                size[2],
                copies,
                misses);
      }
      // Rows 0 (size[0]) when the batch went dense: the reduce below writes the groups instead.
      graph.libraryTask(
          "groupBy",
          Cudf::groupAggregateLongs,
          size,
          CudfAggregation.SUM.code(),
          keyColumnsData,
          valueData,
          capacity,
          outKeys,
          outSums,
          outGroups);
      if (denseKeyId >= 0) {
        graph
            .task(
                "denseReduce",
                DeviceKernels::denseReduce,
                context,
                dense,
                size[2],
                copies,
                capacity,
                outKeys,
                outSums,
                outGroups)
            .transferToHost(DataTransferMode.UNDER_DEMAND, misses);
      }
      graph.transferToHost(DataTransferMode.UNDER_DEMAND, outKeys, outSums, outGroups);
    }

    /** Copies back one batch's groups and merges them; returns the batch's live rows. */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    long collect(TornadoExecutionResult executed, GroupedAggregate result) {
      if (stage != null && stage.groups()) {
        return stage.collect(executed, result);
      }
      executed.transferToHost(outGroups);
      int groups = outGroups.get(0);
      if (dense.get(0) != 0) {
        executed.transferToHost(misses);
        if (misses.get(0) != 0) {
          throw new IllegalStateException(
              misses.get(0) + " rows have a key outside their files' footer bounds");
        }
      }
      if (groups == 0) {
        return 0L;
      }
      for (int k = 0; k < groupKeyColumns; k++) {
        executed.transferToHost(
            new DataRange(outKeys).withOffset((long) k * capacity).withSize(groups));
      }
      for (int v = 0; v <= values + counts; v++) {
        executed.transferToHost(
            new DataRange(outSums).withOffset((long) v * capacity).withSize(groups));
      }
      long liveRows = 0;
      double[] sums = new double[values];
      double[] nonNull = new double[values];
      for (int g = 0; g < groups; g++) {
        // A dense slot no row reached has no rows.
        if (outKeys.get(g) == DeviceKernels.DEAD_KEY
            || outSums.get((long) values * capacity + g) == 0.0) {
          continue;
        }
        List<Object> key = Lists.newArrayListWithCapacity(keys);
        for (int k = 0; k < keys; k++) {
          long value = outKeys.get((long) k * capacity + g);
          if (nullableKey[k] && value == DeviceKernels.NULL_KEY) {
            key.add(null);
          } else {
            key.add(stringKey[k] ? ScanLayout.unpackString(value) : (Object) value);
          }
        }
        for (int v = 0; v < values; v++) {
          sums[v] = outSums.get((long) v * capacity + g);
        }
        double rows = outSums.get((long) values * capacity + g);
        for (int v = 0; v < values; v++) {
          nonNull[v] =
              countColumn[v] < 0
                  ? rows
                  : outSums.get((long) (values + 1 + countColumn[v]) * capacity + g);
        }
        result.add(key, sums, rows, nonNull);
        liveRows += (long) rows;
      }
      return liveRows;
    }
  }

  /** Compaction: the live rows of a batch, compacted and written to a new Parquet file. */
  private final class RewriteRegion extends MaterializeRegion {
    private final StringBuilder outputPath = new StringBuilder(PATH_CAPACITY);
    private final String names;
    private final int[] fieldIds;
    private final ParquetColumnType[] types;
    private final boolean[] optional;
    // Per column: a DECIMAL64's precision * 256 + scale, a STRING's read-validity slot.
    private final int[] details;
    // Read when the write runs: {codec, row-group rows, string bytes a column, rows read}.
    private final long[] settings = new long[4];
    private final boolean lineage;
    private final IntArray fileStarts;
    private final IntArray fileCount = hostInts(1);
    private final LongArray firstRowIds;
    private final LongArray sequences;
    private final RowOrder order;
    private final int zColumns;
    private final IntArray zSpec;
    private final IntArray zFlags;
    private final int[] zSize;
    private final LongArray zKeys;
    private final IntArray zOrder;
    private String location;
    // Where cuDF writes the batch's file when its location is on an object store, and the batch's
    // partition spec and partition.
    private Path staged;
    private org.apache.iceberg.PartitionSpec spec;
    private org.apache.iceberg.StructLike partition;

    /**
     * {@code lineage}: a v3 table's rewrite, which also writes each row's {@code _row_id} and
     * {@code _last_updated_sequence_number}, as two INT64 columns after the table's.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    RewriteRegion(Planned planned, boolean lineage, RowOrder order) {
      super(planned, lineage ? 2 : 0);
      this.lineage = lineage;
      this.order = order;
      this.zColumns = order.columns().size();
      this.zSpec = hostInts(Math.max(1, 3 * zColumns));
      this.zFlags = hostInts(Math.max(1, zColumns));
      this.zSize = new int[] {0, order.words()};
      this.zKeys = hostLongs(zColumns == 0 ? 1L : (long) order.words() * capacity);
      this.zOrder = hostInts(zColumns == 0 ? 1 : capacity);
      for (int c = 0; c < zColumns; c++) {
        Types.NestedField field = table.schema().findField(order.columns().get(c));
        if (field == null) {
          throw new IllegalArgumentException("No column " + order.columns().get(c));
        }
        zFlags.set(c, order.flags()[c]);
        CudfType type = layout.type(field.fieldId());
        zSpec.set(3 * c, type == CudfType.INT32 ? 0 : type == CudfType.INT64 ? 1 : 2);
        zSpec.set(3 * c + 1, layout.slot(field.fieldId()));
        zSpec.set(
            3 * c + 2,
            layout.anyOptional() && field.isOptional() ? layout.readIndex(field.fieldId()) : -1);
      }
      this.fileStarts = hostInts(maxFiles);
      this.firstRowIds = hostLongs(maxFiles);
      this.sequences = hostLongs(maxFiles);
      List<Types.NestedField> fields = Lists.newArrayList(layout.fields());
      if (lineage) {
        fields.add(MetadataColumns.ROW_ID);
        fields.add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
      }
      this.fieldIds = new int[fields.size()];
      this.types = new ParquetColumnType[fields.size()];
      this.optional = new boolean[fields.size()];
      this.details = new int[fields.size()];
      StringBuilder joined = new StringBuilder();
      for (int c = 0; c < fields.size(); c++) {
        Types.NestedField field = fields.get(c);
        fieldIds[c] = field.fieldId();
        // Lineage is written for every row, so its columns carry no nulls.
        optional[c] = field.isOptional() && c < layout.fields().size();
        types[c] =
            switch (field.type().typeId()) {
              case INTEGER -> ParquetColumnType.INT32;
              case DATE -> ParquetColumnType.DATE;
              case LONG -> ParquetColumnType.INT64;
              case TIMESTAMP -> ParquetColumnType.TIMESTAMP_MICROS;
              case DOUBLE -> ParquetColumnType.FLOAT64;
              case FLOAT -> ParquetColumnType.FLOAT32;
              case DECIMAL -> {
                Types.DecimalType decimal = (Types.DecimalType) field.type();
                details[c] = decimal.precision() * 256 + decimal.scale();
                yield ParquetColumnType.DECIMAL64;
              }
              case STRING -> {
                // Gathered from the read's offsets and bytes inside the write; its validity is the
                // read's, at the column's read index.
                details[c] = layout.readIndex(field.fieldId());
                yield ParquetColumnType.STRING;
              }
              default ->
                  throw new UnsupportedOperationException(
                      "Cannot write " + field + " on the device");
            };
        if (c > 0) {
          joined.append('\n');
        }
        joined.append(field.name());
      }
      this.names = joined.toString();
    }

    @Override
    IntArray positions() {
      return zColumns == 0 ? super.positions() : zOrder;
    }

    @Override
    void beforeGather(TaskGraph graph) {
      if (zColumns > 0 && !order.zorder()) {
        graph
            .transferToDevice(DataTransferMode.FIRST_EXECUTION, zSpec, zFlags)
            .task(
                "sortkeys",
                DeviceKernels::sortOrderKeys,
                context,
                zSpec,
                zFlags,
                zColumns,
                live,
                ints,
                longs,
                doubles,
                valid,
                capacity,
                zKeys)
            .libraryTask("zsort", Cudf::sortedOrderLongs, zSize, zKeys, capacity, zOrder);
      } else if (zColumns > 0) {
        graph
            .transferToDevice(DataTransferMode.FIRST_EXECUTION, zSpec)
            .task(
                "zorder",
                DeviceKernels::zorderKeys,
                context,
                zSpec,
                zColumns,
                live,
                ints,
                longs,
                doubles,
                valid,
                capacity,
                zKeys)
            .libraryTask("zsort", Cudf::sortedOrderLongs, zSize, zKeys, capacity, zOrder);
      }
    }

    @Override
    void consumer(TaskGraph graph) {
      super.consumer(graph);
      if (lineage) {
        graph
            .transferToDevice(
                DataTransferMode.EVERY_EXECUTION, fileStarts, fileCount, firstRowIds, sequences)
            .task(
                "lineage",
                DeviceKernels::lineage,
                context,
                positions(),
                liveCount(),
                fileStarts,
                fileCount,
                firstRowIds,
                sequences,
                // After the numeric INT64 slots: the writer takes a string from the read, not
                // from the INT64 slot the layout keeps for its packed key.
                layout.firstStringSlot(),
                capacity,
                outLongs());
      }
      settings[0] = codec();
      // Rows a row group: libcudf's default unless set (-Diceberg.tornadovm.write-row-group-rows),
      // e.g. one row group a file for a layout the GPU reads in fewer, larger pieces.
      settings[1] = Long.getLong("iceberg.tornadovm.write-row-group-rows", 0L);
      settings[2] = stringStride;
      graph.libraryTask(
          "write",
          Cudf::writeParquetColumnsWithStrings,
          outputPath,
          names,
          fieldIds,
          types,
          optional,
          details,
          new long[] {-1},
          liveCount(),
          capacity,
          outInts(),
          outLongs(),
          outDoubles(),
          outValid(),
          stringOffsets,
          stringChars,
          valid,
          positions(),
          settings);
    }

    /**
     * The table's Parquet codec ({@code write.parquet.compression-codec}, ZSTD by default), or the
     * {@code iceberg.tornadovm.write-codec} override; the device writer has ZSTD, Snappy and none.
     */
    private int codec() {
      String codec =
          System.getProperty(
              "iceberg.tornadovm.write-codec",
              table
                  .properties()
                  .getOrDefault(
                      org.apache.iceberg.TableProperties.PARQUET_COMPRESSION,
                      org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT));
      return switch (codec.toLowerCase(Locale.ROOT)) {
        case "zstd" -> 2;
        case "snappy" -> 1;
        case "uncompressed", "none" -> 0;
        default ->
            throw new UnsupportedOperationException("The device writer has no " + codec + " codec");
      };
    }

    /** A new file a batch, where Iceberg's location provider puts data files. */
    @Override
    void stageOutput(HostDeletes.FilePrep prep) {
      zSize[0] = (int) prep.rows();
      if (lineage) {
        if (prep.storedLineage()) {
          throw new UnsupportedOperationException(
              "A data file already stores row lineage columns; rewriting it again is not supported");
        }
        List<Integer> starts = prep.fileStarts();
        fileCount.set(0, starts.size());
        for (int f = 0; f < starts.size(); f++) {
          Long firstRowId = prep.firstRowIds().get(f);
          Long sequence = prep.sequences().get(f);
          if (firstRowId == null || sequence == null) {
            throw new UnsupportedOperationException(
                "A data file of a v3 table has no first row id or data sequence number");
          }
          fileStarts.set(f, starts.get(f));
          firstRowIds.set(f, firstRowId);
          sequences.set(f, sequence);
        }
      }
      spec = prep.spec();
      partition = prep.partition();
      settings[3] = prep.rows();
      String name = "gpu-rewrite-" + UUID.randomUUID() + ".parquet";
      location =
          spec.isUnpartitioned()
              ? table.locationProvider().newDataLocation(name)
              : table.locationProvider().newDataLocation(spec, partition, name);
      staged = LocalFiles.isLocal(location) ? null : LocalFiles.stagingFile();
      String local = staged == null ? LocalFiles.path(location, table.io()) : staged.toString();
      try {
        // cuDF opens the file but makes no directories: a new partition's directory is made here.
        Files.createDirectories(Paths.get(local).getParent());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      outputPath.setLength(0);
      outputPath.append(local);
    }

    /** The batch's file as an Iceberg data file, with metrics from the footer libcudf wrote. */
    long collect(TornadoExecutionResult executed, List<DataFile> written) {
      executed.transferToHost(liveCount());
      int rows = liveCount().get(0);
      if (staged != null) {
        if (rows == 0) {
          LocalFiles.discard(staged);
          return 0;
        }
        LocalFiles.upload(staged, location, table.io());
      }
      InputFile file = table.io().newInputFile(location);
      if (rows == 0) {
        table.io().deleteFile(location);
        return 0;
      }
      org.apache.iceberg.Metrics fileMetrics =
          ParquetUtil.fileMetrics(file, org.apache.iceberg.MetricsConfig.forTable(table));
      org.apache.iceberg.DataFiles.Builder builder =
          org.apache.iceberg.DataFiles.builder(spec)
              .withPartition(partition)
              .withPath(location)
              .withFormat(org.apache.iceberg.FileFormat.PARQUET)
              .withFileSizeInBytes(file.getLength())
              .withMetrics(fileMetrics);
      if (!order.zorder()) {
        builder.withSortOrder(table.sortOrders().get(order.sortOrderId()));
      }
      written.add(builder.build());
      return rows;
    }
  }

  /** The positions merge-on-read deletes in each batch's files, for deletion vectors. */
  private static final class DeadRegion extends Region {
    private final ByteArray dead = hostBytes(capacity);
    private final IntArray droppedRows = hostInts(capacity);
    private final IntArray deadCount = hostInts(1);
    private HostDeletes.FilePrep prep;

    DeadRegion(Planned planned) {
      super(planned);
    }

    @Override
    List<String> extraTasks() {
      return List.of("dead");
    }

    @Override
    void consumer(TaskGraph graph) {
      graph
          .task("dead", DeviceKernels::deadRows, context, meta, live, dead)
          .libraryTask(
              "droppedRows",
              Cudf::selectedIndices,
              capacity,
              capacity,
              dead,
              droppedRows,
              deadCount)
          .transferToHost(DataTransferMode.UNDER_DEMAND, deadCount, droppedRows);
    }

    @Override
    void stageOutput(HostDeletes.FilePrep batch) {
      this.prep = batch;
    }

    /** Hands the batch's deleted positions, by data file, to the writer; how many there were. */
    long collect(TornadoExecutionResult executed, PositionSink writer) {
      executed.transferToHost(deadCount);
      int rows = deadCount.get(0);
      if (rows > 0) {
        executed.transferToHost(new DataRange(droppedRows).withSize(rows));
      }
      List<Integer> starts = prep.fileStarts();
      List<FileScanTask> tasks = prep.tasks();
      synchronized (writer) {
        int file = 0;
        // Ascending positions: each file's run follows the previous one's.
        for (int r = 0; r < rows; r++) {
          int position = droppedRows.get(r);
          while (file + 1 < starts.size() && position >= starts.get(file + 1)) {
            file++;
          }
          DataFile data = tasks.get(file).file();
          writer.delete(
              data.location(),
              position - starts.get(file),
              tasks.get(file).spec(),
              data.partition());
        }
      }
      return rows;
    }
  }

  /**
   * Where a maintenance operation's deleted positions go: one deletion vector per data file for a
   * v3 table, one file-scoped position-delete file per data file for a v2 table -- what Iceberg's
   * own writers produce for each version ({@code DeleteGranularity.FILE} for v2).
   */
  interface PositionSink {
    void delete(
        String path,
        long position,
        org.apache.iceberg.PartitionSpec spec,
        org.apache.iceberg.StructLike partition);

    /** Writes what was collected; the delete files written. */
    List<DeleteFile> close() throws IOException;

    static PositionSink vectors(Table table, long operationId) {
      OutputFileFactory files =
          OutputFileFactory.builderFor(table, 1, operationId)
              .format(org.apache.iceberg.FileFormat.PUFFIN)
              .build();
      // Earlier deletes are already in the live mask: nothing to load.
      BaseDVFileWriter writer = new BaseDVFileWriter(files, path -> null);
      return new PositionSink() {
        @Override
        public void delete(
            String path,
            long position,
            org.apache.iceberg.PartitionSpec spec,
            org.apache.iceberg.StructLike partition) {
          writer.delete(path, position, spec, partition);
        }

        @Override
        public List<DeleteFile> close() throws IOException {
          writer.close();
          return writer.result().deleteFiles();
        }
      };
    }

    static PositionSink positionFiles(Table table, long operationId) {
      OutputFileFactory files =
          OutputFileFactory.builderFor(table, 1, operationId)
              .format(org.apache.iceberg.FileFormat.PARQUET)
              .build();
      Map<String, FilePositions> byFile = new TreeMap<>();
      return new PositionSink() {
        @Override
        public void delete(
            String path,
            long position,
            org.apache.iceberg.PartitionSpec spec,
            org.apache.iceberg.StructLike partition) {
          byFile.computeIfAbsent(path, p -> new FilePositions(spec, partition)).add(position);
        }

        @Override
        public List<DeleteFile> close() throws IOException {
          // One delete file a data file, each written on its own thread: Iceberg's generic
          // Parquet writer takes one row at a time (9.5M positions took 4.9 s on one thread).
          // One writer per ~32k positions, up to a core each: small files written on every core
          // contend (2xA10 host, 128 cores, 1M positions over 320 files: 1.25 s on 128 writers,
          // 0.80 s on 32), large ones still gain from all of them (9.9M positions: 1.87 s on 128).
          List<Map.Entry<String, FilePositions>> entries = Lists.newArrayList(byFile.entrySet());
          long positions = 0;
          for (Map.Entry<String, FilePositions> entry : entries) {
            positions += entry.getValue().count;
          }
          int threads =
              (int)
                  Math.min(
                      Math.min(entries.size(), Runtime.getRuntime().availableProcessors()),
                      Math.max(1, (positions + POSITIONS_PER_WRITER - 1) / POSITIONS_PER_WRITER));
          int writersSet = Integer.getInteger("iceberg.tornadovm.positionWriters", 0);
          if (writersSet > 0) {
            threads = Math.min(entries.size(), writersSet);
          }
          ExecutorService writers = Executors.newFixedThreadPool(Math.max(1, threads));
          try {
            List<Future<DeleteFile>> futures = Lists.newArrayList();
            for (Map.Entry<String, FilePositions> entry : entries) {
              futures.add(writers.submit(() -> write(entry.getKey(), entry.getValue())));
            }
            List<DeleteFile> written = Lists.newArrayList();
            for (Future<DeleteFile> future : futures) {
              written.add(future.get());
            }
            return written;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          } catch (ExecutionException e) {
            throw new IOException(e.getCause());
          } finally {
            writers.shutdownNow();
          }
        }

        private DeleteFile write(String path, FilePositions file) throws IOException {
          PositionDelete<org.apache.iceberg.data.Record> row = PositionDelete.create();
          // Full bounds on file_path (forPositionDelete) are what mark the file as scoped to
          // one data file.
          PositionDeleteWriter<org.apache.iceberg.data.Record> writer =
              FormatModelRegistry.<org.apache.iceberg.data.Record>positionDeleteWriteBuilder(
                      org.apache.iceberg.FileFormat.PARQUET,
                      files.newOutputFile(file.spec, file.partition))
                  .spec(file.spec)
                  .partition(file.partition)
                  .metricsConfig(org.apache.iceberg.MetricsConfig.forPositionDelete())
                  .build();
          try (PositionDeleteWriter<org.apache.iceberg.data.Record> closing = writer) {
            Arrays.sort(file.positions, 0, file.count);
            for (int i = 0; i < file.count; i++) {
              closing.write(row.set(path, file.positions[i]));
            }
          }
          return writer.toDeleteFile();
        }
      };
    }
  }

  /** One data file's deleted positions, for a position-delete file. */
  private static final long POSITIONS_PER_WRITER = 32_768;

  private static final class FilePositions {
    private final org.apache.iceberg.PartitionSpec spec;
    private final org.apache.iceberg.StructLike partition;
    private long[] positions = new long[1024];
    private int count = 0;

    FilePositions(org.apache.iceberg.PartitionSpec spec, org.apache.iceberg.StructLike partition) {
      this.spec = spec;
      this.partition = partition;
    }

    void add(long position) {
      if (count == positions.length) {
        positions = Arrays.copyOf(positions, count * 2);
      }
      positions[count++] = position;
    }
  }

  /** Per batch, the smallest distinct theta-sketch hashes of each statistics column. */
  private static final class StatsRegion extends Region {
    private final int statsColumns;
    private final int keep;
    private final IntArray[] specs;
    private final LongArray[] hashes;
    private final LongArray[] sorted;
    private final ByteArray[] firsts;
    private final IntArray[] firstIndices;
    private final IntArray[] counts;
    private final LongArray[] kept;
    private final int[][] sizes;

    StatsRegion(Planned planned, List<Types.NestedField> fields, int keep) {
      super(planned);
      this.statsColumns = fields.size();
      this.keep = keep;
      this.specs = new IntArray[statsColumns];
      this.hashes = new LongArray[statsColumns];
      this.sorted = new LongArray[statsColumns];
      this.firsts = new ByteArray[statsColumns];
      this.firstIndices = new IntArray[statsColumns];
      this.counts = new IntArray[statsColumns];
      this.kept = new LongArray[statsColumns];
      this.sizes = new int[statsColumns][];
      for (int c = 0; c < statsColumns; c++) {
        Types.NestedField field = fields.get(c);
        CudfType type = layout.type(field.fieldId());
        specs[c] = hostInts(3);
        specs[c].set(
            0,
            type == CudfType.INT32
                ? DeviceKernels.STATS_INT
                : type == CudfType.INT64 ? DeviceKernels.STATS_LONG : DeviceKernels.STATS_DOUBLE);
        specs[c].set(1, layout.slot(field.fieldId()));
        specs[c].set(
            2, layout.anyOptional() && field.isOptional() ? layout.readIndex(field.fieldId()) : -1);
        hashes[c] = hostLongs(capacity);
        sorted[c] = hostLongs(capacity);
        firsts[c] = hostBytes(capacity);
        firstIndices[c] = hostInts(capacity);
        counts[c] = hostInts(1);
        kept[c] = hostLongs(keep);
        sizes[c] = new int[] {capacity};
      }
    }

    @Override
    List<String> extraTasks() {
      List<String> tasks = Lists.newArrayList();
      for (int c = 0; c < statsColumns; c++) {
        tasks.add("statsHash" + c);
        tasks.add("statsFirsts" + c);
        tasks.add("statsKeep" + c);
      }
      return tasks;
    }

    @Override
    void consumer(TaskGraph graph) {
      for (int c = 0; c < statsColumns; c++) {
        graph
            .transferToDevice(DataTransferMode.FIRST_EXECUTION, specs[c])
            .task(
                "statsHash" + c,
                DeviceKernels::statsHashes,
                context,
                meta,
                live,
                ints,
                longs,
                doubles,
                valid,
                specs[c],
                capacity,
                hashes[c])
            .libraryTask("statsSort" + c, Cudf::sortKeys, sizes[c], hashes[c], sorted[c])
            .task(
                "statsFirsts" + c,
                DeviceKernels::firstOfRuns,
                context,
                sorted[c],
                capacity,
                firsts[c])
            .libraryTask(
                "statsSelect" + c,
                Cudf::selectedIndices,
                capacity,
                capacity,
                firsts[c],
                firstIndices[c],
                counts[c])
            .task(
                "statsKeep" + c,
                DeviceKernels::gatherFirsts,
                context,
                firstIndices[c],
                counts[c],
                sorted[c],
                keep,
                kept[c])
            .transferToHost(DataTransferMode.UNDER_DEMAND, kept[c]);
      }
    }

    /** Folds the batch's smallest distinct hashes into each column's running set. */
    long collect(TornadoExecutionResult executed, List<NavigableSet<Long>> smallest, int limit) {
      for (int c = 0; c < statsColumns; c++) {
        executed.transferToHost(kept[c]);
        NavigableSet<Long> set = smallest.get(c);
        synchronized (set) {
          for (int j = 0; j < keep; j++) {
            long hash = kept[c].get(j);
            if (hash == Long.MAX_VALUE) {
              break;
            }
            if (set.size() < limit) {
              set.add(hash);
            } else if (hash < set.last() && set.add(hash)) {
              set.pollLast();
            }
          }
        }
      }
      return 0;
    }
  }
}

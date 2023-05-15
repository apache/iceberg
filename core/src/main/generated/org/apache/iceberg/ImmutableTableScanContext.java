package org.apache.iceberg;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.MetricsReporter;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TableScanContext}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTableScanContext.builder()}.
 */
@Generated(from = "TableScanContext", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableTableScanContext extends TableScanContext {
  private final @Nullable Long snapshotId;
  private final Expression rowFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final boolean returnColumnStats;
  private final @Nullable Collection<String> selectedColumns;
  private final @Nullable Schema projectedSchema;
  private final Map<String, String> options;
  private final @Nullable Long fromSnapshotId;
  private final boolean fromSnapshotInclusive;
  private final @Nullable Long toSnapshotId;
  private final ExecutorService planExecutor;
  private transient final boolean planWithCustomizedExecutor;
  private final MetricsReporter metricsReporter;

  private ImmutableTableScanContext(ImmutableTableScanContext.Builder builder) {
    this.snapshotId = builder.snapshotId;
    this.selectedColumns = builder.selectedColumns;
    this.projectedSchema = builder.projectedSchema;
    this.fromSnapshotId = builder.fromSnapshotId;
    this.toSnapshotId = builder.toSnapshotId;
    if (builder.rowFilter != null) {
      initShim.rowFilter(builder.rowFilter);
    }
    if (builder.ignoreResidualsIsSet()) {
      initShim.ignoreResiduals(builder.ignoreResiduals);
    }
    if (builder.caseSensitiveIsSet()) {
      initShim.caseSensitive(builder.caseSensitive);
    }
    if (builder.returnColumnStatsIsSet()) {
      initShim.returnColumnStats(builder.returnColumnStats);
    }
    if (builder.optionsIsSet()) {
      initShim.options(createUnmodifiableMap(false, false, builder.options));
    }
    if (builder.fromSnapshotInclusiveIsSet()) {
      initShim.fromSnapshotInclusive(builder.fromSnapshotInclusive);
    }
    if (builder.planExecutor != null) {
      initShim.planExecutor(builder.planExecutor);
    }
    if (builder.metricsReporter != null) {
      initShim.metricsReporter(builder.metricsReporter);
    }
    this.rowFilter = initShim.rowFilter();
    this.ignoreResiduals = initShim.ignoreResiduals();
    this.caseSensitive = initShim.caseSensitive();
    this.returnColumnStats = initShim.returnColumnStats();
    this.options = initShim.options();
    this.fromSnapshotInclusive = initShim.fromSnapshotInclusive();
    this.planExecutor = initShim.planExecutor();
    this.planWithCustomizedExecutor = initShim.planWithCustomizedExecutor();
    this.metricsReporter = initShim.metricsReporter();
    this.initShim = null;
  }

  private ImmutableTableScanContext(
      @Nullable Long snapshotId,
      Expression rowFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      boolean returnColumnStats,
      @Nullable Collection<String> selectedColumns,
      @Nullable Schema projectedSchema,
      Map<String, String> options,
      @Nullable Long fromSnapshotId,
      boolean fromSnapshotInclusive,
      @Nullable Long toSnapshotId,
      ExecutorService planExecutor,
      MetricsReporter metricsReporter) {
    this.snapshotId = snapshotId;
    initShim.rowFilter(rowFilter);
    initShim.ignoreResiduals(ignoreResiduals);
    initShim.caseSensitive(caseSensitive);
    initShim.returnColumnStats(returnColumnStats);
    this.selectedColumns = selectedColumns;
    this.projectedSchema = projectedSchema;
    initShim.options(options);
    this.fromSnapshotId = fromSnapshotId;
    initShim.fromSnapshotInclusive(fromSnapshotInclusive);
    this.toSnapshotId = toSnapshotId;
    initShim.planExecutor(planExecutor);
    initShim.metricsReporter(metricsReporter);
    this.rowFilter = initShim.rowFilter();
    this.ignoreResiduals = initShim.ignoreResiduals();
    this.caseSensitive = initShim.caseSensitive();
    this.returnColumnStats = initShim.returnColumnStats();
    this.options = initShim.options();
    this.fromSnapshotInclusive = initShim.fromSnapshotInclusive();
    this.planExecutor = initShim.planExecutor();
    this.planWithCustomizedExecutor = initShim.planWithCustomizedExecutor();
    this.metricsReporter = initShim.metricsReporter();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "TableScanContext", generator = "Immutables")
  private final class InitShim {
    private byte rowFilterBuildStage = STAGE_UNINITIALIZED;
    private Expression rowFilter;

    Expression rowFilter() {
      if (rowFilterBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (rowFilterBuildStage == STAGE_UNINITIALIZED) {
        rowFilterBuildStage = STAGE_INITIALIZING;
        this.rowFilter = Objects.requireNonNull(ImmutableTableScanContext.super.rowFilter(), "rowFilter");
        rowFilterBuildStage = STAGE_INITIALIZED;
      }
      return this.rowFilter;
    }

    void rowFilter(Expression rowFilter) {
      this.rowFilter = rowFilter;
      rowFilterBuildStage = STAGE_INITIALIZED;
    }

    private byte ignoreResidualsBuildStage = STAGE_UNINITIALIZED;
    private boolean ignoreResiduals;

    boolean ignoreResiduals() {
      if (ignoreResidualsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (ignoreResidualsBuildStage == STAGE_UNINITIALIZED) {
        ignoreResidualsBuildStage = STAGE_INITIALIZING;
        this.ignoreResiduals = ImmutableTableScanContext.super.ignoreResiduals();
        ignoreResidualsBuildStage = STAGE_INITIALIZED;
      }
      return this.ignoreResiduals;
    }

    void ignoreResiduals(boolean ignoreResiduals) {
      this.ignoreResiduals = ignoreResiduals;
      ignoreResidualsBuildStage = STAGE_INITIALIZED;
    }

    private byte caseSensitiveBuildStage = STAGE_UNINITIALIZED;
    private boolean caseSensitive;

    boolean caseSensitive() {
      if (caseSensitiveBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (caseSensitiveBuildStage == STAGE_UNINITIALIZED) {
        caseSensitiveBuildStage = STAGE_INITIALIZING;
        this.caseSensitive = ImmutableTableScanContext.super.caseSensitive();
        caseSensitiveBuildStage = STAGE_INITIALIZED;
      }
      return this.caseSensitive;
    }

    void caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      caseSensitiveBuildStage = STAGE_INITIALIZED;
    }

    private byte returnColumnStatsBuildStage = STAGE_UNINITIALIZED;
    private boolean returnColumnStats;

    boolean returnColumnStats() {
      if (returnColumnStatsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (returnColumnStatsBuildStage == STAGE_UNINITIALIZED) {
        returnColumnStatsBuildStage = STAGE_INITIALIZING;
        this.returnColumnStats = ImmutableTableScanContext.super.returnColumnStats();
        returnColumnStatsBuildStage = STAGE_INITIALIZED;
      }
      return this.returnColumnStats;
    }

    void returnColumnStats(boolean returnColumnStats) {
      this.returnColumnStats = returnColumnStats;
      returnColumnStatsBuildStage = STAGE_INITIALIZED;
    }

    private byte optionsBuildStage = STAGE_UNINITIALIZED;
    private Map<String, String> options;

    Map<String, String> options() {
      if (optionsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (optionsBuildStage == STAGE_UNINITIALIZED) {
        optionsBuildStage = STAGE_INITIALIZING;
        this.options = createUnmodifiableMap(true, false, ImmutableTableScanContext.super.options());
        optionsBuildStage = STAGE_INITIALIZED;
      }
      return this.options;
    }

    void options(Map<String, String> options) {
      this.options = options;
      optionsBuildStage = STAGE_INITIALIZED;
    }

    private byte fromSnapshotInclusiveBuildStage = STAGE_UNINITIALIZED;
    private boolean fromSnapshotInclusive;

    boolean fromSnapshotInclusive() {
      if (fromSnapshotInclusiveBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (fromSnapshotInclusiveBuildStage == STAGE_UNINITIALIZED) {
        fromSnapshotInclusiveBuildStage = STAGE_INITIALIZING;
        this.fromSnapshotInclusive = ImmutableTableScanContext.super.fromSnapshotInclusive();
        fromSnapshotInclusiveBuildStage = STAGE_INITIALIZED;
      }
      return this.fromSnapshotInclusive;
    }

    void fromSnapshotInclusive(boolean fromSnapshotInclusive) {
      this.fromSnapshotInclusive = fromSnapshotInclusive;
      fromSnapshotInclusiveBuildStage = STAGE_INITIALIZED;
    }

    private byte planExecutorBuildStage = STAGE_UNINITIALIZED;
    private ExecutorService planExecutor;

    ExecutorService planExecutor() {
      if (planExecutorBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (planExecutorBuildStage == STAGE_UNINITIALIZED) {
        planExecutorBuildStage = STAGE_INITIALIZING;
        this.planExecutor = Objects.requireNonNull(ImmutableTableScanContext.super.planExecutor(), "planExecutor");
        planExecutorBuildStage = STAGE_INITIALIZED;
      }
      return this.planExecutor;
    }

    void planExecutor(ExecutorService planExecutor) {
      this.planExecutor = planExecutor;
      planExecutorBuildStage = STAGE_INITIALIZED;
    }

    private byte planWithCustomizedExecutorBuildStage = STAGE_UNINITIALIZED;
    private boolean planWithCustomizedExecutor;

    boolean planWithCustomizedExecutor() {
      if (planWithCustomizedExecutorBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (planWithCustomizedExecutorBuildStage == STAGE_UNINITIALIZED) {
        planWithCustomizedExecutorBuildStage = STAGE_INITIALIZING;
        this.planWithCustomizedExecutor = ImmutableTableScanContext.super.planWithCustomizedExecutor();
        planWithCustomizedExecutorBuildStage = STAGE_INITIALIZED;
      }
      return this.planWithCustomizedExecutor;
    }

    private byte metricsReporterBuildStage = STAGE_UNINITIALIZED;
    private MetricsReporter metricsReporter;

    MetricsReporter metricsReporter() {
      if (metricsReporterBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (metricsReporterBuildStage == STAGE_UNINITIALIZED) {
        metricsReporterBuildStage = STAGE_INITIALIZING;
        this.metricsReporter = Objects.requireNonNull(ImmutableTableScanContext.super.metricsReporter(), "metricsReporter");
        metricsReporterBuildStage = STAGE_INITIALIZED;
      }
      return this.metricsReporter;
    }

    void metricsReporter(MetricsReporter metricsReporter) {
      this.metricsReporter = metricsReporter;
      metricsReporterBuildStage = STAGE_INITIALIZED;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (rowFilterBuildStage == STAGE_INITIALIZING) attributes.add("rowFilter");
      if (ignoreResidualsBuildStage == STAGE_INITIALIZING) attributes.add("ignoreResiduals");
      if (caseSensitiveBuildStage == STAGE_INITIALIZING) attributes.add("caseSensitive");
      if (returnColumnStatsBuildStage == STAGE_INITIALIZING) attributes.add("returnColumnStats");
      if (optionsBuildStage == STAGE_INITIALIZING) attributes.add("options");
      if (fromSnapshotInclusiveBuildStage == STAGE_INITIALIZING) attributes.add("fromSnapshotInclusive");
      if (planExecutorBuildStage == STAGE_INITIALIZING) attributes.add("planExecutor");
      if (planWithCustomizedExecutorBuildStage == STAGE_INITIALIZING) attributes.add("planWithCustomizedExecutor");
      if (metricsReporterBuildStage == STAGE_INITIALIZING) attributes.add("metricsReporter");
      return "Cannot build TableScanContext, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code snapshotId} attribute
   */
  @Override
  public @Nullable Long snapshotId() {
    return snapshotId;
  }

  /**
   * @return The value of the {@code rowFilter} attribute
   */
  @Override
  public Expression rowFilter() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.rowFilter()
        : this.rowFilter;
  }

  /**
   * @return The value of the {@code ignoreResiduals} attribute
   */
  @Override
  public boolean ignoreResiduals() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.ignoreResiduals()
        : this.ignoreResiduals;
  }

  /**
   * @return The value of the {@code caseSensitive} attribute
   */
  @Override
  public boolean caseSensitive() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.caseSensitive()
        : this.caseSensitive;
  }

  /**
   * @return The value of the {@code returnColumnStats} attribute
   */
  @Override
  public boolean returnColumnStats() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.returnColumnStats()
        : this.returnColumnStats;
  }

  /**
   * @return The value of the {@code selectedColumns} attribute
   */
  @Override
  public @Nullable Collection<String> selectedColumns() {
    return selectedColumns;
  }

  /**
   * @return The value of the {@code projectedSchema} attribute
   */
  @Override
  public @Nullable Schema projectedSchema() {
    return projectedSchema;
  }

  /**
   * @return The value of the {@code options} attribute
   */
  @Override
  public Map<String, String> options() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.options()
        : this.options;
  }

  /**
   * @return The value of the {@code fromSnapshotId} attribute
   */
  @Override
  public @Nullable Long fromSnapshotId() {
    return fromSnapshotId;
  }

  /**
   * @return The value of the {@code fromSnapshotInclusive} attribute
   */
  @Override
  public boolean fromSnapshotInclusive() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.fromSnapshotInclusive()
        : this.fromSnapshotInclusive;
  }

  /**
   * @return The value of the {@code toSnapshotId} attribute
   */
  @Override
  public @Nullable Long toSnapshotId() {
    return toSnapshotId;
  }

  /**
   * @return The value of the {@code planExecutor} attribute
   */
  @Override
  public ExecutorService planExecutor() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.planExecutor()
        : this.planExecutor;
  }

  /**
   * @return The computed-at-construction value of the {@code planWithCustomizedExecutor} attribute
   */
  @Override
  boolean planWithCustomizedExecutor() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.planWithCustomizedExecutor()
        : this.planWithCustomizedExecutor;
  }

  /**
   * @return The value of the {@code metricsReporter} attribute
   */
  @Override
  public MetricsReporter metricsReporter() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.metricsReporter()
        : this.metricsReporter;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#snapshotId() snapshotId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for snapshotId (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withSnapshotId(@Nullable Long value) {
    if (Objects.equals(this.snapshotId, value)) return this;
    return new ImmutableTableScanContext(
        value,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#rowFilter() rowFilter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for rowFilter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withRowFilter(Expression value) {
    if (this.rowFilter == value) return this;
    Expression newValue = Objects.requireNonNull(value, "rowFilter");
    return new ImmutableTableScanContext(
        this.snapshotId,
        newValue,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#ignoreResiduals() ignoreResiduals} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for ignoreResiduals
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withIgnoreResiduals(boolean value) {
    if (this.ignoreResiduals == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        value,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#caseSensitive() caseSensitive} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for caseSensitive
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withCaseSensitive(boolean value) {
    if (this.caseSensitive == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        value,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#returnColumnStats() returnColumnStats} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for returnColumnStats
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withReturnColumnStats(boolean value) {
    if (this.returnColumnStats == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        value,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#selectedColumns() selectedColumns} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for selectedColumns (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withSelectedColumns(@Nullable Collection<String> value) {
    if (this.selectedColumns == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        value,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#projectedSchema() projectedSchema} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for projectedSchema (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withProjectedSchema(@Nullable Schema value) {
    if (this.projectedSchema == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        value,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by replacing the {@link TableScanContext#options() options} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the options map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableTableScanContext withOptions(Map<String, ? extends String> entries) {
    if (this.options == entries) return this;
    Map<String, String> newValue = createUnmodifiableMap(true, false, entries);
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        newValue,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#fromSnapshotId() fromSnapshotId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fromSnapshotId (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withFromSnapshotId(@Nullable Long value) {
    if (Objects.equals(this.fromSnapshotId, value)) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        value,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#fromSnapshotInclusive() fromSnapshotInclusive} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for fromSnapshotInclusive
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withFromSnapshotInclusive(boolean value) {
    if (this.fromSnapshotInclusive == value) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        value,
        this.toSnapshotId,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#toSnapshotId() toSnapshotId} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for toSnapshotId (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withToSnapshotId(@Nullable Long value) {
    if (Objects.equals(this.toSnapshotId, value)) return this;
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        value,
        this.planExecutor,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#planExecutor() planExecutor} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for planExecutor
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withPlanExecutor(ExecutorService value) {
    if (this.planExecutor == value) return this;
    ExecutorService newValue = Objects.requireNonNull(value, "planExecutor");
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        newValue,
        this.metricsReporter);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TableScanContext#metricsReporter() metricsReporter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for metricsReporter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTableScanContext withMetricsReporter(MetricsReporter value) {
    if (this.metricsReporter == value) return this;
    MetricsReporter newValue = Objects.requireNonNull(value, "metricsReporter");
    return new ImmutableTableScanContext(
        this.snapshotId,
        this.rowFilter,
        this.ignoreResiduals,
        this.caseSensitive,
        this.returnColumnStats,
        this.selectedColumns,
        this.projectedSchema,
        this.options,
        this.fromSnapshotId,
        this.fromSnapshotInclusive,
        this.toSnapshotId,
        this.planExecutor,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTableScanContext} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTableScanContext
        && equalTo(0, (ImmutableTableScanContext) another);
  }

  private boolean equalTo(int synthetic, ImmutableTableScanContext another) {
    return Objects.equals(snapshotId, another.snapshotId)
        && rowFilter.equals(another.rowFilter)
        && ignoreResiduals == another.ignoreResiduals
        && caseSensitive == another.caseSensitive
        && returnColumnStats == another.returnColumnStats
        && Objects.equals(selectedColumns, another.selectedColumns)
        && Objects.equals(projectedSchema, another.projectedSchema)
        && options.equals(another.options)
        && Objects.equals(fromSnapshotId, another.fromSnapshotId)
        && fromSnapshotInclusive == another.fromSnapshotInclusive
        && Objects.equals(toSnapshotId, another.toSnapshotId)
        && planExecutor.equals(another.planExecutor)
        && planWithCustomizedExecutor == another.planWithCustomizedExecutor
        && metricsReporter.equals(another.metricsReporter);
  }

  /**
   * Computes a hash code from attributes: {@code snapshotId}, {@code rowFilter}, {@code ignoreResiduals}, {@code caseSensitive}, {@code returnColumnStats}, {@code selectedColumns}, {@code projectedSchema}, {@code options}, {@code fromSnapshotId}, {@code fromSnapshotInclusive}, {@code toSnapshotId}, {@code planExecutor}, {@code planWithCustomizedExecutor}, {@code metricsReporter}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Objects.hashCode(snapshotId);
    h += (h << 5) + rowFilter.hashCode();
    h += (h << 5) + Boolean.hashCode(ignoreResiduals);
    h += (h << 5) + Boolean.hashCode(caseSensitive);
    h += (h << 5) + Boolean.hashCode(returnColumnStats);
    h += (h << 5) + Objects.hashCode(selectedColumns);
    h += (h << 5) + Objects.hashCode(projectedSchema);
    h += (h << 5) + options.hashCode();
    h += (h << 5) + Objects.hashCode(fromSnapshotId);
    h += (h << 5) + Boolean.hashCode(fromSnapshotInclusive);
    h += (h << 5) + Objects.hashCode(toSnapshotId);
    h += (h << 5) + planExecutor.hashCode();
    h += (h << 5) + Boolean.hashCode(planWithCustomizedExecutor);
    h += (h << 5) + metricsReporter.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code TableScanContext} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TableScanContext{"
        + "snapshotId=" + snapshotId
        + ", rowFilter=" + rowFilter
        + ", ignoreResiduals=" + ignoreResiduals
        + ", caseSensitive=" + caseSensitive
        + ", returnColumnStats=" + returnColumnStats
        + ", selectedColumns=" + selectedColumns
        + ", projectedSchema=" + projectedSchema
        + ", options=" + options
        + ", fromSnapshotId=" + fromSnapshotId
        + ", fromSnapshotInclusive=" + fromSnapshotInclusive
        + ", toSnapshotId=" + toSnapshotId
        + ", planExecutor=" + planExecutor
        + ", planWithCustomizedExecutor=" + planWithCustomizedExecutor
        + ", metricsReporter=" + metricsReporter
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link TableScanContext} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TableScanContext instance
   */
  public static ImmutableTableScanContext copyOf(TableScanContext instance) {
    if (instance instanceof ImmutableTableScanContext) {
      return (ImmutableTableScanContext) instance;
    }
    return ImmutableTableScanContext.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTableScanContext ImmutableTableScanContext}.
   * <pre>
   * ImmutableTableScanContext.builder()
   *    .snapshotId(Long | null) // nullable {@link TableScanContext#snapshotId() snapshotId}
   *    .rowFilter(org.apache.iceberg.expressions.Expression) // optional {@link TableScanContext#rowFilter() rowFilter}
   *    .ignoreResiduals(boolean) // optional {@link TableScanContext#ignoreResiduals() ignoreResiduals}
   *    .caseSensitive(boolean) // optional {@link TableScanContext#caseSensitive() caseSensitive}
   *    .returnColumnStats(boolean) // optional {@link TableScanContext#returnColumnStats() returnColumnStats}
   *    .selectedColumns(Collection&amp;lt;String&amp;gt; | null) // nullable {@link TableScanContext#selectedColumns() selectedColumns}
   *    .projectedSchema(org.apache.iceberg.Schema | null) // nullable {@link TableScanContext#projectedSchema() projectedSchema}
   *    .putOptions|putAllOptions(String =&gt; String) // {@link TableScanContext#options() options} mappings
   *    .fromSnapshotId(Long | null) // nullable {@link TableScanContext#fromSnapshotId() fromSnapshotId}
   *    .fromSnapshotInclusive(boolean) // optional {@link TableScanContext#fromSnapshotInclusive() fromSnapshotInclusive}
   *    .toSnapshotId(Long | null) // nullable {@link TableScanContext#toSnapshotId() toSnapshotId}
   *    .planExecutor(concurrent.ExecutorService) // optional {@link TableScanContext#planExecutor() planExecutor}
   *    .metricsReporter(org.apache.iceberg.metrics.MetricsReporter) // optional {@link TableScanContext#metricsReporter() metricsReporter}
   *    .build();
   * </pre>
   * @return A new ImmutableTableScanContext builder
   */
  public static ImmutableTableScanContext.Builder builder() {
    return new ImmutableTableScanContext.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTableScanContext ImmutableTableScanContext}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TableScanContext", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long OPT_BIT_IGNORE_RESIDUALS = 0x1L;
    private static final long OPT_BIT_CASE_SENSITIVE = 0x2L;
    private static final long OPT_BIT_RETURN_COLUMN_STATS = 0x4L;
    private static final long OPT_BIT_OPTIONS = 0x8L;
    private static final long OPT_BIT_FROM_SNAPSHOT_INCLUSIVE = 0x10L;
    private long optBits;

    private @Nullable Long snapshotId;
    private @Nullable Expression rowFilter;
    private boolean ignoreResiduals;
    private boolean caseSensitive;
    private boolean returnColumnStats;
    private @Nullable Collection<String> selectedColumns;
    private @Nullable Schema projectedSchema;
    private Map<String, String> options = new LinkedHashMap<String, String>();
    private @Nullable Long fromSnapshotId;
    private boolean fromSnapshotInclusive;
    private @Nullable Long toSnapshotId;
    private @Nullable ExecutorService planExecutor;
    private @Nullable MetricsReporter metricsReporter;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TableScanContext} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(TableScanContext instance) {
      Objects.requireNonNull(instance, "instance");
      @Nullable Long snapshotIdValue = instance.snapshotId();
      if (snapshotIdValue != null) {
        snapshotId(snapshotIdValue);
      }
      rowFilter(instance.rowFilter());
      ignoreResiduals(instance.ignoreResiduals());
      caseSensitive(instance.caseSensitive());
      returnColumnStats(instance.returnColumnStats());
      @Nullable Collection<String> selectedColumnsValue = instance.selectedColumns();
      if (selectedColumnsValue != null) {
        selectedColumns(selectedColumnsValue);
      }
      @Nullable Schema projectedSchemaValue = instance.projectedSchema();
      if (projectedSchemaValue != null) {
        projectedSchema(projectedSchemaValue);
      }
      putAllOptions(instance.options());
      @Nullable Long fromSnapshotIdValue = instance.fromSnapshotId();
      if (fromSnapshotIdValue != null) {
        fromSnapshotId(fromSnapshotIdValue);
      }
      fromSnapshotInclusive(instance.fromSnapshotInclusive());
      @Nullable Long toSnapshotIdValue = instance.toSnapshotId();
      if (toSnapshotIdValue != null) {
        toSnapshotId(toSnapshotIdValue);
      }
      planExecutor(instance.planExecutor());
      metricsReporter(instance.metricsReporter());
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#snapshotId() snapshotId} attribute.
     * @param snapshotId The value for snapshotId (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder snapshotId(@Nullable Long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#rowFilter() rowFilter} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#rowFilter() rowFilter}.</em>
     * @param rowFilter The value for rowFilter 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder rowFilter(Expression rowFilter) {
      this.rowFilter = Objects.requireNonNull(rowFilter, "rowFilter");
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#ignoreResiduals() ignoreResiduals} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#ignoreResiduals() ignoreResiduals}.</em>
     * @param ignoreResiduals The value for ignoreResiduals 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder ignoreResiduals(boolean ignoreResiduals) {
      this.ignoreResiduals = ignoreResiduals;
      optBits |= OPT_BIT_IGNORE_RESIDUALS;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#caseSensitive() caseSensitive} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#caseSensitive() caseSensitive}.</em>
     * @param caseSensitive The value for caseSensitive 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      optBits |= OPT_BIT_CASE_SENSITIVE;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#returnColumnStats() returnColumnStats} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#returnColumnStats() returnColumnStats}.</em>
     * @param returnColumnStats The value for returnColumnStats 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder returnColumnStats(boolean returnColumnStats) {
      this.returnColumnStats = returnColumnStats;
      optBits |= OPT_BIT_RETURN_COLUMN_STATS;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#selectedColumns() selectedColumns} attribute.
     * @param selectedColumns The value for selectedColumns (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder selectedColumns(@Nullable Collection<String> selectedColumns) {
      this.selectedColumns = selectedColumns;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#projectedSchema() projectedSchema} attribute.
     * @param projectedSchema The value for projectedSchema (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder projectedSchema(@Nullable Schema projectedSchema) {
      this.projectedSchema = projectedSchema;
      return this;
    }

    /**
     * Put one entry to the {@link TableScanContext#options() options} map.
     * @param key The key in the options map
     * @param value The associated value in the options map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putOptions(String key, String value) {
      this.options.put(
          Objects.requireNonNull(key, "options key"),
          value == null ? Objects.requireNonNull(value, "options value for key: " + key) : value);
      optBits |= OPT_BIT_OPTIONS;
      return this;
    }

    /**
     * Put one entry to the {@link TableScanContext#options() options} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putOptions(Map.Entry<String, ? extends String> entry) {
      String k = entry.getKey();
      String v = entry.getValue();
      this.options.put(
          Objects.requireNonNull(k, "options key"),
          v == null ? Objects.requireNonNull(v, "options value for key: " + k) : v);
      optBits |= OPT_BIT_OPTIONS;
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link TableScanContext#options() options} map. Nulls are not permitted
     * @param entries The entries that will be added to the options map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder options(Map<String, ? extends String> entries) {
      this.options.clear();
      optBits |= OPT_BIT_OPTIONS;
      return putAllOptions(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link TableScanContext#options() options} map. Nulls are not permitted
     * @param entries The entries that will be added to the options map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllOptions(Map<String, ? extends String> entries) {
      for (Map.Entry<String, ? extends String> e : entries.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        this.options.put(
            Objects.requireNonNull(k, "options key"),
            v == null ? Objects.requireNonNull(v, "options value for key: " + k) : v);
      }
      optBits |= OPT_BIT_OPTIONS;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#fromSnapshotId() fromSnapshotId} attribute.
     * @param fromSnapshotId The value for fromSnapshotId (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fromSnapshotId(@Nullable Long fromSnapshotId) {
      this.fromSnapshotId = fromSnapshotId;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#fromSnapshotInclusive() fromSnapshotInclusive} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#fromSnapshotInclusive() fromSnapshotInclusive}.</em>
     * @param fromSnapshotInclusive The value for fromSnapshotInclusive 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fromSnapshotInclusive(boolean fromSnapshotInclusive) {
      this.fromSnapshotInclusive = fromSnapshotInclusive;
      optBits |= OPT_BIT_FROM_SNAPSHOT_INCLUSIVE;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#toSnapshotId() toSnapshotId} attribute.
     * @param toSnapshotId The value for toSnapshotId (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder toSnapshotId(@Nullable Long toSnapshotId) {
      this.toSnapshotId = toSnapshotId;
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#planExecutor() planExecutor} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#planExecutor() planExecutor}.</em>
     * @param planExecutor The value for planExecutor 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder planExecutor(ExecutorService planExecutor) {
      this.planExecutor = Objects.requireNonNull(planExecutor, "planExecutor");
      return this;
    }

    /**
     * Initializes the value for the {@link TableScanContext#metricsReporter() metricsReporter} attribute.
     * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link TableScanContext#metricsReporter() metricsReporter}.</em>
     * @param metricsReporter The value for metricsReporter 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder metricsReporter(MetricsReporter metricsReporter) {
      this.metricsReporter = Objects.requireNonNull(metricsReporter, "metricsReporter");
      return this;
    }

    /**
     * Builds a new {@link ImmutableTableScanContext ImmutableTableScanContext}.
     * @return An immutable instance of TableScanContext
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTableScanContext build() {
      return new ImmutableTableScanContext(this);
    }

    private boolean ignoreResidualsIsSet() {
      return (optBits & OPT_BIT_IGNORE_RESIDUALS) != 0;
    }

    private boolean caseSensitiveIsSet() {
      return (optBits & OPT_BIT_CASE_SENSITIVE) != 0;
    }

    private boolean returnColumnStatsIsSet() {
      return (optBits & OPT_BIT_RETURN_COLUMN_STATS) != 0;
    }

    private boolean optionsIsSet() {
      return (optBits & OPT_BIT_OPTIONS) != 0;
    }

    private boolean fromSnapshotInclusiveIsSet() {
      return (optBits & OPT_BIT_FROM_SNAPSHOT_INCLUSIVE) != 0;
    }
  }

  private static <K, V> Map<K, V> createUnmodifiableMap(boolean checkNulls, boolean skipNulls, Map<? extends K, ? extends V> map) {
    switch (map.size()) {
    case 0: return Collections.emptyMap();
    case 1: {
      Map.Entry<? extends K, ? extends V> e = map.entrySet().iterator().next();
      K k = e.getKey();
      V v = e.getValue();
      if (checkNulls) {
        Objects.requireNonNull(k, "key");
        if (v == null) Objects.requireNonNull(v, "value for key: " + k);
      }
      if (skipNulls && (k == null || v == null)) {
        return Collections.emptyMap();
      }
      return Collections.singletonMap(k, v);
    }
    default: {
      Map<K, V> linkedMap = new LinkedHashMap<>(map.size());
      if (skipNulls || checkNulls) {
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
          K k = e.getKey();
          V v = e.getValue();
          if (skipNulls) {
            if (k == null || v == null) continue;
          } else if (checkNulls) {
            Objects.requireNonNull(k, "key");
            if (v == null) Objects.requireNonNull(v, "value for key: " + k);
          }
          linkedMap.put(k, v);
        }
      } else {
        linkedMap.putAll(map);
      }
      return Collections.unmodifiableMap(linkedMap);
    }
    }
  }
}

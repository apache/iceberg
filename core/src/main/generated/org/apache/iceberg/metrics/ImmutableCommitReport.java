package org.apache.iceberg.metrics;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CommitReport}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCommitReport.builder()}.
 */
@Generated(from = "CommitReport", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableCommitReport implements CommitReport {
  private final String tableName;
  private final long snapshotId;
  private final long sequenceNumber;
  private final String operation;
  private final CommitMetricsResult commitMetrics;
  private final Map<String, String> metadata;

  private ImmutableCommitReport(
      String tableName,
      long snapshotId,
      long sequenceNumber,
      String operation,
      CommitMetricsResult commitMetrics,
      Map<String, String> metadata) {
    this.tableName = tableName;
    this.snapshotId = snapshotId;
    this.sequenceNumber = sequenceNumber;
    this.operation = operation;
    this.commitMetrics = commitMetrics;
    this.metadata = metadata;
  }

  /**
   * @return The value of the {@code tableName} attribute
   */
  @Override
  public String tableName() {
    return tableName;
  }

  /**
   * @return The value of the {@code snapshotId} attribute
   */
  @Override
  public long snapshotId() {
    return snapshotId;
  }

  /**
   * @return The value of the {@code sequenceNumber} attribute
   */
  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  /**
   * @return The value of the {@code operation} attribute
   */
  @Override
  public String operation() {
    return operation;
  }

  /**
   * @return The value of the {@code commitMetrics} attribute
   */
  @Override
  public CommitMetricsResult commitMetrics() {
    return commitMetrics;
  }

  /**
   * @return The value of the {@code metadata} attribute
   */
  @Override
  public Map<String, String> metadata() {
    return metadata;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitReport#tableName() tableName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for tableName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitReport withTableName(String value) {
    String newValue = Objects.requireNonNull(value, "tableName");
    if (this.tableName.equals(newValue)) return this;
    return new ImmutableCommitReport(
        newValue,
        this.snapshotId,
        this.sequenceNumber,
        this.operation,
        this.commitMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitReport#snapshotId() snapshotId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for snapshotId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitReport withSnapshotId(long value) {
    if (this.snapshotId == value) return this;
    return new ImmutableCommitReport(this.tableName, value, this.sequenceNumber, this.operation, this.commitMetrics, this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitReport#sequenceNumber() sequenceNumber} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sequenceNumber
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitReport withSequenceNumber(long value) {
    if (this.sequenceNumber == value) return this;
    return new ImmutableCommitReport(this.tableName, this.snapshotId, value, this.operation, this.commitMetrics, this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitReport#operation() operation} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for operation
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitReport withOperation(String value) {
    String newValue = Objects.requireNonNull(value, "operation");
    if (this.operation.equals(newValue)) return this;
    return new ImmutableCommitReport(
        this.tableName,
        this.snapshotId,
        this.sequenceNumber,
        newValue,
        this.commitMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitReport#commitMetrics() commitMetrics} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for commitMetrics
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitReport withCommitMetrics(CommitMetricsResult value) {
    if (this.commitMetrics == value) return this;
    CommitMetricsResult newValue = Objects.requireNonNull(value, "commitMetrics");
    return new ImmutableCommitReport(this.tableName, this.snapshotId, this.sequenceNumber, this.operation, newValue, this.metadata);
  }

  /**
   * Copy the current immutable object by replacing the {@link CommitReport#metadata() metadata} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the metadata map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableCommitReport withMetadata(Map<String, ? extends String> entries) {
    if (this.metadata == entries) return this;
    Map<String, String> newValue = createUnmodifiableMap(true, false, entries);
    return new ImmutableCommitReport(
        this.tableName,
        this.snapshotId,
        this.sequenceNumber,
        this.operation,
        this.commitMetrics,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCommitReport} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCommitReport
        && equalTo(0, (ImmutableCommitReport) another);
  }

  private boolean equalTo(int synthetic, ImmutableCommitReport another) {
    return tableName.equals(another.tableName)
        && snapshotId == another.snapshotId
        && sequenceNumber == another.sequenceNumber
        && operation.equals(another.operation)
        && commitMetrics.equals(another.commitMetrics)
        && metadata.equals(another.metadata);
  }

  /**
   * Computes a hash code from attributes: {@code tableName}, {@code snapshotId}, {@code sequenceNumber}, {@code operation}, {@code commitMetrics}, {@code metadata}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + tableName.hashCode();
    h += (h << 5) + Long.hashCode(snapshotId);
    h += (h << 5) + Long.hashCode(sequenceNumber);
    h += (h << 5) + operation.hashCode();
    h += (h << 5) + commitMetrics.hashCode();
    h += (h << 5) + metadata.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code CommitReport} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CommitReport{"
        + "tableName=" + tableName
        + ", snapshotId=" + snapshotId
        + ", sequenceNumber=" + sequenceNumber
        + ", operation=" + operation
        + ", commitMetrics=" + commitMetrics
        + ", metadata=" + metadata
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link CommitReport} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CommitReport instance
   */
  public static ImmutableCommitReport copyOf(CommitReport instance) {
    if (instance instanceof ImmutableCommitReport) {
      return (ImmutableCommitReport) instance;
    }
    return ImmutableCommitReport.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCommitReport ImmutableCommitReport}.
   * <pre>
   * ImmutableCommitReport.builder()
   *    .tableName(String) // required {@link CommitReport#tableName() tableName}
   *    .snapshotId(long) // required {@link CommitReport#snapshotId() snapshotId}
   *    .sequenceNumber(long) // required {@link CommitReport#sequenceNumber() sequenceNumber}
   *    .operation(String) // required {@link CommitReport#operation() operation}
   *    .commitMetrics(org.apache.iceberg.metrics.CommitMetricsResult) // required {@link CommitReport#commitMetrics() commitMetrics}
   *    .putMetadata|putAllMetadata(String =&gt; String) // {@link CommitReport#metadata() metadata} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableCommitReport builder
   */
  public static ImmutableCommitReport.Builder builder() {
    return new ImmutableCommitReport.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCommitReport ImmutableCommitReport}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CommitReport", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TABLE_NAME = 0x1L;
    private static final long INIT_BIT_SNAPSHOT_ID = 0x2L;
    private static final long INIT_BIT_SEQUENCE_NUMBER = 0x4L;
    private static final long INIT_BIT_OPERATION = 0x8L;
    private static final long INIT_BIT_COMMIT_METRICS = 0x10L;
    private long initBits = 0x1fL;

    private @Nullable String tableName;
    private long snapshotId;
    private long sequenceNumber;
    private @Nullable String operation;
    private @Nullable CommitMetricsResult commitMetrics;
    private Map<String, String> metadata = new LinkedHashMap<String, String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CommitReport} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CommitReport instance) {
      Objects.requireNonNull(instance, "instance");
      tableName(instance.tableName());
      snapshotId(instance.snapshotId());
      sequenceNumber(instance.sequenceNumber());
      operation(instance.operation());
      commitMetrics(instance.commitMetrics());
      putAllMetadata(instance.metadata());
      return this;
    }

    /**
     * Initializes the value for the {@link CommitReport#tableName() tableName} attribute.
     * @param tableName The value for tableName 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder tableName(String tableName) {
      this.tableName = Objects.requireNonNull(tableName, "tableName");
      initBits &= ~INIT_BIT_TABLE_NAME;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitReport#snapshotId() snapshotId} attribute.
     * @param snapshotId The value for snapshotId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder snapshotId(long snapshotId) {
      this.snapshotId = snapshotId;
      initBits &= ~INIT_BIT_SNAPSHOT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitReport#sequenceNumber() sequenceNumber} attribute.
     * @param sequenceNumber The value for sequenceNumber 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sequenceNumber(long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
      initBits &= ~INIT_BIT_SEQUENCE_NUMBER;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitReport#operation() operation} attribute.
     * @param operation The value for operation 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder operation(String operation) {
      this.operation = Objects.requireNonNull(operation, "operation");
      initBits &= ~INIT_BIT_OPERATION;
      return this;
    }

    /**
     * Initializes the value for the {@link CommitReport#commitMetrics() commitMetrics} attribute.
     * @param commitMetrics The value for commitMetrics 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder commitMetrics(CommitMetricsResult commitMetrics) {
      this.commitMetrics = Objects.requireNonNull(commitMetrics, "commitMetrics");
      initBits &= ~INIT_BIT_COMMIT_METRICS;
      return this;
    }

    /**
     * Put one entry to the {@link CommitReport#metadata() metadata} map.
     * @param key The key in the metadata map
     * @param value The associated value in the metadata map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putMetadata(String key, String value) {
      this.metadata.put(
          Objects.requireNonNull(key, "metadata key"),
          value == null ? Objects.requireNonNull(value, "metadata value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link CommitReport#metadata() metadata} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putMetadata(Map.Entry<String, ? extends String> entry) {
      String k = entry.getKey();
      String v = entry.getValue();
      this.metadata.put(
          Objects.requireNonNull(k, "metadata key"),
          v == null ? Objects.requireNonNull(v, "metadata value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link CommitReport#metadata() metadata} map. Nulls are not permitted
     * @param entries The entries that will be added to the metadata map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder metadata(Map<String, ? extends String> entries) {
      this.metadata.clear();
      return putAllMetadata(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link CommitReport#metadata() metadata} map. Nulls are not permitted
     * @param entries The entries that will be added to the metadata map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllMetadata(Map<String, ? extends String> entries) {
      for (Map.Entry<String, ? extends String> e : entries.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        this.metadata.put(
            Objects.requireNonNull(k, "metadata key"),
            v == null ? Objects.requireNonNull(v, "metadata value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableCommitReport ImmutableCommitReport}.
     * @return An immutable instance of CommitReport
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCommitReport build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableCommitReport(
          tableName,
          snapshotId,
          sequenceNumber,
          operation,
          commitMetrics,
          createUnmodifiableMap(false, false, metadata));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TABLE_NAME) != 0) attributes.add("tableName");
      if ((initBits & INIT_BIT_SNAPSHOT_ID) != 0) attributes.add("snapshotId");
      if ((initBits & INIT_BIT_SEQUENCE_NUMBER) != 0) attributes.add("sequenceNumber");
      if ((initBits & INIT_BIT_OPERATION) != 0) attributes.add("operation");
      if ((initBits & INIT_BIT_COMMIT_METRICS) != 0) attributes.add("commitMetrics");
      return "Cannot build CommitReport, some of required attributes are not set " + attributes;
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

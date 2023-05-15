package org.apache.iceberg.metrics;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.iceberg.expressions.Expression;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ScanReport}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableScanReport.builder()}.
 */
@Generated(from = "ScanReport", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableScanReport implements ScanReport {
  private final String tableName;
  private final long snapshotId;
  private final Expression filter;
  private final int schemaId;
  private final List<Integer> projectedFieldIds;
  private final List<String> projectedFieldNames;
  private final ScanMetricsResult scanMetrics;
  private final Map<String, String> metadata;

  private ImmutableScanReport(
      String tableName,
      long snapshotId,
      Expression filter,
      int schemaId,
      List<Integer> projectedFieldIds,
      List<String> projectedFieldNames,
      ScanMetricsResult scanMetrics,
      Map<String, String> metadata) {
    this.tableName = tableName;
    this.snapshotId = snapshotId;
    this.filter = filter;
    this.schemaId = schemaId;
    this.projectedFieldIds = projectedFieldIds;
    this.projectedFieldNames = projectedFieldNames;
    this.scanMetrics = scanMetrics;
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
   * @return The value of the {@code filter} attribute
   */
  @Override
  public Expression filter() {
    return filter;
  }

  /**
   * @return The value of the {@code schemaId} attribute
   */
  @Override
  public int schemaId() {
    return schemaId;
  }

  /**
   * @return The value of the {@code projectedFieldIds} attribute
   */
  @Override
  public List<Integer> projectedFieldIds() {
    return projectedFieldIds;
  }

  /**
   * @return The value of the {@code projectedFieldNames} attribute
   */
  @Override
  public List<String> projectedFieldNames() {
    return projectedFieldNames;
  }

  /**
   * @return The value of the {@code scanMetrics} attribute
   */
  @Override
  public ScanMetricsResult scanMetrics() {
    return scanMetrics;
  }

  /**
   * @return The value of the {@code metadata} attribute
   */
  @Override
  public Map<String, String> metadata() {
    return metadata;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanReport#tableName() tableName} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for tableName
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanReport withTableName(String value) {
    String newValue = Objects.requireNonNull(value, "tableName");
    if (this.tableName.equals(newValue)) return this;
    return new ImmutableScanReport(
        newValue,
        this.snapshotId,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanReport#snapshotId() snapshotId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for snapshotId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanReport withSnapshotId(long value) {
    if (this.snapshotId == value) return this;
    return new ImmutableScanReport(
        this.tableName,
        value,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanReport#filter() filter} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for filter
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanReport withFilter(Expression value) {
    if (this.filter == value) return this;
    Expression newValue = Objects.requireNonNull(value, "filter");
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        newValue,
        this.schemaId,
        this.projectedFieldIds,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanReport#schemaId() schemaId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schemaId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanReport withSchemaId(int value) {
    if (this.schemaId == value) return this;
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        value,
        this.projectedFieldIds,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScanReport#projectedFieldIds() projectedFieldIds}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScanReport withProjectedFieldIds(int... elements) {
    ArrayList<Integer> wrappedList = new ArrayList<>(elements.length);
    for (int element : elements) {
      wrappedList.add(element);
    }
    List<Integer> newValue = createUnmodifiableList(false, wrappedList);
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        newValue,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScanReport#projectedFieldIds() projectedFieldIds}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of projectedFieldIds elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScanReport withProjectedFieldIds(Iterable<Integer> elements) {
    if (this.projectedFieldIds == elements) return this;
    List<Integer> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        newValue,
        this.projectedFieldNames,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScanReport#projectedFieldNames() projectedFieldNames}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScanReport withProjectedFieldNames(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        newValue,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ScanReport#projectedFieldNames() projectedFieldNames}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of projectedFieldNames elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScanReport withProjectedFieldNames(Iterable<String> elements) {
    if (this.projectedFieldNames == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        newValue,
        this.scanMetrics,
        this.metadata);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ScanReport#scanMetrics() scanMetrics} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for scanMetrics
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableScanReport withScanMetrics(ScanMetricsResult value) {
    if (this.scanMetrics == value) return this;
    ScanMetricsResult newValue = Objects.requireNonNull(value, "scanMetrics");
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        this.projectedFieldNames,
        newValue,
        this.metadata);
  }

  /**
   * Copy the current immutable object by replacing the {@link ScanReport#metadata() metadata} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the metadata map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableScanReport withMetadata(Map<String, ? extends String> entries) {
    if (this.metadata == entries) return this;
    Map<String, String> newValue = createUnmodifiableMap(true, false, entries);
    return new ImmutableScanReport(
        this.tableName,
        this.snapshotId,
        this.filter,
        this.schemaId,
        this.projectedFieldIds,
        this.projectedFieldNames,
        this.scanMetrics,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableScanReport} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableScanReport
        && equalTo(0, (ImmutableScanReport) another);
  }

  private boolean equalTo(int synthetic, ImmutableScanReport another) {
    return tableName.equals(another.tableName)
        && snapshotId == another.snapshotId
        && filter.equals(another.filter)
        && schemaId == another.schemaId
        && projectedFieldIds.equals(another.projectedFieldIds)
        && projectedFieldNames.equals(another.projectedFieldNames)
        && scanMetrics.equals(another.scanMetrics)
        && metadata.equals(another.metadata);
  }

  /**
   * Computes a hash code from attributes: {@code tableName}, {@code snapshotId}, {@code filter}, {@code schemaId}, {@code projectedFieldIds}, {@code projectedFieldNames}, {@code scanMetrics}, {@code metadata}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + tableName.hashCode();
    h += (h << 5) + Long.hashCode(snapshotId);
    h += (h << 5) + filter.hashCode();
    h += (h << 5) + schemaId;
    h += (h << 5) + projectedFieldIds.hashCode();
    h += (h << 5) + projectedFieldNames.hashCode();
    h += (h << 5) + scanMetrics.hashCode();
    h += (h << 5) + metadata.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code ScanReport} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ScanReport{"
        + "tableName=" + tableName
        + ", snapshotId=" + snapshotId
        + ", filter=" + filter
        + ", schemaId=" + schemaId
        + ", projectedFieldIds=" + projectedFieldIds
        + ", projectedFieldNames=" + projectedFieldNames
        + ", scanMetrics=" + scanMetrics
        + ", metadata=" + metadata
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ScanReport} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ScanReport instance
   */
  public static ImmutableScanReport copyOf(ScanReport instance) {
    if (instance instanceof ImmutableScanReport) {
      return (ImmutableScanReport) instance;
    }
    return ImmutableScanReport.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableScanReport ImmutableScanReport}.
   * <pre>
   * ImmutableScanReport.builder()
   *    .tableName(String) // required {@link ScanReport#tableName() tableName}
   *    .snapshotId(long) // required {@link ScanReport#snapshotId() snapshotId}
   *    .filter(org.apache.iceberg.expressions.Expression) // required {@link ScanReport#filter() filter}
   *    .schemaId(int) // required {@link ScanReport#schemaId() schemaId}
   *    .addProjectedFieldIds|addAllProjectedFieldIds(int) // {@link ScanReport#projectedFieldIds() projectedFieldIds} elements
   *    .addProjectedFieldNames|addAllProjectedFieldNames(String) // {@link ScanReport#projectedFieldNames() projectedFieldNames} elements
   *    .scanMetrics(org.apache.iceberg.metrics.ScanMetricsResult) // required {@link ScanReport#scanMetrics() scanMetrics}
   *    .putMetadata|putAllMetadata(String =&gt; String) // {@link ScanReport#metadata() metadata} mappings
   *    .build();
   * </pre>
   * @return A new ImmutableScanReport builder
   */
  public static ImmutableScanReport.Builder builder() {
    return new ImmutableScanReport.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableScanReport ImmutableScanReport}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ScanReport", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TABLE_NAME = 0x1L;
    private static final long INIT_BIT_SNAPSHOT_ID = 0x2L;
    private static final long INIT_BIT_FILTER = 0x4L;
    private static final long INIT_BIT_SCHEMA_ID = 0x8L;
    private static final long INIT_BIT_SCAN_METRICS = 0x10L;
    private long initBits = 0x1fL;

    private @Nullable String tableName;
    private long snapshotId;
    private @Nullable Expression filter;
    private int schemaId;
    private List<Integer> projectedFieldIds = new ArrayList<Integer>();
    private List<String> projectedFieldNames = new ArrayList<String>();
    private @Nullable ScanMetricsResult scanMetrics;
    private Map<String, String> metadata = new LinkedHashMap<String, String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ScanReport} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ScanReport instance) {
      Objects.requireNonNull(instance, "instance");
      tableName(instance.tableName());
      snapshotId(instance.snapshotId());
      filter(instance.filter());
      schemaId(instance.schemaId());
      addAllProjectedFieldIds(instance.projectedFieldIds());
      addAllProjectedFieldNames(instance.projectedFieldNames());
      scanMetrics(instance.scanMetrics());
      putAllMetadata(instance.metadata());
      return this;
    }

    /**
     * Initializes the value for the {@link ScanReport#tableName() tableName} attribute.
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
     * Initializes the value for the {@link ScanReport#snapshotId() snapshotId} attribute.
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
     * Initializes the value for the {@link ScanReport#filter() filter} attribute.
     * @param filter The value for filter 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder filter(Expression filter) {
      this.filter = Objects.requireNonNull(filter, "filter");
      initBits &= ~INIT_BIT_FILTER;
      return this;
    }

    /**
     * Initializes the value for the {@link ScanReport#schemaId() schemaId} attribute.
     * @param schemaId The value for schemaId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder schemaId(int schemaId) {
      this.schemaId = schemaId;
      initBits &= ~INIT_BIT_SCHEMA_ID;
      return this;
    }

    /**
     * Adds one element to {@link ScanReport#projectedFieldIds() projectedFieldIds} list.
     * @param element A projectedFieldIds element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addProjectedFieldIds(int element) {
      this.projectedFieldIds.add(element);
      return this;
    }

    /**
     * Adds elements to {@link ScanReport#projectedFieldIds() projectedFieldIds} list.
     * @param elements An array of projectedFieldIds elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addProjectedFieldIds(int... elements) {
      for (int element : elements) {
        this.projectedFieldIds.add(element);
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ScanReport#projectedFieldIds() projectedFieldIds} list.
     * @param elements An iterable of projectedFieldIds elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder projectedFieldIds(Iterable<Integer> elements) {
      this.projectedFieldIds.clear();
      return addAllProjectedFieldIds(elements);
    }

    /**
     * Adds elements to {@link ScanReport#projectedFieldIds() projectedFieldIds} list.
     * @param elements An iterable of projectedFieldIds elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllProjectedFieldIds(Iterable<Integer> elements) {
      for (Integer element : elements) {
        this.projectedFieldIds.add(Objects.requireNonNull(element, "projectedFieldIds element"));
      }
      return this;
    }

    /**
     * Adds one element to {@link ScanReport#projectedFieldNames() projectedFieldNames} list.
     * @param element A projectedFieldNames element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addProjectedFieldNames(String element) {
      this.projectedFieldNames.add(Objects.requireNonNull(element, "projectedFieldNames element"));
      return this;
    }

    /**
     * Adds elements to {@link ScanReport#projectedFieldNames() projectedFieldNames} list.
     * @param elements An array of projectedFieldNames elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addProjectedFieldNames(String... elements) {
      for (String element : elements) {
        this.projectedFieldNames.add(Objects.requireNonNull(element, "projectedFieldNames element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ScanReport#projectedFieldNames() projectedFieldNames} list.
     * @param elements An iterable of projectedFieldNames elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder projectedFieldNames(Iterable<String> elements) {
      this.projectedFieldNames.clear();
      return addAllProjectedFieldNames(elements);
    }

    /**
     * Adds elements to {@link ScanReport#projectedFieldNames() projectedFieldNames} list.
     * @param elements An iterable of projectedFieldNames elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllProjectedFieldNames(Iterable<String> elements) {
      for (String element : elements) {
        this.projectedFieldNames.add(Objects.requireNonNull(element, "projectedFieldNames element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ScanReport#scanMetrics() scanMetrics} attribute.
     * @param scanMetrics The value for scanMetrics 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder scanMetrics(ScanMetricsResult scanMetrics) {
      this.scanMetrics = Objects.requireNonNull(scanMetrics, "scanMetrics");
      initBits &= ~INIT_BIT_SCAN_METRICS;
      return this;
    }

    /**
     * Put one entry to the {@link ScanReport#metadata() metadata} map.
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
     * Put one entry to the {@link ScanReport#metadata() metadata} map. Nulls are not permitted
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
     * Sets or replaces all mappings from the specified map as entries for the {@link ScanReport#metadata() metadata} map. Nulls are not permitted
     * @param entries The entries that will be added to the metadata map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder metadata(Map<String, ? extends String> entries) {
      this.metadata.clear();
      return putAllMetadata(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link ScanReport#metadata() metadata} map. Nulls are not permitted
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
     * Builds a new {@link ImmutableScanReport ImmutableScanReport}.
     * @return An immutable instance of ScanReport
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableScanReport build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableScanReport(
          tableName,
          snapshotId,
          filter,
          schemaId,
          createUnmodifiableList(true, projectedFieldIds),
          createUnmodifiableList(true, projectedFieldNames),
          scanMetrics,
          createUnmodifiableMap(false, false, metadata));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TABLE_NAME) != 0) attributes.add("tableName");
      if ((initBits & INIT_BIT_SNAPSHOT_ID) != 0) attributes.add("snapshotId");
      if ((initBits & INIT_BIT_FILTER) != 0) attributes.add("filter");
      if ((initBits & INIT_BIT_SCHEMA_ID) != 0) attributes.add("schemaId");
      if ((initBits & INIT_BIT_SCAN_METRICS) != 0) attributes.add("scanMetrics");
      return "Cannot build ScanReport, some of required attributes are not set " + attributes;
    }
  }

  private static <T> List<T> createSafeList(Iterable<? extends T> iterable, boolean checkNulls, boolean skipNulls) {
    ArrayList<T> list;
    if (iterable instanceof Collection<?>) {
      int size = ((Collection<?>) iterable).size();
      if (size == 0) return Collections.emptyList();
      list = new ArrayList<>();
    } else {
      list = new ArrayList<>();
    }
    for (T element : iterable) {
      if (skipNulls && element == null) continue;
      if (checkNulls) Objects.requireNonNull(element, "element");
      list.add(element);
    }
    return list;
  }

  private static <T> List<T> createUnmodifiableList(boolean clone, List<T> list) {
    switch(list.size()) {
    case 0: return Collections.emptyList();
    case 1: return Collections.singletonList(list.get(0));
    default:
      if (clone) {
        return Collections.unmodifiableList(new ArrayList<>(list));
      } else {
        if (list instanceof ArrayList<?>) {
          ((ArrayList<?>) list).trimToSize();
        }
        return Collections.unmodifiableList(list);
      }
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

package org.apache.iceberg.view;

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
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link ViewVersion}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableViewVersion.builder()}.
 */
@Generated(from = "ViewVersion", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableViewVersion implements ViewVersion {
  private final int versionId;
  private final long timestampMillis;
  private final Map<String, String> summary;
  private final List<ViewRepresentation> representations;
  private final int schemaId;

  private ImmutableViewVersion(
      int versionId,
      long timestampMillis,
      Map<String, String> summary,
      List<ViewRepresentation> representations,
      int schemaId) {
    this.versionId = versionId;
    this.timestampMillis = timestampMillis;
    this.summary = summary;
    this.representations = representations;
    this.schemaId = schemaId;
  }

  /**
   *Return this version's id. Version ids are monotonically increasing 
   */
  @Override
  public int versionId() {
    return versionId;
  }

  /**
   * Return this version's timestamp.
   * <p>This timestamp is the same as those produced by {@link System#currentTimeMillis()}.
   * @return a long timestamp in milliseconds
   */
  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  /**
   * Return the version summary
   * @return a version summary
   */
  @Override
  public Map<String, String> summary() {
    return summary;
  }

  /**
   * Return the list of other view representations.
   * <p>May contain SQL view representations for other dialects.
   * @return the list of view representations
   */
  @Override
  public List<ViewRepresentation> representations() {
    return representations;
  }

  /**
   *The query output schema at version create time, without aliases 
   */
  @Override
  public int schemaId() {
    return schemaId;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ViewVersion#versionId() versionId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for versionId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableViewVersion withVersionId(int value) {
    if (this.versionId == value) return this;
    return validate(new ImmutableViewVersion(value, this.timestampMillis, this.summary, this.representations, this.schemaId));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ViewVersion#timestampMillis() timestampMillis} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timestampMillis
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableViewVersion withTimestampMillis(long value) {
    if (this.timestampMillis == value) return this;
    return validate(new ImmutableViewVersion(this.versionId, value, this.summary, this.representations, this.schemaId));
  }

  /**
   * Copy the current immutable object by replacing the {@link ViewVersion#summary() summary} map with the specified map.
   * Nulls are not permitted as keys or values.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param entries The entries to be added to the summary map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableViewVersion withSummary(Map<String, ? extends String> entries) {
    if (this.summary == entries) return this;
    Map<String, String> newValue = createUnmodifiableMap(true, false, entries);
    return validate(new ImmutableViewVersion(this.versionId, this.timestampMillis, newValue, this.representations, this.schemaId));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ViewVersion#representations() representations}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableViewVersion withRepresentations(ViewRepresentation... elements) {
    List<ViewRepresentation> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return validate(new ImmutableViewVersion(this.versionId, this.timestampMillis, this.summary, newValue, this.schemaId));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link ViewVersion#representations() representations}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of representations elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableViewVersion withRepresentations(Iterable<? extends ViewRepresentation> elements) {
    if (this.representations == elements) return this;
    List<ViewRepresentation> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return validate(new ImmutableViewVersion(this.versionId, this.timestampMillis, this.summary, newValue, this.schemaId));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ViewVersion#schemaId() schemaId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for schemaId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableViewVersion withSchemaId(int value) {
    if (this.schemaId == value) return this;
    return validate(new ImmutableViewVersion(this.versionId, this.timestampMillis, this.summary, this.representations, value));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableViewVersion} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableViewVersion
        && equalTo(0, (ImmutableViewVersion) another);
  }

  private boolean equalTo(int synthetic, ImmutableViewVersion another) {
    return versionId == another.versionId
        && timestampMillis == another.timestampMillis
        && summary.equals(another.summary)
        && representations.equals(another.representations)
        && schemaId == another.schemaId;
  }

  /**
   * Computes a hash code from attributes: {@code versionId}, {@code timestampMillis}, {@code summary}, {@code representations}, {@code schemaId}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + versionId;
    h += (h << 5) + Long.hashCode(timestampMillis);
    h += (h << 5) + summary.hashCode();
    h += (h << 5) + representations.hashCode();
    h += (h << 5) + schemaId;
    return h;
  }

  /**
   * Prints the immutable value {@code ViewVersion} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ViewVersion{"
        + "versionId=" + versionId
        + ", timestampMillis=" + timestampMillis
        + ", summary=" + summary
        + ", representations=" + representations
        + ", schemaId=" + schemaId
        + "}";
  }

  @SuppressWarnings("Immutable")
  private transient volatile long lazyInitBitmap;

  private static final long OPERATION_LAZY_INIT_BIT = 0x1L;

  @SuppressWarnings("Immutable")
  private transient String operation;

  /**
   * {@inheritDoc}
   * <p>
   * Returns a lazily initialized value of the {@link ViewVersion#operation() operation} attribute.
   * Initialized once and only once and stored for subsequent access with proper synchronization.
   * In case of any exception or error thrown by the lazy value initializer,
   * the result will not be memoised (i.e. remembered) and on next call computation
   * will be attempted again.
   * @return A lazily initialized value of the {@code operation} attribute
   */
  @Override
  public String operation() {
    if ((lazyInitBitmap & OPERATION_LAZY_INIT_BIT) == 0) {
      synchronized (this) {
        if ((lazyInitBitmap & OPERATION_LAZY_INIT_BIT) == 0) {
          this.operation = Objects.requireNonNull(ViewVersion.super.operation(), "operation");
          lazyInitBitmap |= OPERATION_LAZY_INIT_BIT;
        }
      }
    }
    return operation;
  }

  private static ImmutableViewVersion validate(ImmutableViewVersion instance) {
    instance.check();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link ViewVersion} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ViewVersion instance
   */
  public static ImmutableViewVersion copyOf(ViewVersion instance) {
    if (instance instanceof ImmutableViewVersion) {
      return (ImmutableViewVersion) instance;
    }
    return ImmutableViewVersion.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableViewVersion ImmutableViewVersion}.
   * <pre>
   * ImmutableViewVersion.builder()
   *    .versionId(int) // required {@link ViewVersion#versionId() versionId}
   *    .timestampMillis(long) // required {@link ViewVersion#timestampMillis() timestampMillis}
   *    .putSummary|putAllSummary(String =&gt; String) // {@link ViewVersion#summary() summary} mappings
   *    .addRepresentations|addAllRepresentations(org.apache.iceberg.view.ViewRepresentation) // {@link ViewVersion#representations() representations} elements
   *    .schemaId(int) // required {@link ViewVersion#schemaId() schemaId}
   *    .build();
   * </pre>
   * @return A new ImmutableViewVersion builder
   */
  public static ImmutableViewVersion.Builder builder() {
    return new ImmutableViewVersion.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableViewVersion ImmutableViewVersion}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ViewVersion", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VERSION_ID = 0x1L;
    private static final long INIT_BIT_TIMESTAMP_MILLIS = 0x2L;
    private static final long INIT_BIT_SCHEMA_ID = 0x4L;
    private long initBits = 0x7L;

    private int versionId;
    private long timestampMillis;
    private Map<String, String> summary = new LinkedHashMap<String, String>();
    private List<ViewRepresentation> representations = new ArrayList<ViewRepresentation>();
    private int schemaId;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ViewVersion} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ViewVersion instance) {
      Objects.requireNonNull(instance, "instance");
      versionId(instance.versionId());
      timestampMillis(instance.timestampMillis());
      putAllSummary(instance.summary());
      addAllRepresentations(instance.representations());
      schemaId(instance.schemaId());
      return this;
    }

    /**
     * Initializes the value for the {@link ViewVersion#versionId() versionId} attribute.
     * @param versionId The value for versionId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder versionId(int versionId) {
      this.versionId = versionId;
      initBits &= ~INIT_BIT_VERSION_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link ViewVersion#timestampMillis() timestampMillis} attribute.
     * @param timestampMillis The value for timestampMillis 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder timestampMillis(long timestampMillis) {
      this.timestampMillis = timestampMillis;
      initBits &= ~INIT_BIT_TIMESTAMP_MILLIS;
      return this;
    }

    /**
     * Put one entry to the {@link ViewVersion#summary() summary} map.
     * @param key The key in the summary map
     * @param value The associated value in the summary map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putSummary(String key, String value) {
      this.summary.put(
          Objects.requireNonNull(key, "summary key"),
          value == null ? Objects.requireNonNull(value, "summary value for key: " + key) : value);
      return this;
    }

    /**
     * Put one entry to the {@link ViewVersion#summary() summary} map. Nulls are not permitted
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putSummary(Map.Entry<String, ? extends String> entry) {
      String k = entry.getKey();
      String v = entry.getValue();
      this.summary.put(
          Objects.requireNonNull(k, "summary key"),
          v == null ? Objects.requireNonNull(v, "summary value for key: " + k) : v);
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link ViewVersion#summary() summary} map. Nulls are not permitted
     * @param entries The entries that will be added to the summary map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder summary(Map<String, ? extends String> entries) {
      this.summary.clear();
      return putAllSummary(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link ViewVersion#summary() summary} map. Nulls are not permitted
     * @param entries The entries that will be added to the summary map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder putAllSummary(Map<String, ? extends String> entries) {
      for (Map.Entry<String, ? extends String> e : entries.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        this.summary.put(
            Objects.requireNonNull(k, "summary key"),
            v == null ? Objects.requireNonNull(v, "summary value for key: " + k) : v);
      }
      return this;
    }

    /**
     * Adds one element to {@link ViewVersion#representations() representations} list.
     * @param element A representations element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addRepresentations(ViewRepresentation element) {
      this.representations.add(Objects.requireNonNull(element, "representations element"));
      return this;
    }

    /**
     * Adds elements to {@link ViewVersion#representations() representations} list.
     * @param elements An array of representations elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addRepresentations(ViewRepresentation... elements) {
      for (ViewRepresentation element : elements) {
        this.representations.add(Objects.requireNonNull(element, "representations element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link ViewVersion#representations() representations} list.
     * @param elements An iterable of representations elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder representations(Iterable<? extends ViewRepresentation> elements) {
      this.representations.clear();
      return addAllRepresentations(elements);
    }

    /**
     * Adds elements to {@link ViewVersion#representations() representations} list.
     * @param elements An iterable of representations elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllRepresentations(Iterable<? extends ViewRepresentation> elements) {
      for (ViewRepresentation element : elements) {
        this.representations.add(Objects.requireNonNull(element, "representations element"));
      }
      return this;
    }

    /**
     * Initializes the value for the {@link ViewVersion#schemaId() schemaId} attribute.
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
     * Builds a new {@link ImmutableViewVersion ImmutableViewVersion}.
     * @return An immutable instance of ViewVersion
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableViewVersion build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return ImmutableViewVersion.validate(new ImmutableViewVersion(
          versionId,
          timestampMillis,
          createUnmodifiableMap(false, false, summary),
          createUnmodifiableList(true, representations),
          schemaId));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VERSION_ID) != 0) attributes.add("versionId");
      if ((initBits & INIT_BIT_TIMESTAMP_MILLIS) != 0) attributes.add("timestampMillis");
      if ((initBits & INIT_BIT_SCHEMA_ID) != 0) attributes.add("schemaId");
      return "Cannot build ViewVersion, some of required attributes are not set " + attributes;
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

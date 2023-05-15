package org.apache.iceberg.view;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.iceberg.catalog.Namespace;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link SQLViewRepresentation}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableSQLViewRepresentation.builder()}.
 */
@Generated(from = "SQLViewRepresentation", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableSQLViewRepresentation implements SQLViewRepresentation {
  private final String sql;
  private final String dialect;
  private final @Nullable String defaultCatalog;
  private final @Nullable Namespace defaultNamespace;
  private final List<String> fieldComments;
  private final List<String> fieldAliases;

  private ImmutableSQLViewRepresentation(
      String sql,
      String dialect,
      @Nullable String defaultCatalog,
      @Nullable Namespace defaultNamespace,
      List<String> fieldComments,
      List<String> fieldAliases) {
    this.sql = sql;
    this.dialect = dialect;
    this.defaultCatalog = defaultCatalog;
    this.defaultNamespace = defaultNamespace;
    this.fieldComments = fieldComments;
    this.fieldAliases = fieldAliases;
  }

  /**
   *The view query SQL text. 
   */
  @Override
  public String sql() {
    return sql;
  }

  /**
   *The view query SQL dialect. 
   */
  @Override
  public String dialect() {
    return dialect;
  }

  /**
   *The default catalog when the view is created. 
   */
  @Override
  public @Nullable String defaultCatalog() {
    return defaultCatalog;
  }

  /**
   *The default namespace when the view is created. 
   */
  @Override
  public @Nullable Namespace defaultNamespace() {
    return defaultNamespace;
  }

  /**
   *The view field comments. 
   */
  @Override
  public List<String> fieldComments() {
    return fieldComments;
  }

  /**
   *The view field aliases. 
   */
  @Override
  public List<String> fieldAliases() {
    return fieldAliases;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SQLViewRepresentation#sql() sql} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for sql
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSQLViewRepresentation withSql(String value) {
    String newValue = Objects.requireNonNull(value, "sql");
    if (this.sql.equals(newValue)) return this;
    return new ImmutableSQLViewRepresentation(
        newValue,
        this.dialect,
        this.defaultCatalog,
        this.defaultNamespace,
        this.fieldComments,
        this.fieldAliases);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SQLViewRepresentation#dialect() dialect} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for dialect
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSQLViewRepresentation withDialect(String value) {
    String newValue = Objects.requireNonNull(value, "dialect");
    if (this.dialect.equals(newValue)) return this;
    return new ImmutableSQLViewRepresentation(
        this.sql,
        newValue,
        this.defaultCatalog,
        this.defaultNamespace,
        this.fieldComments,
        this.fieldAliases);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SQLViewRepresentation#defaultCatalog() defaultCatalog} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for defaultCatalog (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSQLViewRepresentation withDefaultCatalog(@Nullable String value) {
    if (Objects.equals(this.defaultCatalog, value)) return this;
    return new ImmutableSQLViewRepresentation(this.sql, this.dialect, value, this.defaultNamespace, this.fieldComments, this.fieldAliases);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SQLViewRepresentation#defaultNamespace() defaultNamespace} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for defaultNamespace (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableSQLViewRepresentation withDefaultNamespace(@Nullable Namespace value) {
    if (this.defaultNamespace == value) return this;
    return new ImmutableSQLViewRepresentation(this.sql, this.dialect, this.defaultCatalog, value, this.fieldComments, this.fieldAliases);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SQLViewRepresentation#fieldComments() fieldComments}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSQLViewRepresentation withFieldComments(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableSQLViewRepresentation(this.sql, this.dialect, this.defaultCatalog, this.defaultNamespace, newValue, this.fieldAliases);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SQLViewRepresentation#fieldComments() fieldComments}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of fieldComments elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSQLViewRepresentation withFieldComments(Iterable<String> elements) {
    if (this.fieldComments == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableSQLViewRepresentation(this.sql, this.dialect, this.defaultCatalog, this.defaultNamespace, newValue, this.fieldAliases);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SQLViewRepresentation#fieldAliases() fieldAliases}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSQLViewRepresentation withFieldAliases(String... elements) {
    List<String> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
    return new ImmutableSQLViewRepresentation(
        this.sql,
        this.dialect,
        this.defaultCatalog,
        this.defaultNamespace,
        this.fieldComments,
        newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link SQLViewRepresentation#fieldAliases() fieldAliases}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of fieldAliases elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableSQLViewRepresentation withFieldAliases(Iterable<String> elements) {
    if (this.fieldAliases == elements) return this;
    List<String> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
    return new ImmutableSQLViewRepresentation(
        this.sql,
        this.dialect,
        this.defaultCatalog,
        this.defaultNamespace,
        this.fieldComments,
        newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSQLViewRepresentation} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableSQLViewRepresentation
        && equalTo(0, (ImmutableSQLViewRepresentation) another);
  }

  private boolean equalTo(int synthetic, ImmutableSQLViewRepresentation another) {
    return sql.equals(another.sql)
        && dialect.equals(another.dialect)
        && Objects.equals(defaultCatalog, another.defaultCatalog)
        && Objects.equals(defaultNamespace, another.defaultNamespace)
        && fieldComments.equals(another.fieldComments)
        && fieldAliases.equals(another.fieldAliases);
  }

  /**
   * Computes a hash code from attributes: {@code sql}, {@code dialect}, {@code defaultCatalog}, {@code defaultNamespace}, {@code fieldComments}, {@code fieldAliases}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + sql.hashCode();
    h += (h << 5) + dialect.hashCode();
    h += (h << 5) + Objects.hashCode(defaultCatalog);
    h += (h << 5) + Objects.hashCode(defaultNamespace);
    h += (h << 5) + fieldComments.hashCode();
    h += (h << 5) + fieldAliases.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SQLViewRepresentation} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SQLViewRepresentation{"
        + "sql=" + sql
        + ", dialect=" + dialect
        + ", defaultCatalog=" + defaultCatalog
        + ", defaultNamespace=" + defaultNamespace
        + ", fieldComments=" + fieldComments
        + ", fieldAliases=" + fieldAliases
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link SQLViewRepresentation} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SQLViewRepresentation instance
   */
  public static ImmutableSQLViewRepresentation copyOf(SQLViewRepresentation instance) {
    if (instance instanceof ImmutableSQLViewRepresentation) {
      return (ImmutableSQLViewRepresentation) instance;
    }
    return ImmutableSQLViewRepresentation.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
   * <pre>
   * ImmutableSQLViewRepresentation.builder()
   *    .sql(String) // required {@link SQLViewRepresentation#sql() sql}
   *    .dialect(String) // required {@link SQLViewRepresentation#dialect() dialect}
   *    .defaultCatalog(String | null) // nullable {@link SQLViewRepresentation#defaultCatalog() defaultCatalog}
   *    .defaultNamespace(org.apache.iceberg.catalog.Namespace | null) // nullable {@link SQLViewRepresentation#defaultNamespace() defaultNamespace}
   *    .addFieldComments|addAllFieldComments(String) // {@link SQLViewRepresentation#fieldComments() fieldComments} elements
   *    .addFieldAliases|addAllFieldAliases(String) // {@link SQLViewRepresentation#fieldAliases() fieldAliases} elements
   *    .build();
   * </pre>
   * @return A new ImmutableSQLViewRepresentation builder
   */
  public static ImmutableSQLViewRepresentation.Builder builder() {
    return new ImmutableSQLViewRepresentation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "SQLViewRepresentation", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_SQL = 0x1L;
    private static final long INIT_BIT_DIALECT = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String sql;
    private @Nullable String dialect;
    private @Nullable String defaultCatalog;
    private @Nullable Namespace defaultNamespace;
    private List<String> fieldComments = new ArrayList<String>();
    private List<String> fieldAliases = new ArrayList<String>();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code SQLViewRepresentation} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(SQLViewRepresentation instance) {
      Objects.requireNonNull(instance, "instance");
      sql(instance.sql());
      dialect(instance.dialect());
      @Nullable String defaultCatalogValue = instance.defaultCatalog();
      if (defaultCatalogValue != null) {
        defaultCatalog(defaultCatalogValue);
      }
      @Nullable Namespace defaultNamespaceValue = instance.defaultNamespace();
      if (defaultNamespaceValue != null) {
        defaultNamespace(defaultNamespaceValue);
      }
      addAllFieldComments(instance.fieldComments());
      addAllFieldAliases(instance.fieldAliases());
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#sql() sql} attribute.
     * @param sql The value for sql 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder sql(String sql) {
      this.sql = Objects.requireNonNull(sql, "sql");
      initBits &= ~INIT_BIT_SQL;
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#dialect() dialect} attribute.
     * @param dialect The value for dialect 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder dialect(String dialect) {
      this.dialect = Objects.requireNonNull(dialect, "dialect");
      initBits &= ~INIT_BIT_DIALECT;
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#defaultCatalog() defaultCatalog} attribute.
     * @param defaultCatalog The value for defaultCatalog (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder defaultCatalog(@Nullable String defaultCatalog) {
      this.defaultCatalog = defaultCatalog;
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#defaultNamespace() defaultNamespace} attribute.
     * @param defaultNamespace The value for defaultNamespace (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder defaultNamespace(@Nullable Namespace defaultNamespace) {
      this.defaultNamespace = defaultNamespace;
      return this;
    }

    /**
     * Adds one element to {@link SQLViewRepresentation#fieldComments() fieldComments} list.
     * @param element A fieldComments element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFieldComments(String element) {
      this.fieldComments.add(Objects.requireNonNull(element, "fieldComments element"));
      return this;
    }

    /**
     * Adds elements to {@link SQLViewRepresentation#fieldComments() fieldComments} list.
     * @param elements An array of fieldComments elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFieldComments(String... elements) {
      for (String element : elements) {
        this.fieldComments.add(Objects.requireNonNull(element, "fieldComments element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link SQLViewRepresentation#fieldComments() fieldComments} list.
     * @param elements An iterable of fieldComments elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fieldComments(Iterable<String> elements) {
      this.fieldComments.clear();
      return addAllFieldComments(elements);
    }

    /**
     * Adds elements to {@link SQLViewRepresentation#fieldComments() fieldComments} list.
     * @param elements An iterable of fieldComments elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllFieldComments(Iterable<String> elements) {
      for (String element : elements) {
        this.fieldComments.add(Objects.requireNonNull(element, "fieldComments element"));
      }
      return this;
    }

    /**
     * Adds one element to {@link SQLViewRepresentation#fieldAliases() fieldAliases} list.
     * @param element A fieldAliases element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFieldAliases(String element) {
      this.fieldAliases.add(Objects.requireNonNull(element, "fieldAliases element"));
      return this;
    }

    /**
     * Adds elements to {@link SQLViewRepresentation#fieldAliases() fieldAliases} list.
     * @param elements An array of fieldAliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addFieldAliases(String... elements) {
      for (String element : elements) {
        this.fieldAliases.add(Objects.requireNonNull(element, "fieldAliases element"));
      }
      return this;
    }


    /**
     * Sets or replaces all elements for {@link SQLViewRepresentation#fieldAliases() fieldAliases} list.
     * @param elements An iterable of fieldAliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder fieldAliases(Iterable<String> elements) {
      this.fieldAliases.clear();
      return addAllFieldAliases(elements);
    }

    /**
     * Adds elements to {@link SQLViewRepresentation#fieldAliases() fieldAliases} list.
     * @param elements An iterable of fieldAliases elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllFieldAliases(Iterable<String> elements) {
      for (String element : elements) {
        this.fieldAliases.add(Objects.requireNonNull(element, "fieldAliases element"));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
     * @return An immutable instance of SQLViewRepresentation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSQLViewRepresentation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableSQLViewRepresentation(
          sql,
          dialect,
          defaultCatalog,
          defaultNamespace,
          createUnmodifiableList(true, fieldComments),
          createUnmodifiableList(true, fieldAliases));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_SQL) != 0) attributes.add("sql");
      if ((initBits & INIT_BIT_DIALECT) != 0) attributes.add("dialect");
      return "Cannot build SQLViewRepresentation, some of required attributes are not set " + attributes;
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
}

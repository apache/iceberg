package org.apache.iceberg.actions;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * {@code ImmutableMigrateTable} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link MigrateTable}.
 * @see ImmutableMigrateTable.Result
 */
@Generated(from = "MigrateTable", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableMigrateTable {
  private ImmutableMigrateTable() {}

  /**
   * Immutable implementation of {@link MigrateTable.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableMigrateTable.Result.builder()}.
   */
  @Generated(from = "MigrateTable.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements MigrateTable.Result {
    private final long migratedDataFilesCount;

    private Result(long migratedDataFilesCount) {
      this.migratedDataFilesCount = migratedDataFilesCount;
    }

    /**
     *Returns the number of migrated data files. 
     */
    @Override
    public long migratedDataFilesCount() {
      return migratedDataFilesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link MigrateTable.Result#migratedDataFilesCount() migratedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for migratedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableMigrateTable.Result withMigratedDataFilesCount(long value) {
      if (this.migratedDataFilesCount == value) return this;
      return new ImmutableMigrateTable.Result(value);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableMigrateTable.Result
          && equalTo(0, (ImmutableMigrateTable.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableMigrateTable.Result another) {
      return migratedDataFilesCount == another.migratedDataFilesCount;
    }

    /**
     * Computes a hash code from attributes: {@code migratedDataFilesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + Long.hashCode(migratedDataFilesCount);
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "migratedDataFilesCount=" + migratedDataFilesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link MigrateTable.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableMigrateTable.Result copyOf(MigrateTable.Result instance) {
      if (instance instanceof ImmutableMigrateTable.Result) {
        return (ImmutableMigrateTable.Result) instance;
      }
      return ImmutableMigrateTable.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableMigrateTable.Result Result}.
     * <pre>
     * ImmutableMigrateTable.Result.builder()
     *    .migratedDataFilesCount(long) // required {@link MigrateTable.Result#migratedDataFilesCount() migratedDataFilesCount}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableMigrateTable.Result.Builder builder() {
      return new ImmutableMigrateTable.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableMigrateTable.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "MigrateTable.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_MIGRATED_DATA_FILES_COUNT = 0x1L;
      private long initBits = 0x1L;

      private long migratedDataFilesCount;

      private Builder() {
      }

      /**
       * Fill a builder with attribute values from the provided {@code Result} instance.
       * Regular attribute values will be replaced with those from the given instance.
       * Absent optional values will not replace present values.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(MigrateTable.Result instance) {
        Objects.requireNonNull(instance, "instance");
        migratedDataFilesCount(instance.migratedDataFilesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link MigrateTable.Result#migratedDataFilesCount() migratedDataFilesCount} attribute.
       * @param migratedDataFilesCount The value for migratedDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder migratedDataFilesCount(long migratedDataFilesCount) {
        this.migratedDataFilesCount = migratedDataFilesCount;
        initBits &= ~INIT_BIT_MIGRATED_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableMigrateTable.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableMigrateTable.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableMigrateTable.Result(migratedDataFilesCount);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_MIGRATED_DATA_FILES_COUNT) != 0) attributes.add("migratedDataFilesCount");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

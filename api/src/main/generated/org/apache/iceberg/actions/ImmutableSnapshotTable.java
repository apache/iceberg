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
 * {@code ImmutableSnapshotTable} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link SnapshotTable}.
 * @see ImmutableSnapshotTable.Result
 */
@Generated(from = "SnapshotTable", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableSnapshotTable {
  private ImmutableSnapshotTable() {}

  /**
   * Immutable implementation of {@link SnapshotTable.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableSnapshotTable.Result.builder()}.
   */
  @Generated(from = "SnapshotTable.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements SnapshotTable.Result {
    private final long importedDataFilesCount;

    private Result(long importedDataFilesCount) {
      this.importedDataFilesCount = importedDataFilesCount;
    }

    /**
     *Returns the number of imported data files. 
     */
    @Override
    public long importedDataFilesCount() {
      return importedDataFilesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link SnapshotTable.Result#importedDataFilesCount() importedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for importedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableSnapshotTable.Result withImportedDataFilesCount(long value) {
      if (this.importedDataFilesCount == value) return this;
      return new ImmutableSnapshotTable.Result(value);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableSnapshotTable.Result
          && equalTo(0, (ImmutableSnapshotTable.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableSnapshotTable.Result another) {
      return importedDataFilesCount == another.importedDataFilesCount;
    }

    /**
     * Computes a hash code from attributes: {@code importedDataFilesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + Long.hashCode(importedDataFilesCount);
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "importedDataFilesCount=" + importedDataFilesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link SnapshotTable.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableSnapshotTable.Result copyOf(SnapshotTable.Result instance) {
      if (instance instanceof ImmutableSnapshotTable.Result) {
        return (ImmutableSnapshotTable.Result) instance;
      }
      return ImmutableSnapshotTable.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableSnapshotTable.Result Result}.
     * <pre>
     * ImmutableSnapshotTable.Result.builder()
     *    .importedDataFilesCount(long) // required {@link SnapshotTable.Result#importedDataFilesCount() importedDataFilesCount}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableSnapshotTable.Result.Builder builder() {
      return new ImmutableSnapshotTable.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableSnapshotTable.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "SnapshotTable.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_IMPORTED_DATA_FILES_COUNT = 0x1L;
      private long initBits = 0x1L;

      private long importedDataFilesCount;

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
      public final Builder from(SnapshotTable.Result instance) {
        Objects.requireNonNull(instance, "instance");
        importedDataFilesCount(instance.importedDataFilesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link SnapshotTable.Result#importedDataFilesCount() importedDataFilesCount} attribute.
       * @param importedDataFilesCount The value for importedDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder importedDataFilesCount(long importedDataFilesCount) {
        this.importedDataFilesCount = importedDataFilesCount;
        initBits &= ~INIT_BIT_IMPORTED_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableSnapshotTable.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableSnapshotTable.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableSnapshotTable.Result(importedDataFilesCount);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_IMPORTED_DATA_FILES_COUNT) != 0) attributes.add("importedDataFilesCount");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

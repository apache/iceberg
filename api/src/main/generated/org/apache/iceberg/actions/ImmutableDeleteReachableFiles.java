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
 * {@code ImmutableDeleteReachableFiles} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link DeleteReachableFiles}.
 * @see ImmutableDeleteReachableFiles.Result
 */
@Generated(from = "DeleteReachableFiles", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableDeleteReachableFiles {
  private ImmutableDeleteReachableFiles() {}

  /**
   * Immutable implementation of {@link DeleteReachableFiles.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableDeleteReachableFiles.Result.builder()}.
   */
  @Generated(from = "DeleteReachableFiles.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements DeleteReachableFiles.Result {
    private final long deletedDataFilesCount;
    private final long deletedEqualityDeleteFilesCount;
    private final long deletedPositionDeleteFilesCount;
    private final long deletedManifestsCount;
    private final long deletedManifestListsCount;
    private final long deletedOtherFilesCount;

    private Result(
        long deletedDataFilesCount,
        long deletedEqualityDeleteFilesCount,
        long deletedPositionDeleteFilesCount,
        long deletedManifestsCount,
        long deletedManifestListsCount,
        long deletedOtherFilesCount) {
      this.deletedDataFilesCount = deletedDataFilesCount;
      this.deletedEqualityDeleteFilesCount = deletedEqualityDeleteFilesCount;
      this.deletedPositionDeleteFilesCount = deletedPositionDeleteFilesCount;
      this.deletedManifestsCount = deletedManifestsCount;
      this.deletedManifestListsCount = deletedManifestListsCount;
      this.deletedOtherFilesCount = deletedOtherFilesCount;
    }

    /**
     *Returns the number of deleted data files. 
     */
    @Override
    public long deletedDataFilesCount() {
      return deletedDataFilesCount;
    }

    /**
     *Returns the number of deleted equality delete files. 
     */
    @Override
    public long deletedEqualityDeleteFilesCount() {
      return deletedEqualityDeleteFilesCount;
    }

    /**
     *Returns the number of deleted position delete files. 
     */
    @Override
    public long deletedPositionDeleteFilesCount() {
      return deletedPositionDeleteFilesCount;
    }

    /**
     *Returns the number of deleted manifests. 
     */
    @Override
    public long deletedManifestsCount() {
      return deletedManifestsCount;
    }

    /**
     *Returns the number of deleted manifest lists. 
     */
    @Override
    public long deletedManifestListsCount() {
      return deletedManifestListsCount;
    }

    /**
     *Returns the number of deleted metadata json, version hint files. 
     */
    @Override
    public long deletedOtherFilesCount() {
      return deletedOtherFilesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedDataFilesCount() deletedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedDataFilesCount(long value) {
      if (this.deletedDataFilesCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          value,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedOtherFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedEqualityDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedEqualityDeleteFilesCount(long value) {
      if (this.deletedEqualityDeleteFilesCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          this.deletedDataFilesCount,
          value,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedOtherFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedPositionDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedPositionDeleteFilesCount(long value) {
      if (this.deletedPositionDeleteFilesCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          value,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedOtherFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedManifestsCount() deletedManifestsCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedManifestsCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedManifestsCount(long value) {
      if (this.deletedManifestsCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          value,
          this.deletedManifestListsCount,
          this.deletedOtherFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedManifestListsCount() deletedManifestListsCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedManifestListsCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedManifestListsCount(long value) {
      if (this.deletedManifestListsCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          value,
          this.deletedOtherFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteReachableFiles.Result#deletedOtherFilesCount() deletedOtherFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedOtherFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteReachableFiles.Result withDeletedOtherFilesCount(long value) {
      if (this.deletedOtherFilesCount == value) return this;
      return new ImmutableDeleteReachableFiles.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          value);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableDeleteReachableFiles.Result
          && equalTo(0, (ImmutableDeleteReachableFiles.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableDeleteReachableFiles.Result another) {
      return deletedDataFilesCount == another.deletedDataFilesCount
          && deletedEqualityDeleteFilesCount == another.deletedEqualityDeleteFilesCount
          && deletedPositionDeleteFilesCount == another.deletedPositionDeleteFilesCount
          && deletedManifestsCount == another.deletedManifestsCount
          && deletedManifestListsCount == another.deletedManifestListsCount
          && deletedOtherFilesCount == another.deletedOtherFilesCount;
    }

    /**
     * Computes a hash code from attributes: {@code deletedDataFilesCount}, {@code deletedEqualityDeleteFilesCount}, {@code deletedPositionDeleteFilesCount}, {@code deletedManifestsCount}, {@code deletedManifestListsCount}, {@code deletedOtherFilesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + Long.hashCode(deletedDataFilesCount);
      h += (h << 5) + Long.hashCode(deletedEqualityDeleteFilesCount);
      h += (h << 5) + Long.hashCode(deletedPositionDeleteFilesCount);
      h += (h << 5) + Long.hashCode(deletedManifestsCount);
      h += (h << 5) + Long.hashCode(deletedManifestListsCount);
      h += (h << 5) + Long.hashCode(deletedOtherFilesCount);
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "deletedDataFilesCount=" + deletedDataFilesCount
          + ", deletedEqualityDeleteFilesCount=" + deletedEqualityDeleteFilesCount
          + ", deletedPositionDeleteFilesCount=" + deletedPositionDeleteFilesCount
          + ", deletedManifestsCount=" + deletedManifestsCount
          + ", deletedManifestListsCount=" + deletedManifestListsCount
          + ", deletedOtherFilesCount=" + deletedOtherFilesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link DeleteReachableFiles.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableDeleteReachableFiles.Result copyOf(DeleteReachableFiles.Result instance) {
      if (instance instanceof ImmutableDeleteReachableFiles.Result) {
        return (ImmutableDeleteReachableFiles.Result) instance;
      }
      return ImmutableDeleteReachableFiles.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableDeleteReachableFiles.Result Result}.
     * <pre>
     * ImmutableDeleteReachableFiles.Result.builder()
     *    .deletedDataFilesCount(long) // required {@link DeleteReachableFiles.Result#deletedDataFilesCount() deletedDataFilesCount}
     *    .deletedEqualityDeleteFilesCount(long) // required {@link DeleteReachableFiles.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount}
     *    .deletedPositionDeleteFilesCount(long) // required {@link DeleteReachableFiles.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount}
     *    .deletedManifestsCount(long) // required {@link DeleteReachableFiles.Result#deletedManifestsCount() deletedManifestsCount}
     *    .deletedManifestListsCount(long) // required {@link DeleteReachableFiles.Result#deletedManifestListsCount() deletedManifestListsCount}
     *    .deletedOtherFilesCount(long) // required {@link DeleteReachableFiles.Result#deletedOtherFilesCount() deletedOtherFilesCount}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableDeleteReachableFiles.Result.Builder builder() {
      return new ImmutableDeleteReachableFiles.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableDeleteReachableFiles.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "DeleteReachableFiles.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_DELETED_DATA_FILES_COUNT = 0x1L;
      private static final long INIT_BIT_DELETED_EQUALITY_DELETE_FILES_COUNT = 0x2L;
      private static final long INIT_BIT_DELETED_POSITION_DELETE_FILES_COUNT = 0x4L;
      private static final long INIT_BIT_DELETED_MANIFESTS_COUNT = 0x8L;
      private static final long INIT_BIT_DELETED_MANIFEST_LISTS_COUNT = 0x10L;
      private static final long INIT_BIT_DELETED_OTHER_FILES_COUNT = 0x20L;
      private long initBits = 0x3fL;

      private long deletedDataFilesCount;
      private long deletedEqualityDeleteFilesCount;
      private long deletedPositionDeleteFilesCount;
      private long deletedManifestsCount;
      private long deletedManifestListsCount;
      private long deletedOtherFilesCount;

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
      public final Builder from(DeleteReachableFiles.Result instance) {
        Objects.requireNonNull(instance, "instance");
        deletedDataFilesCount(instance.deletedDataFilesCount());
        deletedEqualityDeleteFilesCount(instance.deletedEqualityDeleteFilesCount());
        deletedPositionDeleteFilesCount(instance.deletedPositionDeleteFilesCount());
        deletedManifestsCount(instance.deletedManifestsCount());
        deletedManifestListsCount(instance.deletedManifestListsCount());
        deletedOtherFilesCount(instance.deletedOtherFilesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedDataFilesCount() deletedDataFilesCount} attribute.
       * @param deletedDataFilesCount The value for deletedDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedDataFilesCount(long deletedDataFilesCount) {
        this.deletedDataFilesCount = deletedDataFilesCount;
        initBits &= ~INIT_BIT_DELETED_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount} attribute.
       * @param deletedEqualityDeleteFilesCount The value for deletedEqualityDeleteFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedEqualityDeleteFilesCount(long deletedEqualityDeleteFilesCount) {
        this.deletedEqualityDeleteFilesCount = deletedEqualityDeleteFilesCount;
        initBits &= ~INIT_BIT_DELETED_EQUALITY_DELETE_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount} attribute.
       * @param deletedPositionDeleteFilesCount The value for deletedPositionDeleteFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedPositionDeleteFilesCount(long deletedPositionDeleteFilesCount) {
        this.deletedPositionDeleteFilesCount = deletedPositionDeleteFilesCount;
        initBits &= ~INIT_BIT_DELETED_POSITION_DELETE_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedManifestsCount() deletedManifestsCount} attribute.
       * @param deletedManifestsCount The value for deletedManifestsCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedManifestsCount(long deletedManifestsCount) {
        this.deletedManifestsCount = deletedManifestsCount;
        initBits &= ~INIT_BIT_DELETED_MANIFESTS_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedManifestListsCount() deletedManifestListsCount} attribute.
       * @param deletedManifestListsCount The value for deletedManifestListsCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedManifestListsCount(long deletedManifestListsCount) {
        this.deletedManifestListsCount = deletedManifestListsCount;
        initBits &= ~INIT_BIT_DELETED_MANIFEST_LISTS_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteReachableFiles.Result#deletedOtherFilesCount() deletedOtherFilesCount} attribute.
       * @param deletedOtherFilesCount The value for deletedOtherFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedOtherFilesCount(long deletedOtherFilesCount) {
        this.deletedOtherFilesCount = deletedOtherFilesCount;
        initBits &= ~INIT_BIT_DELETED_OTHER_FILES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableDeleteReachableFiles.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableDeleteReachableFiles.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableDeleteReachableFiles.Result(
            deletedDataFilesCount,
            deletedEqualityDeleteFilesCount,
            deletedPositionDeleteFilesCount,
            deletedManifestsCount,
            deletedManifestListsCount,
            deletedOtherFilesCount);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_DELETED_DATA_FILES_COUNT) != 0) attributes.add("deletedDataFilesCount");
        if ((initBits & INIT_BIT_DELETED_EQUALITY_DELETE_FILES_COUNT) != 0) attributes.add("deletedEqualityDeleteFilesCount");
        if ((initBits & INIT_BIT_DELETED_POSITION_DELETE_FILES_COUNT) != 0) attributes.add("deletedPositionDeleteFilesCount");
        if ((initBits & INIT_BIT_DELETED_MANIFESTS_COUNT) != 0) attributes.add("deletedManifestsCount");
        if ((initBits & INIT_BIT_DELETED_MANIFEST_LISTS_COUNT) != 0) attributes.add("deletedManifestListsCount");
        if ((initBits & INIT_BIT_DELETED_OTHER_FILES_COUNT) != 0) attributes.add("deletedOtherFilesCount");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

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
 * {@code ImmutableExpireSnapshots} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link ExpireSnapshots}.
 * @see ImmutableExpireSnapshots.Result
 */
@Generated(from = "ExpireSnapshots", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableExpireSnapshots {
  private ImmutableExpireSnapshots() {}

  /**
   * Immutable implementation of {@link ExpireSnapshots.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableExpireSnapshots.Result.builder()}.
   */
  @Generated(from = "ExpireSnapshots.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements ExpireSnapshots.Result {
    private final long deletedDataFilesCount;
    private final long deletedEqualityDeleteFilesCount;
    private final long deletedPositionDeleteFilesCount;
    private final long deletedManifestsCount;
    private final long deletedManifestListsCount;
    private final long deletedStatisticsFilesCount;

    private Result(ImmutableExpireSnapshots.Result.Builder builder) {
      this.deletedDataFilesCount = builder.deletedDataFilesCount;
      this.deletedEqualityDeleteFilesCount = builder.deletedEqualityDeleteFilesCount;
      this.deletedPositionDeleteFilesCount = builder.deletedPositionDeleteFilesCount;
      this.deletedManifestsCount = builder.deletedManifestsCount;
      this.deletedManifestListsCount = builder.deletedManifestListsCount;
      this.deletedStatisticsFilesCount = builder.deletedStatisticsFilesCountIsSet()
          ? builder.deletedStatisticsFilesCount
          : ExpireSnapshots.Result.super.deletedStatisticsFilesCount();
    }

    private Result(
        long deletedDataFilesCount,
        long deletedEqualityDeleteFilesCount,
        long deletedPositionDeleteFilesCount,
        long deletedManifestsCount,
        long deletedManifestListsCount,
        long deletedStatisticsFilesCount) {
      this.deletedDataFilesCount = deletedDataFilesCount;
      this.deletedEqualityDeleteFilesCount = deletedEqualityDeleteFilesCount;
      this.deletedPositionDeleteFilesCount = deletedPositionDeleteFilesCount;
      this.deletedManifestsCount = deletedManifestsCount;
      this.deletedManifestListsCount = deletedManifestListsCount;
      this.deletedStatisticsFilesCount = deletedStatisticsFilesCount;
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
     *Returns the number of deleted statistics files. 
     */
    @Override
    public long deletedStatisticsFilesCount() {
      return deletedStatisticsFilesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedDataFilesCount() deletedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedDataFilesCount(long value) {
      if (this.deletedDataFilesCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
          value,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedStatisticsFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedEqualityDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedEqualityDeleteFilesCount(long value) {
      if (this.deletedEqualityDeleteFilesCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
          this.deletedDataFilesCount,
          value,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedStatisticsFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedPositionDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedPositionDeleteFilesCount(long value) {
      if (this.deletedPositionDeleteFilesCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          value,
          this.deletedManifestsCount,
          this.deletedManifestListsCount,
          this.deletedStatisticsFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedManifestsCount() deletedManifestsCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedManifestsCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedManifestsCount(long value) {
      if (this.deletedManifestsCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          value,
          this.deletedManifestListsCount,
          this.deletedStatisticsFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedManifestListsCount() deletedManifestListsCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedManifestListsCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedManifestListsCount(long value) {
      if (this.deletedManifestListsCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
          this.deletedDataFilesCount,
          this.deletedEqualityDeleteFilesCount,
          this.deletedPositionDeleteFilesCount,
          this.deletedManifestsCount,
          value,
          this.deletedStatisticsFilesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link ExpireSnapshots.Result#deletedStatisticsFilesCount() deletedStatisticsFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for deletedStatisticsFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableExpireSnapshots.Result withDeletedStatisticsFilesCount(long value) {
      if (this.deletedStatisticsFilesCount == value) return this;
      return new ImmutableExpireSnapshots.Result(
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
      return another instanceof ImmutableExpireSnapshots.Result
          && equalTo(0, (ImmutableExpireSnapshots.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableExpireSnapshots.Result another) {
      return deletedDataFilesCount == another.deletedDataFilesCount
          && deletedEqualityDeleteFilesCount == another.deletedEqualityDeleteFilesCount
          && deletedPositionDeleteFilesCount == another.deletedPositionDeleteFilesCount
          && deletedManifestsCount == another.deletedManifestsCount
          && deletedManifestListsCount == another.deletedManifestListsCount
          && deletedStatisticsFilesCount == another.deletedStatisticsFilesCount;
    }

    /**
     * Computes a hash code from attributes: {@code deletedDataFilesCount}, {@code deletedEqualityDeleteFilesCount}, {@code deletedPositionDeleteFilesCount}, {@code deletedManifestsCount}, {@code deletedManifestListsCount}, {@code deletedStatisticsFilesCount}.
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
      h += (h << 5) + Long.hashCode(deletedStatisticsFilesCount);
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
          + ", deletedStatisticsFilesCount=" + deletedStatisticsFilesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link ExpireSnapshots.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableExpireSnapshots.Result copyOf(ExpireSnapshots.Result instance) {
      if (instance instanceof ImmutableExpireSnapshots.Result) {
        return (ImmutableExpireSnapshots.Result) instance;
      }
      return ImmutableExpireSnapshots.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableExpireSnapshots.Result Result}.
     * <pre>
     * ImmutableExpireSnapshots.Result.builder()
     *    .deletedDataFilesCount(long) // required {@link ExpireSnapshots.Result#deletedDataFilesCount() deletedDataFilesCount}
     *    .deletedEqualityDeleteFilesCount(long) // required {@link ExpireSnapshots.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount}
     *    .deletedPositionDeleteFilesCount(long) // required {@link ExpireSnapshots.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount}
     *    .deletedManifestsCount(long) // required {@link ExpireSnapshots.Result#deletedManifestsCount() deletedManifestsCount}
     *    .deletedManifestListsCount(long) // required {@link ExpireSnapshots.Result#deletedManifestListsCount() deletedManifestListsCount}
     *    .deletedStatisticsFilesCount(long) // optional {@link ExpireSnapshots.Result#deletedStatisticsFilesCount() deletedStatisticsFilesCount}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableExpireSnapshots.Result.Builder builder() {
      return new ImmutableExpireSnapshots.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableExpireSnapshots.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "ExpireSnapshots.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_DELETED_DATA_FILES_COUNT = 0x1L;
      private static final long INIT_BIT_DELETED_EQUALITY_DELETE_FILES_COUNT = 0x2L;
      private static final long INIT_BIT_DELETED_POSITION_DELETE_FILES_COUNT = 0x4L;
      private static final long INIT_BIT_DELETED_MANIFESTS_COUNT = 0x8L;
      private static final long INIT_BIT_DELETED_MANIFEST_LISTS_COUNT = 0x10L;
      private static final long OPT_BIT_DELETED_STATISTICS_FILES_COUNT = 0x1L;
      private long initBits = 0x1fL;
      private long optBits;

      private long deletedDataFilesCount;
      private long deletedEqualityDeleteFilesCount;
      private long deletedPositionDeleteFilesCount;
      private long deletedManifestsCount;
      private long deletedManifestListsCount;
      private long deletedStatisticsFilesCount;

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
      public final Builder from(ExpireSnapshots.Result instance) {
        Objects.requireNonNull(instance, "instance");
        deletedDataFilesCount(instance.deletedDataFilesCount());
        deletedEqualityDeleteFilesCount(instance.deletedEqualityDeleteFilesCount());
        deletedPositionDeleteFilesCount(instance.deletedPositionDeleteFilesCount());
        deletedManifestsCount(instance.deletedManifestsCount());
        deletedManifestListsCount(instance.deletedManifestListsCount());
        deletedStatisticsFilesCount(instance.deletedStatisticsFilesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedDataFilesCount() deletedDataFilesCount} attribute.
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
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedEqualityDeleteFilesCount() deletedEqualityDeleteFilesCount} attribute.
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
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedPositionDeleteFilesCount() deletedPositionDeleteFilesCount} attribute.
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
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedManifestsCount() deletedManifestsCount} attribute.
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
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedManifestListsCount() deletedManifestListsCount} attribute.
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
       * Initializes the value for the {@link ExpireSnapshots.Result#deletedStatisticsFilesCount() deletedStatisticsFilesCount} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link ExpireSnapshots.Result#deletedStatisticsFilesCount() deletedStatisticsFilesCount}.</em>
       * @param deletedStatisticsFilesCount The value for deletedStatisticsFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder deletedStatisticsFilesCount(long deletedStatisticsFilesCount) {
        this.deletedStatisticsFilesCount = deletedStatisticsFilesCount;
        optBits |= OPT_BIT_DELETED_STATISTICS_FILES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableExpireSnapshots.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableExpireSnapshots.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableExpireSnapshots.Result(this);
      }

      private boolean deletedStatisticsFilesCountIsSet() {
        return (optBits & OPT_BIT_DELETED_STATISTICS_FILES_COUNT) != 0;
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_DELETED_DATA_FILES_COUNT) != 0) attributes.add("deletedDataFilesCount");
        if ((initBits & INIT_BIT_DELETED_EQUALITY_DELETE_FILES_COUNT) != 0) attributes.add("deletedEqualityDeleteFilesCount");
        if ((initBits & INIT_BIT_DELETED_POSITION_DELETE_FILES_COUNT) != 0) attributes.add("deletedPositionDeleteFilesCount");
        if ((initBits & INIT_BIT_DELETED_MANIFESTS_COUNT) != 0) attributes.add("deletedManifestsCount");
        if ((initBits & INIT_BIT_DELETED_MANIFEST_LISTS_COUNT) != 0) attributes.add("deletedManifestListsCount");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

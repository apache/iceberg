package org.apache.iceberg.actions;

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
import org.apache.iceberg.StructLike;
import org.immutables.value.Generated;

/**
 * {@code ImmutableRewritePositionDeleteFiles} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link RewritePositionDeleteFiles}.
 * @see ImmutableRewritePositionDeleteFiles.Result
 * @see ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult
 * @see ImmutableRewritePositionDeleteFiles.FileGroupInfo
 */
@Generated(from = "RewritePositionDeleteFiles", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableRewritePositionDeleteFiles {
  private ImmutableRewritePositionDeleteFiles() {}

  /**
   * Immutable implementation of {@link RewritePositionDeleteFiles.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewritePositionDeleteFiles.Result.builder()}.
   */
  @Generated(from = "RewritePositionDeleteFiles.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements RewritePositionDeleteFiles.Result {
    private final List<RewritePositionDeleteFiles.FileGroupRewriteResult> rewriteResults;

    private Result(
        List<RewritePositionDeleteFiles.FileGroupRewriteResult> rewriteResults) {
      this.rewriteResults = rewriteResults;
    }

    /**
     * @return The value of the {@code rewriteResults} attribute
     */
    @Override
    public List<RewritePositionDeleteFiles.FileGroupRewriteResult> rewriteResults() {
      return rewriteResults;
    }

    /**
     * Copy the current immutable object with elements that replace the content of {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults}.
     * @param elements The elements to set
     * @return A modified copy of {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.Result withRewriteResults(RewritePositionDeleteFiles.FileGroupRewriteResult... elements) {
      List<RewritePositionDeleteFiles.FileGroupRewriteResult> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
      return new ImmutableRewritePositionDeleteFiles.Result(newValue);
    }

    /**
     * Copy the current immutable object with elements that replace the content of {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults}.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param elements An iterable of rewriteResults elements to set
     * @return A modified copy of {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.Result withRewriteResults(Iterable<? extends RewritePositionDeleteFiles.FileGroupRewriteResult> elements) {
      if (this.rewriteResults == elements) return this;
      List<RewritePositionDeleteFiles.FileGroupRewriteResult> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
      return new ImmutableRewritePositionDeleteFiles.Result(newValue);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewritePositionDeleteFiles.Result
          && equalTo(0, (ImmutableRewritePositionDeleteFiles.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewritePositionDeleteFiles.Result another) {
      return rewriteResults.equals(another.rewriteResults);
    }

    /**
     * Computes a hash code from attributes: {@code rewriteResults}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + rewriteResults.hashCode();
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "rewriteResults=" + rewriteResults
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewritePositionDeleteFiles.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableRewritePositionDeleteFiles.Result copyOf(RewritePositionDeleteFiles.Result instance) {
      if (instance instanceof ImmutableRewritePositionDeleteFiles.Result) {
        return (ImmutableRewritePositionDeleteFiles.Result) instance;
      }
      return ImmutableRewritePositionDeleteFiles.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewritePositionDeleteFiles.Result Result}.
     * <pre>
     * ImmutableRewritePositionDeleteFiles.Result.builder()
     *    .addRewriteResults|addAllRewriteResults(org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupRewriteResult) // {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults} elements
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableRewritePositionDeleteFiles.Result.Builder builder() {
      return new ImmutableRewritePositionDeleteFiles.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewritePositionDeleteFiles.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewritePositionDeleteFiles.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private List<RewritePositionDeleteFiles.FileGroupRewriteResult> rewriteResults = new ArrayList<RewritePositionDeleteFiles.FileGroupRewriteResult>();

      private Builder() {
      }

      /**
       * Fill a builder with attribute values from the provided {@code Result} instance.
       * Regular attribute values will be replaced with those from the given instance.
       * Absent optional values will not replace present values.
       * Collection elements and entries will be added, not replaced.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(RewritePositionDeleteFiles.Result instance) {
        Objects.requireNonNull(instance, "instance");
        addAllRewriteResults(instance.rewriteResults());
        return this;
      }

      /**
       * Adds one element to {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults} list.
       * @param element A rewriteResults element
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addRewriteResults(RewritePositionDeleteFiles.FileGroupRewriteResult element) {
        this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        return this;
      }

      /**
       * Adds elements to {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An array of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addRewriteResults(RewritePositionDeleteFiles.FileGroupRewriteResult... elements) {
        for (RewritePositionDeleteFiles.FileGroupRewriteResult element : elements) {
          this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        }
        return this;
      }


      /**
       * Sets or replaces all elements for {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An iterable of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewriteResults(Iterable<? extends RewritePositionDeleteFiles.FileGroupRewriteResult> elements) {
        this.rewriteResults.clear();
        return addAllRewriteResults(elements);
      }

      /**
       * Adds elements to {@link RewritePositionDeleteFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An iterable of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addAllRewriteResults(Iterable<? extends RewritePositionDeleteFiles.FileGroupRewriteResult> elements) {
        for (RewritePositionDeleteFiles.FileGroupRewriteResult element : elements) {
          this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        }
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewritePositionDeleteFiles.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewritePositionDeleteFiles.Result build() {
        return new ImmutableRewritePositionDeleteFiles.Result(createUnmodifiableList(true, rewriteResults));
      }
    }
  }

  /**
   * Immutable implementation of {@link RewritePositionDeleteFiles.FileGroupRewriteResult}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.builder()}.
   */
  @Generated(from = "RewritePositionDeleteFiles.FileGroupRewriteResult", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class FileGroupRewriteResult
      implements RewritePositionDeleteFiles.FileGroupRewriteResult {
    private final RewritePositionDeleteFiles.FileGroupInfo info;
    private final int rewrittenDeleteFilesCount;
    private final int addedDeleteFilesCount;
    private final long rewrittenBytesCount;
    private final long addedBytesCount;

    private FileGroupRewriteResult(
        RewritePositionDeleteFiles.FileGroupInfo info,
        int rewrittenDeleteFilesCount,
        int addedDeleteFilesCount,
        long rewrittenBytesCount,
        long addedBytesCount) {
      this.info = info;
      this.rewrittenDeleteFilesCount = rewrittenDeleteFilesCount;
      this.addedDeleteFilesCount = addedDeleteFilesCount;
      this.rewrittenBytesCount = rewrittenBytesCount;
      this.addedBytesCount = addedBytesCount;
    }

    /**
     *Description of this position delete file group. 
     */
    @Override
    public RewritePositionDeleteFiles.FileGroupInfo info() {
      return info;
    }

    /**
     *Returns the count of the position delete files that been rewritten in this group. 
     */
    @Override
    public int rewrittenDeleteFilesCount() {
      return rewrittenDeleteFilesCount;
    }

    /**
     *Returns the count of the added position delete files in this group. 
     */
    @Override
    public int addedDeleteFilesCount() {
      return addedDeleteFilesCount;
    }

    /**
     *Returns the number of bytes of rewritten position delete files in this group. 
     */
    @Override
    public long rewrittenBytesCount() {
      return rewrittenBytesCount;
    }

    /**
     *Returns the number of bytes of newly added position delete files in this group. 
     */
    @Override
    public long addedBytesCount() {
      return addedBytesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#info() info} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for info
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult withInfo(RewritePositionDeleteFiles.FileGroupInfo value) {
      if (this.info == value) return this;
      RewritePositionDeleteFiles.FileGroupInfo newValue = Objects.requireNonNull(value, "info");
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(
          newValue,
          this.rewrittenDeleteFilesCount,
          this.addedDeleteFilesCount,
          this.rewrittenBytesCount,
          this.addedBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenDeleteFilesCount() rewrittenDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult withRewrittenDeleteFilesCount(int value) {
      if (this.rewrittenDeleteFilesCount == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(this.info, value, this.addedDeleteFilesCount, this.rewrittenBytesCount, this.addedBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedDeleteFilesCount() addedDeleteFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for addedDeleteFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult withAddedDeleteFilesCount(int value) {
      if (this.addedDeleteFilesCount == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(
          this.info,
          this.rewrittenDeleteFilesCount,
          value,
          this.rewrittenBytesCount,
          this.addedBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenBytesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult withRewrittenBytesCount(long value) {
      if (this.rewrittenBytesCount == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(
          this.info,
          this.rewrittenDeleteFilesCount,
          this.addedDeleteFilesCount,
          value,
          this.addedBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedBytesCount() addedBytesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for addedBytesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult withAddedBytesCount(long value) {
      if (this.addedBytesCount == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(
          this.info,
          this.rewrittenDeleteFilesCount,
          this.addedDeleteFilesCount,
          this.rewrittenBytesCount,
          value);
    }

    /**
     * This instance is equal to all instances of {@code FileGroupRewriteResult} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult
          && equalTo(0, (ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult another) {
      return info.equals(another.info)
          && rewrittenDeleteFilesCount == another.rewrittenDeleteFilesCount
          && addedDeleteFilesCount == another.addedDeleteFilesCount
          && rewrittenBytesCount == another.rewrittenBytesCount
          && addedBytesCount == another.addedBytesCount;
    }

    /**
     * Computes a hash code from attributes: {@code info}, {@code rewrittenDeleteFilesCount}, {@code addedDeleteFilesCount}, {@code rewrittenBytesCount}, {@code addedBytesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + info.hashCode();
      h += (h << 5) + rewrittenDeleteFilesCount;
      h += (h << 5) + addedDeleteFilesCount;
      h += (h << 5) + Long.hashCode(rewrittenBytesCount);
      h += (h << 5) + Long.hashCode(addedBytesCount);
      return h;
    }

    /**
     * Prints the immutable value {@code FileGroupRewriteResult} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "FileGroupRewriteResult{"
          + "info=" + info
          + ", rewrittenDeleteFilesCount=" + rewrittenDeleteFilesCount
          + ", addedDeleteFilesCount=" + addedDeleteFilesCount
          + ", rewrittenBytesCount=" + rewrittenBytesCount
          + ", addedBytesCount=" + addedBytesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewritePositionDeleteFiles.FileGroupRewriteResult} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable FileGroupRewriteResult instance
     */
    public static ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult copyOf(RewritePositionDeleteFiles.FileGroupRewriteResult instance) {
      if (instance instanceof ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult) {
        return (ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult) instance;
      }
      return ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult FileGroupRewriteResult}.
     * <pre>
     * ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.builder()
     *    .info(org.apache.iceberg.actions.RewritePositionDeleteFiles.FileGroupInfo) // required {@link RewritePositionDeleteFiles.FileGroupRewriteResult#info() info}
     *    .rewrittenDeleteFilesCount(int) // required {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenDeleteFilesCount() rewrittenDeleteFilesCount}
     *    .addedDeleteFilesCount(int) // required {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedDeleteFilesCount() addedDeleteFilesCount}
     *    .rewrittenBytesCount(long) // required {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount}
     *    .addedBytesCount(long) // required {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedBytesCount() addedBytesCount}
     *    .build();
     * </pre>
     * @return A new FileGroupRewriteResult builder
     */
    public static ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.Builder builder() {
      return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult FileGroupRewriteResult}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewritePositionDeleteFiles.FileGroupRewriteResult", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_INFO = 0x1L;
      private static final long INIT_BIT_REWRITTEN_DELETE_FILES_COUNT = 0x2L;
      private static final long INIT_BIT_ADDED_DELETE_FILES_COUNT = 0x4L;
      private static final long INIT_BIT_REWRITTEN_BYTES_COUNT = 0x8L;
      private static final long INIT_BIT_ADDED_BYTES_COUNT = 0x10L;
      private long initBits = 0x1fL;

      private @Nullable RewritePositionDeleteFiles.FileGroupInfo info;
      private int rewrittenDeleteFilesCount;
      private int addedDeleteFilesCount;
      private long rewrittenBytesCount;
      private long addedBytesCount;

      private Builder() {
      }

      /**
       * Fill a builder with attribute values from the provided {@code FileGroupRewriteResult} instance.
       * Regular attribute values will be replaced with those from the given instance.
       * Absent optional values will not replace present values.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(RewritePositionDeleteFiles.FileGroupRewriteResult instance) {
        Objects.requireNonNull(instance, "instance");
        info(instance.info());
        rewrittenDeleteFilesCount(instance.rewrittenDeleteFilesCount());
        addedDeleteFilesCount(instance.addedDeleteFilesCount());
        rewrittenBytesCount(instance.rewrittenBytesCount());
        addedBytesCount(instance.addedBytesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#info() info} attribute.
       * @param info The value for info 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder info(RewritePositionDeleteFiles.FileGroupInfo info) {
        this.info = Objects.requireNonNull(info, "info");
        initBits &= ~INIT_BIT_INFO;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenDeleteFilesCount() rewrittenDeleteFilesCount} attribute.
       * @param rewrittenDeleteFilesCount The value for rewrittenDeleteFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenDeleteFilesCount(int rewrittenDeleteFilesCount) {
        this.rewrittenDeleteFilesCount = rewrittenDeleteFilesCount;
        initBits &= ~INIT_BIT_REWRITTEN_DELETE_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedDeleteFilesCount() addedDeleteFilesCount} attribute.
       * @param addedDeleteFilesCount The value for addedDeleteFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addedDeleteFilesCount(int addedDeleteFilesCount) {
        this.addedDeleteFilesCount = addedDeleteFilesCount;
        initBits &= ~INIT_BIT_ADDED_DELETE_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount} attribute.
       * @param rewrittenBytesCount The value for rewrittenBytesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenBytesCount(long rewrittenBytesCount) {
        this.rewrittenBytesCount = rewrittenBytesCount;
        initBits &= ~INIT_BIT_REWRITTEN_BYTES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupRewriteResult#addedBytesCount() addedBytesCount} attribute.
       * @param addedBytesCount The value for addedBytesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addedBytesCount(long addedBytesCount) {
        this.addedBytesCount = addedBytesCount;
        initBits &= ~INIT_BIT_ADDED_BYTES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult FileGroupRewriteResult}.
       * @return An immutable instance of FileGroupRewriteResult
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableRewritePositionDeleteFiles.FileGroupRewriteResult(
            info,
            rewrittenDeleteFilesCount,
            addedDeleteFilesCount,
            rewrittenBytesCount,
            addedBytesCount);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_INFO) != 0) attributes.add("info");
        if ((initBits & INIT_BIT_REWRITTEN_DELETE_FILES_COUNT) != 0) attributes.add("rewrittenDeleteFilesCount");
        if ((initBits & INIT_BIT_ADDED_DELETE_FILES_COUNT) != 0) attributes.add("addedDeleteFilesCount");
        if ((initBits & INIT_BIT_REWRITTEN_BYTES_COUNT) != 0) attributes.add("rewrittenBytesCount");
        if ((initBits & INIT_BIT_ADDED_BYTES_COUNT) != 0) attributes.add("addedBytesCount");
        return "Cannot build FileGroupRewriteResult, some of required attributes are not set " + attributes;
      }
    }
  }

  /**
   * Immutable implementation of {@link RewritePositionDeleteFiles.FileGroupInfo}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewritePositionDeleteFiles.FileGroupInfo.builder()}.
   */
  @Generated(from = "RewritePositionDeleteFiles.FileGroupInfo", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class FileGroupInfo
      implements RewritePositionDeleteFiles.FileGroupInfo {
    private final int globalIndex;
    private final int partitionIndex;
    private final StructLike partition;

    private FileGroupInfo(int globalIndex, int partitionIndex, StructLike partition) {
      this.globalIndex = globalIndex;
      this.partitionIndex = partitionIndex;
      this.partition = partition;
    }

    /**
     * Returns which position delete file group this is out of the total set of file groups for this
     * rewrite
     */
    @Override
    public int globalIndex() {
      return globalIndex;
    }

    /**
     * Returns which position delete file group this is out of the set of file groups for this
     * partition
     */
    @Override
    public int partitionIndex() {
      return partitionIndex;
    }

    /**
     * Returns which partition this position delete file group contains files from. This will be of
     * the type of the table's unified partition type considering all specs in a table.
     */
    @Override
    public StructLike partition() {
      return partition;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupInfo#globalIndex() globalIndex} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for globalIndex
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupInfo withGlobalIndex(int value) {
      if (this.globalIndex == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupInfo(value, this.partitionIndex, this.partition);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupInfo#partitionIndex() partitionIndex} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for partitionIndex
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupInfo withPartitionIndex(int value) {
      if (this.partitionIndex == value) return this;
      return new ImmutableRewritePositionDeleteFiles.FileGroupInfo(this.globalIndex, value, this.partition);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewritePositionDeleteFiles.FileGroupInfo#partition() partition} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for partition
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewritePositionDeleteFiles.FileGroupInfo withPartition(StructLike value) {
      if (this.partition == value) return this;
      StructLike newValue = Objects.requireNonNull(value, "partition");
      return new ImmutableRewritePositionDeleteFiles.FileGroupInfo(this.globalIndex, this.partitionIndex, newValue);
    }

    /**
     * This instance is equal to all instances of {@code FileGroupInfo} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewritePositionDeleteFiles.FileGroupInfo
          && equalTo(0, (ImmutableRewritePositionDeleteFiles.FileGroupInfo) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewritePositionDeleteFiles.FileGroupInfo another) {
      return globalIndex == another.globalIndex
          && partitionIndex == another.partitionIndex
          && partition.equals(another.partition);
    }

    /**
     * Computes a hash code from attributes: {@code globalIndex}, {@code partitionIndex}, {@code partition}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + globalIndex;
      h += (h << 5) + partitionIndex;
      h += (h << 5) + partition.hashCode();
      return h;
    }

    /**
     * Prints the immutable value {@code FileGroupInfo} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "FileGroupInfo{"
          + "globalIndex=" + globalIndex
          + ", partitionIndex=" + partitionIndex
          + ", partition=" + partition
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewritePositionDeleteFiles.FileGroupInfo} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable FileGroupInfo instance
     */
    public static ImmutableRewritePositionDeleteFiles.FileGroupInfo copyOf(RewritePositionDeleteFiles.FileGroupInfo instance) {
      if (instance instanceof ImmutableRewritePositionDeleteFiles.FileGroupInfo) {
        return (ImmutableRewritePositionDeleteFiles.FileGroupInfo) instance;
      }
      return ImmutableRewritePositionDeleteFiles.FileGroupInfo.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewritePositionDeleteFiles.FileGroupInfo FileGroupInfo}.
     * <pre>
     * ImmutableRewritePositionDeleteFiles.FileGroupInfo.builder()
     *    .globalIndex(int) // required {@link RewritePositionDeleteFiles.FileGroupInfo#globalIndex() globalIndex}
     *    .partitionIndex(int) // required {@link RewritePositionDeleteFiles.FileGroupInfo#partitionIndex() partitionIndex}
     *    .partition(org.apache.iceberg.StructLike) // required {@link RewritePositionDeleteFiles.FileGroupInfo#partition() partition}
     *    .build();
     * </pre>
     * @return A new FileGroupInfo builder
     */
    public static ImmutableRewritePositionDeleteFiles.FileGroupInfo.Builder builder() {
      return new ImmutableRewritePositionDeleteFiles.FileGroupInfo.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewritePositionDeleteFiles.FileGroupInfo FileGroupInfo}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewritePositionDeleteFiles.FileGroupInfo", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_GLOBAL_INDEX = 0x1L;
      private static final long INIT_BIT_PARTITION_INDEX = 0x2L;
      private static final long INIT_BIT_PARTITION = 0x4L;
      private long initBits = 0x7L;

      private int globalIndex;
      private int partitionIndex;
      private @Nullable StructLike partition;

      private Builder() {
      }

      /**
       * Fill a builder with attribute values from the provided {@code FileGroupInfo} instance.
       * Regular attribute values will be replaced with those from the given instance.
       * Absent optional values will not replace present values.
       * @param instance The instance from which to copy values
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder from(RewritePositionDeleteFiles.FileGroupInfo instance) {
        Objects.requireNonNull(instance, "instance");
        globalIndex(instance.globalIndex());
        partitionIndex(instance.partitionIndex());
        partition(instance.partition());
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupInfo#globalIndex() globalIndex} attribute.
       * @param globalIndex The value for globalIndex 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder globalIndex(int globalIndex) {
        this.globalIndex = globalIndex;
        initBits &= ~INIT_BIT_GLOBAL_INDEX;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupInfo#partitionIndex() partitionIndex} attribute.
       * @param partitionIndex The value for partitionIndex 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder partitionIndex(int partitionIndex) {
        this.partitionIndex = partitionIndex;
        initBits &= ~INIT_BIT_PARTITION_INDEX;
        return this;
      }

      /**
       * Initializes the value for the {@link RewritePositionDeleteFiles.FileGroupInfo#partition() partition} attribute.
       * @param partition The value for partition 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder partition(StructLike partition) {
        this.partition = Objects.requireNonNull(partition, "partition");
        initBits &= ~INIT_BIT_PARTITION;
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewritePositionDeleteFiles.FileGroupInfo FileGroupInfo}.
       * @return An immutable instance of FileGroupInfo
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewritePositionDeleteFiles.FileGroupInfo build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableRewritePositionDeleteFiles.FileGroupInfo(globalIndex, partitionIndex, partition);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_GLOBAL_INDEX) != 0) attributes.add("globalIndex");
        if ((initBits & INIT_BIT_PARTITION_INDEX) != 0) attributes.add("partitionIndex");
        if ((initBits & INIT_BIT_PARTITION) != 0) attributes.add("partition");
        return "Cannot build FileGroupInfo, some of required attributes are not set " + attributes;
      }
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

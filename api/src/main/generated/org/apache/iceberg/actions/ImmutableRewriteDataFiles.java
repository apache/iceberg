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
 * {@code ImmutableRewriteDataFiles} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link RewriteDataFiles}.
 * @see ImmutableRewriteDataFiles.Result
 * @see ImmutableRewriteDataFiles.FileGroupRewriteResult
 * @see ImmutableRewriteDataFiles.FileGroupInfo
 */
@Generated(from = "RewriteDataFiles", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableRewriteDataFiles {
  private ImmutableRewriteDataFiles() {}

  /**
   * Immutable implementation of {@link RewriteDataFiles.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewriteDataFiles.Result.builder()}.
   */
  @Generated(from = "RewriteDataFiles.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements RewriteDataFiles.Result {
    private final List<RewriteDataFiles.FileGroupRewriteResult> rewriteResults;
    private final int addedDataFilesCount;
    private final int rewrittenDataFilesCount;
    private final long rewrittenBytesCount;

    private Result(ImmutableRewriteDataFiles.Result.Builder builder) {
      this.rewriteResults = createUnmodifiableList(true, builder.rewriteResults);
      if (builder.addedDataFilesCountIsSet()) {
        initShim.addedDataFilesCount(builder.addedDataFilesCount);
      }
      if (builder.rewrittenDataFilesCountIsSet()) {
        initShim.rewrittenDataFilesCount(builder.rewrittenDataFilesCount);
      }
      if (builder.rewrittenBytesCountIsSet()) {
        initShim.rewrittenBytesCount(builder.rewrittenBytesCount);
      }
      this.addedDataFilesCount = initShim.addedDataFilesCount();
      this.rewrittenDataFilesCount = initShim.rewrittenDataFilesCount();
      this.rewrittenBytesCount = initShim.rewrittenBytesCount();
      this.initShim = null;
    }

    private Result(
        List<RewriteDataFiles.FileGroupRewriteResult> rewriteResults,
        int addedDataFilesCount,
        int rewrittenDataFilesCount,
        long rewrittenBytesCount) {
      this.rewriteResults = rewriteResults;
      this.addedDataFilesCount = addedDataFilesCount;
      this.rewrittenDataFilesCount = rewrittenDataFilesCount;
      this.rewrittenBytesCount = rewrittenBytesCount;
      this.initShim = null;
    }

    private static final byte STAGE_INITIALIZING = -1;
    private static final byte STAGE_UNINITIALIZED = 0;
    private static final byte STAGE_INITIALIZED = 1;
    @SuppressWarnings("Immutable")
    private transient volatile InitShim initShim = new InitShim();

    @Generated(from = "RewriteDataFiles.Result", generator = "Immutables")
    private final class InitShim {
      private byte addedDataFilesCountBuildStage = STAGE_UNINITIALIZED;
      private int addedDataFilesCount;

      int addedDataFilesCount() {
        if (addedDataFilesCountBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
        if (addedDataFilesCountBuildStage == STAGE_UNINITIALIZED) {
          addedDataFilesCountBuildStage = STAGE_INITIALIZING;
          this.addedDataFilesCount = addedDataFilesCountInitialize();
          addedDataFilesCountBuildStage = STAGE_INITIALIZED;
        }
        return this.addedDataFilesCount;
      }

      void addedDataFilesCount(int addedDataFilesCount) {
        this.addedDataFilesCount = addedDataFilesCount;
        addedDataFilesCountBuildStage = STAGE_INITIALIZED;
      }

      private byte rewrittenDataFilesCountBuildStage = STAGE_UNINITIALIZED;
      private int rewrittenDataFilesCount;

      int rewrittenDataFilesCount() {
        if (rewrittenDataFilesCountBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
        if (rewrittenDataFilesCountBuildStage == STAGE_UNINITIALIZED) {
          rewrittenDataFilesCountBuildStage = STAGE_INITIALIZING;
          this.rewrittenDataFilesCount = rewrittenDataFilesCountInitialize();
          rewrittenDataFilesCountBuildStage = STAGE_INITIALIZED;
        }
        return this.rewrittenDataFilesCount;
      }

      void rewrittenDataFilesCount(int rewrittenDataFilesCount) {
        this.rewrittenDataFilesCount = rewrittenDataFilesCount;
        rewrittenDataFilesCountBuildStage = STAGE_INITIALIZED;
      }

      private byte rewrittenBytesCountBuildStage = STAGE_UNINITIALIZED;
      private long rewrittenBytesCount;

      long rewrittenBytesCount() {
        if (rewrittenBytesCountBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
        if (rewrittenBytesCountBuildStage == STAGE_UNINITIALIZED) {
          rewrittenBytesCountBuildStage = STAGE_INITIALIZING;
          this.rewrittenBytesCount = rewrittenBytesCountInitialize();
          rewrittenBytesCountBuildStage = STAGE_INITIALIZED;
        }
        return this.rewrittenBytesCount;
      }

      void rewrittenBytesCount(long rewrittenBytesCount) {
        this.rewrittenBytesCount = rewrittenBytesCount;
        rewrittenBytesCountBuildStage = STAGE_INITIALIZED;
      }

      private String formatInitCycleMessage() {
        List<String> attributes = new ArrayList<>();
        if (addedDataFilesCountBuildStage == STAGE_INITIALIZING) attributes.add("addedDataFilesCount");
        if (rewrittenDataFilesCountBuildStage == STAGE_INITIALIZING) attributes.add("rewrittenDataFilesCount");
        if (rewrittenBytesCountBuildStage == STAGE_INITIALIZING) attributes.add("rewrittenBytesCount");
        return "Cannot build Result, attribute initializers form cycle " + attributes;
      }
    }

    private int addedDataFilesCountInitialize() {
      return RewriteDataFiles.Result.super.addedDataFilesCount();
    }

    private int rewrittenDataFilesCountInitialize() {
      return RewriteDataFiles.Result.super.rewrittenDataFilesCount();
    }

    private long rewrittenBytesCountInitialize() {
      return RewriteDataFiles.Result.super.rewrittenBytesCount();
    }

    /**
     * @return The value of the {@code rewriteResults} attribute
     */
    @Override
    public List<RewriteDataFiles.FileGroupRewriteResult> rewriteResults() {
      return rewriteResults;
    }

    /**
     * @return The value of the {@code addedDataFilesCount} attribute
     */
    @Override
    public int addedDataFilesCount() {
      InitShim shim = this.initShim;
      return shim != null
          ? shim.addedDataFilesCount()
          : this.addedDataFilesCount;
    }

    /**
     * @return The value of the {@code rewrittenDataFilesCount} attribute
     */
    @Override
    public int rewrittenDataFilesCount() {
      InitShim shim = this.initShim;
      return shim != null
          ? shim.rewrittenDataFilesCount()
          : this.rewrittenDataFilesCount;
    }

    /**
     * @return The value of the {@code rewrittenBytesCount} attribute
     */
    @Override
    public long rewrittenBytesCount() {
      InitShim shim = this.initShim;
      return shim != null
          ? shim.rewrittenBytesCount()
          : this.rewrittenBytesCount;
    }

    /**
     * Copy the current immutable object with elements that replace the content of {@link RewriteDataFiles.Result#rewriteResults() rewriteResults}.
     * @param elements The elements to set
     * @return A modified copy of {@code this} object
     */
    public final ImmutableRewriteDataFiles.Result withRewriteResults(RewriteDataFiles.FileGroupRewriteResult... elements) {
      List<RewriteDataFiles.FileGroupRewriteResult> newValue = createUnmodifiableList(false, createSafeList(Arrays.asList(elements), true, false));
      return new ImmutableRewriteDataFiles.Result(newValue, this.addedDataFilesCount, this.rewrittenDataFilesCount, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object with elements that replace the content of {@link RewriteDataFiles.Result#rewriteResults() rewriteResults}.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param elements An iterable of rewriteResults elements to set
     * @return A modified copy of {@code this} object
     */
    public final ImmutableRewriteDataFiles.Result withRewriteResults(Iterable<? extends RewriteDataFiles.FileGroupRewriteResult> elements) {
      if (this.rewriteResults == elements) return this;
      List<RewriteDataFiles.FileGroupRewriteResult> newValue = createUnmodifiableList(false, createSafeList(elements, true, false));
      return new ImmutableRewriteDataFiles.Result(newValue, this.addedDataFilesCount, this.rewrittenDataFilesCount, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.Result#addedDataFilesCount() addedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for addedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.Result withAddedDataFilesCount(int value) {
      if (this.addedDataFilesCount == value) return this;
      return new ImmutableRewriteDataFiles.Result(this.rewriteResults, value, this.rewrittenDataFilesCount, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.Result#rewrittenDataFilesCount() rewrittenDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.Result withRewrittenDataFilesCount(int value) {
      if (this.rewrittenDataFilesCount == value) return this;
      return new ImmutableRewriteDataFiles.Result(this.rewriteResults, this.addedDataFilesCount, value, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.Result#rewrittenBytesCount() rewrittenBytesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenBytesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.Result withRewrittenBytesCount(long value) {
      if (this.rewrittenBytesCount == value) return this;
      return new ImmutableRewriteDataFiles.Result(this.rewriteResults, this.addedDataFilesCount, this.rewrittenDataFilesCount, value);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewriteDataFiles.Result
          && equalTo(0, (ImmutableRewriteDataFiles.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewriteDataFiles.Result another) {
      return rewriteResults.equals(another.rewriteResults)
          && addedDataFilesCount == another.addedDataFilesCount
          && rewrittenDataFilesCount == another.rewrittenDataFilesCount
          && rewrittenBytesCount == another.rewrittenBytesCount;
    }

    /**
     * Computes a hash code from attributes: {@code rewriteResults}, {@code addedDataFilesCount}, {@code rewrittenDataFilesCount}, {@code rewrittenBytesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + rewriteResults.hashCode();
      h += (h << 5) + addedDataFilesCount;
      h += (h << 5) + rewrittenDataFilesCount;
      h += (h << 5) + Long.hashCode(rewrittenBytesCount);
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
          + ", addedDataFilesCount=" + addedDataFilesCount
          + ", rewrittenDataFilesCount=" + rewrittenDataFilesCount
          + ", rewrittenBytesCount=" + rewrittenBytesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewriteDataFiles.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableRewriteDataFiles.Result copyOf(RewriteDataFiles.Result instance) {
      if (instance instanceof ImmutableRewriteDataFiles.Result) {
        return (ImmutableRewriteDataFiles.Result) instance;
      }
      return ImmutableRewriteDataFiles.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewriteDataFiles.Result Result}.
     * <pre>
     * ImmutableRewriteDataFiles.Result.builder()
     *    .addRewriteResults|addAllRewriteResults(org.apache.iceberg.actions.RewriteDataFiles.FileGroupRewriteResult) // {@link RewriteDataFiles.Result#rewriteResults() rewriteResults} elements
     *    .addedDataFilesCount(int) // optional {@link RewriteDataFiles.Result#addedDataFilesCount() addedDataFilesCount}
     *    .rewrittenDataFilesCount(int) // optional {@link RewriteDataFiles.Result#rewrittenDataFilesCount() rewrittenDataFilesCount}
     *    .rewrittenBytesCount(long) // optional {@link RewriteDataFiles.Result#rewrittenBytesCount() rewrittenBytesCount}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableRewriteDataFiles.Result.Builder builder() {
      return new ImmutableRewriteDataFiles.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewriteDataFiles.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewriteDataFiles.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long OPT_BIT_ADDED_DATA_FILES_COUNT = 0x1L;
      private static final long OPT_BIT_REWRITTEN_DATA_FILES_COUNT = 0x2L;
      private static final long OPT_BIT_REWRITTEN_BYTES_COUNT = 0x4L;
      private long optBits;

      private List<RewriteDataFiles.FileGroupRewriteResult> rewriteResults = new ArrayList<RewriteDataFiles.FileGroupRewriteResult>();
      private int addedDataFilesCount;
      private int rewrittenDataFilesCount;
      private long rewrittenBytesCount;

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
      public final Builder from(RewriteDataFiles.Result instance) {
        Objects.requireNonNull(instance, "instance");
        addAllRewriteResults(instance.rewriteResults());
        addedDataFilesCount(instance.addedDataFilesCount());
        rewrittenDataFilesCount(instance.rewrittenDataFilesCount());
        rewrittenBytesCount(instance.rewrittenBytesCount());
        return this;
      }

      /**
       * Adds one element to {@link RewriteDataFiles.Result#rewriteResults() rewriteResults} list.
       * @param element A rewriteResults element
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addRewriteResults(RewriteDataFiles.FileGroupRewriteResult element) {
        this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        return this;
      }

      /**
       * Adds elements to {@link RewriteDataFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An array of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addRewriteResults(RewriteDataFiles.FileGroupRewriteResult... elements) {
        for (RewriteDataFiles.FileGroupRewriteResult element : elements) {
          this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        }
        return this;
      }


      /**
       * Sets or replaces all elements for {@link RewriteDataFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An iterable of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewriteResults(Iterable<? extends RewriteDataFiles.FileGroupRewriteResult> elements) {
        this.rewriteResults.clear();
        return addAllRewriteResults(elements);
      }

      /**
       * Adds elements to {@link RewriteDataFiles.Result#rewriteResults() rewriteResults} list.
       * @param elements An iterable of rewriteResults elements
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addAllRewriteResults(Iterable<? extends RewriteDataFiles.FileGroupRewriteResult> elements) {
        for (RewriteDataFiles.FileGroupRewriteResult element : elements) {
          this.rewriteResults.add(Objects.requireNonNull(element, "rewriteResults element"));
        }
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.Result#addedDataFilesCount() addedDataFilesCount} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RewriteDataFiles.Result#addedDataFilesCount() addedDataFilesCount}.</em>
       * @param addedDataFilesCount The value for addedDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addedDataFilesCount(int addedDataFilesCount) {
        this.addedDataFilesCount = addedDataFilesCount;
        optBits |= OPT_BIT_ADDED_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.Result#rewrittenDataFilesCount() rewrittenDataFilesCount} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RewriteDataFiles.Result#rewrittenDataFilesCount() rewrittenDataFilesCount}.</em>
       * @param rewrittenDataFilesCount The value for rewrittenDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenDataFilesCount(int rewrittenDataFilesCount) {
        this.rewrittenDataFilesCount = rewrittenDataFilesCount;
        optBits |= OPT_BIT_REWRITTEN_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.Result#rewrittenBytesCount() rewrittenBytesCount} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RewriteDataFiles.Result#rewrittenBytesCount() rewrittenBytesCount}.</em>
       * @param rewrittenBytesCount The value for rewrittenBytesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenBytesCount(long rewrittenBytesCount) {
        this.rewrittenBytesCount = rewrittenBytesCount;
        optBits |= OPT_BIT_REWRITTEN_BYTES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewriteDataFiles.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewriteDataFiles.Result build() {
        return new ImmutableRewriteDataFiles.Result(this);
      }

      private boolean addedDataFilesCountIsSet() {
        return (optBits & OPT_BIT_ADDED_DATA_FILES_COUNT) != 0;
      }

      private boolean rewrittenDataFilesCountIsSet() {
        return (optBits & OPT_BIT_REWRITTEN_DATA_FILES_COUNT) != 0;
      }

      private boolean rewrittenBytesCountIsSet() {
        return (optBits & OPT_BIT_REWRITTEN_BYTES_COUNT) != 0;
      }
    }
  }

  /**
   * Immutable implementation of {@link RewriteDataFiles.FileGroupRewriteResult}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewriteDataFiles.FileGroupRewriteResult.builder()}.
   */
  @Generated(from = "RewriteDataFiles.FileGroupRewriteResult", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class FileGroupRewriteResult
      implements RewriteDataFiles.FileGroupRewriteResult {
    private final RewriteDataFiles.FileGroupInfo info;
    private final int addedDataFilesCount;
    private final int rewrittenDataFilesCount;
    private final long rewrittenBytesCount;

    private FileGroupRewriteResult(ImmutableRewriteDataFiles.FileGroupRewriteResult.Builder builder) {
      this.info = builder.info;
      this.addedDataFilesCount = builder.addedDataFilesCount;
      this.rewrittenDataFilesCount = builder.rewrittenDataFilesCount;
      this.rewrittenBytesCount = builder.rewrittenBytesCountIsSet()
          ? builder.rewrittenBytesCount
          : RewriteDataFiles.FileGroupRewriteResult.super.rewrittenBytesCount();
    }

    private FileGroupRewriteResult(
        RewriteDataFiles.FileGroupInfo info,
        int addedDataFilesCount,
        int rewrittenDataFilesCount,
        long rewrittenBytesCount) {
      this.info = info;
      this.addedDataFilesCount = addedDataFilesCount;
      this.rewrittenDataFilesCount = rewrittenDataFilesCount;
      this.rewrittenBytesCount = rewrittenBytesCount;
    }

    /**
     * @return The value of the {@code info} attribute
     */
    @Override
    public RewriteDataFiles.FileGroupInfo info() {
      return info;
    }

    /**
     * @return The value of the {@code addedDataFilesCount} attribute
     */
    @Override
    public int addedDataFilesCount() {
      return addedDataFilesCount;
    }

    /**
     * @return The value of the {@code rewrittenDataFilesCount} attribute
     */
    @Override
    public int rewrittenDataFilesCount() {
      return rewrittenDataFilesCount;
    }

    /**
     * @return The value of the {@code rewrittenBytesCount} attribute
     */
    @Override
    public long rewrittenBytesCount() {
      return rewrittenBytesCount;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupRewriteResult#info() info} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for info
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupRewriteResult withInfo(RewriteDataFiles.FileGroupInfo value) {
      if (this.info == value) return this;
      RewriteDataFiles.FileGroupInfo newValue = Objects.requireNonNull(value, "info");
      return new ImmutableRewriteDataFiles.FileGroupRewriteResult(newValue, this.addedDataFilesCount, this.rewrittenDataFilesCount, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupRewriteResult#addedDataFilesCount() addedDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for addedDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupRewriteResult withAddedDataFilesCount(int value) {
      if (this.addedDataFilesCount == value) return this;
      return new ImmutableRewriteDataFiles.FileGroupRewriteResult(this.info, value, this.rewrittenDataFilesCount, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenDataFilesCount() rewrittenDataFilesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenDataFilesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupRewriteResult withRewrittenDataFilesCount(int value) {
      if (this.rewrittenDataFilesCount == value) return this;
      return new ImmutableRewriteDataFiles.FileGroupRewriteResult(this.info, this.addedDataFilesCount, value, this.rewrittenBytesCount);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenBytesCount
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupRewriteResult withRewrittenBytesCount(long value) {
      if (this.rewrittenBytesCount == value) return this;
      return new ImmutableRewriteDataFiles.FileGroupRewriteResult(this.info, this.addedDataFilesCount, this.rewrittenDataFilesCount, value);
    }

    /**
     * This instance is equal to all instances of {@code FileGroupRewriteResult} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewriteDataFiles.FileGroupRewriteResult
          && equalTo(0, (ImmutableRewriteDataFiles.FileGroupRewriteResult) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewriteDataFiles.FileGroupRewriteResult another) {
      return info.equals(another.info)
          && addedDataFilesCount == another.addedDataFilesCount
          && rewrittenDataFilesCount == another.rewrittenDataFilesCount
          && rewrittenBytesCount == another.rewrittenBytesCount;
    }

    /**
     * Computes a hash code from attributes: {@code info}, {@code addedDataFilesCount}, {@code rewrittenDataFilesCount}, {@code rewrittenBytesCount}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + info.hashCode();
      h += (h << 5) + addedDataFilesCount;
      h += (h << 5) + rewrittenDataFilesCount;
      h += (h << 5) + Long.hashCode(rewrittenBytesCount);
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
          + ", addedDataFilesCount=" + addedDataFilesCount
          + ", rewrittenDataFilesCount=" + rewrittenDataFilesCount
          + ", rewrittenBytesCount=" + rewrittenBytesCount
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewriteDataFiles.FileGroupRewriteResult} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable FileGroupRewriteResult instance
     */
    public static ImmutableRewriteDataFiles.FileGroupRewriteResult copyOf(RewriteDataFiles.FileGroupRewriteResult instance) {
      if (instance instanceof ImmutableRewriteDataFiles.FileGroupRewriteResult) {
        return (ImmutableRewriteDataFiles.FileGroupRewriteResult) instance;
      }
      return ImmutableRewriteDataFiles.FileGroupRewriteResult.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewriteDataFiles.FileGroupRewriteResult FileGroupRewriteResult}.
     * <pre>
     * ImmutableRewriteDataFiles.FileGroupRewriteResult.builder()
     *    .info(org.apache.iceberg.actions.RewriteDataFiles.FileGroupInfo) // required {@link RewriteDataFiles.FileGroupRewriteResult#info() info}
     *    .addedDataFilesCount(int) // required {@link RewriteDataFiles.FileGroupRewriteResult#addedDataFilesCount() addedDataFilesCount}
     *    .rewrittenDataFilesCount(int) // required {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenDataFilesCount() rewrittenDataFilesCount}
     *    .rewrittenBytesCount(long) // optional {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount}
     *    .build();
     * </pre>
     * @return A new FileGroupRewriteResult builder
     */
    public static ImmutableRewriteDataFiles.FileGroupRewriteResult.Builder builder() {
      return new ImmutableRewriteDataFiles.FileGroupRewriteResult.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewriteDataFiles.FileGroupRewriteResult FileGroupRewriteResult}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewriteDataFiles.FileGroupRewriteResult", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_INFO = 0x1L;
      private static final long INIT_BIT_ADDED_DATA_FILES_COUNT = 0x2L;
      private static final long INIT_BIT_REWRITTEN_DATA_FILES_COUNT = 0x4L;
      private static final long OPT_BIT_REWRITTEN_BYTES_COUNT = 0x1L;
      private long initBits = 0x7L;
      private long optBits;

      private @Nullable RewriteDataFiles.FileGroupInfo info;
      private int addedDataFilesCount;
      private int rewrittenDataFilesCount;
      private long rewrittenBytesCount;

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
      public final Builder from(RewriteDataFiles.FileGroupRewriteResult instance) {
        Objects.requireNonNull(instance, "instance");
        info(instance.info());
        addedDataFilesCount(instance.addedDataFilesCount());
        rewrittenDataFilesCount(instance.rewrittenDataFilesCount());
        rewrittenBytesCount(instance.rewrittenBytesCount());
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.FileGroupRewriteResult#info() info} attribute.
       * @param info The value for info 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder info(RewriteDataFiles.FileGroupInfo info) {
        this.info = Objects.requireNonNull(info, "info");
        initBits &= ~INIT_BIT_INFO;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.FileGroupRewriteResult#addedDataFilesCount() addedDataFilesCount} attribute.
       * @param addedDataFilesCount The value for addedDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addedDataFilesCount(int addedDataFilesCount) {
        this.addedDataFilesCount = addedDataFilesCount;
        initBits &= ~INIT_BIT_ADDED_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenDataFilesCount() rewrittenDataFilesCount} attribute.
       * @param rewrittenDataFilesCount The value for rewrittenDataFilesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenDataFilesCount(int rewrittenDataFilesCount) {
        this.rewrittenDataFilesCount = rewrittenDataFilesCount;
        initBits &= ~INIT_BIT_REWRITTEN_DATA_FILES_COUNT;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount} attribute.
       * <p><em>If not set, this attribute will have a default value as returned by the initializer of {@link RewriteDataFiles.FileGroupRewriteResult#rewrittenBytesCount() rewrittenBytesCount}.</em>
       * @param rewrittenBytesCount The value for rewrittenBytesCount 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenBytesCount(long rewrittenBytesCount) {
        this.rewrittenBytesCount = rewrittenBytesCount;
        optBits |= OPT_BIT_REWRITTEN_BYTES_COUNT;
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewriteDataFiles.FileGroupRewriteResult FileGroupRewriteResult}.
       * @return An immutable instance of FileGroupRewriteResult
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewriteDataFiles.FileGroupRewriteResult build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableRewriteDataFiles.FileGroupRewriteResult(this);
      }

      private boolean rewrittenBytesCountIsSet() {
        return (optBits & OPT_BIT_REWRITTEN_BYTES_COUNT) != 0;
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_INFO) != 0) attributes.add("info");
        if ((initBits & INIT_BIT_ADDED_DATA_FILES_COUNT) != 0) attributes.add("addedDataFilesCount");
        if ((initBits & INIT_BIT_REWRITTEN_DATA_FILES_COUNT) != 0) attributes.add("rewrittenDataFilesCount");
        return "Cannot build FileGroupRewriteResult, some of required attributes are not set " + attributes;
      }
    }
  }

  /**
   * Immutable implementation of {@link RewriteDataFiles.FileGroupInfo}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewriteDataFiles.FileGroupInfo.builder()}.
   */
  @Generated(from = "RewriteDataFiles.FileGroupInfo", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class FileGroupInfo implements RewriteDataFiles.FileGroupInfo {
    private final int globalIndex;
    private final int partitionIndex;
    private final StructLike partition;

    private FileGroupInfo(int globalIndex, int partitionIndex, StructLike partition) {
      this.globalIndex = globalIndex;
      this.partitionIndex = partitionIndex;
      this.partition = partition;
    }

    /**
     *returns which file group this is out of the total set of file groups for this rewrite 
     */
    @Override
    public int globalIndex() {
      return globalIndex;
    }

    /**
     *returns which file group this is out of the set of file groups for this partition 
     */
    @Override
    public int partitionIndex() {
      return partitionIndex;
    }

    /**
     *returns which partition this file group contains files from 
     */
    @Override
    public StructLike partition() {
      return partition;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupInfo#globalIndex() globalIndex} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for globalIndex
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupInfo withGlobalIndex(int value) {
      if (this.globalIndex == value) return this;
      return new ImmutableRewriteDataFiles.FileGroupInfo(value, this.partitionIndex, this.partition);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupInfo#partitionIndex() partitionIndex} attribute.
     * A value equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for partitionIndex
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupInfo withPartitionIndex(int value) {
      if (this.partitionIndex == value) return this;
      return new ImmutableRewriteDataFiles.FileGroupInfo(this.globalIndex, value, this.partition);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteDataFiles.FileGroupInfo#partition() partition} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for partition
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteDataFiles.FileGroupInfo withPartition(StructLike value) {
      if (this.partition == value) return this;
      StructLike newValue = Objects.requireNonNull(value, "partition");
      return new ImmutableRewriteDataFiles.FileGroupInfo(this.globalIndex, this.partitionIndex, newValue);
    }

    /**
     * This instance is equal to all instances of {@code FileGroupInfo} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewriteDataFiles.FileGroupInfo
          && equalTo(0, (ImmutableRewriteDataFiles.FileGroupInfo) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewriteDataFiles.FileGroupInfo another) {
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
     * Creates an immutable copy of a {@link RewriteDataFiles.FileGroupInfo} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable FileGroupInfo instance
     */
    public static ImmutableRewriteDataFiles.FileGroupInfo copyOf(RewriteDataFiles.FileGroupInfo instance) {
      if (instance instanceof ImmutableRewriteDataFiles.FileGroupInfo) {
        return (ImmutableRewriteDataFiles.FileGroupInfo) instance;
      }
      return ImmutableRewriteDataFiles.FileGroupInfo.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewriteDataFiles.FileGroupInfo FileGroupInfo}.
     * <pre>
     * ImmutableRewriteDataFiles.FileGroupInfo.builder()
     *    .globalIndex(int) // required {@link RewriteDataFiles.FileGroupInfo#globalIndex() globalIndex}
     *    .partitionIndex(int) // required {@link RewriteDataFiles.FileGroupInfo#partitionIndex() partitionIndex}
     *    .partition(org.apache.iceberg.StructLike) // required {@link RewriteDataFiles.FileGroupInfo#partition() partition}
     *    .build();
     * </pre>
     * @return A new FileGroupInfo builder
     */
    public static ImmutableRewriteDataFiles.FileGroupInfo.Builder builder() {
      return new ImmutableRewriteDataFiles.FileGroupInfo.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewriteDataFiles.FileGroupInfo FileGroupInfo}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewriteDataFiles.FileGroupInfo", generator = "Immutables")
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
      public final Builder from(RewriteDataFiles.FileGroupInfo instance) {
        Objects.requireNonNull(instance, "instance");
        globalIndex(instance.globalIndex());
        partitionIndex(instance.partitionIndex());
        partition(instance.partition());
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteDataFiles.FileGroupInfo#globalIndex() globalIndex} attribute.
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
       * Initializes the value for the {@link RewriteDataFiles.FileGroupInfo#partitionIndex() partitionIndex} attribute.
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
       * Initializes the value for the {@link RewriteDataFiles.FileGroupInfo#partition() partition} attribute.
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
       * Builds a new {@link ImmutableRewriteDataFiles.FileGroupInfo FileGroupInfo}.
       * @return An immutable instance of FileGroupInfo
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewriteDataFiles.FileGroupInfo build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableRewriteDataFiles.FileGroupInfo(globalIndex, partitionIndex, partition);
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

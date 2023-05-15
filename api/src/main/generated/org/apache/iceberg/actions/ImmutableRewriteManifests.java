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
import org.apache.iceberg.ManifestFile;
import org.immutables.value.Generated;

/**
 * {@code ImmutableRewriteManifests} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link RewriteManifests}.
 * @see ImmutableRewriteManifests.Result
 */
@Generated(from = "RewriteManifests", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableRewriteManifests {
  private ImmutableRewriteManifests() {}

  /**
   * Immutable implementation of {@link RewriteManifests.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableRewriteManifests.Result.builder()}.
   */
  @Generated(from = "RewriteManifests.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements RewriteManifests.Result {
    private final Iterable<ManifestFile> rewrittenManifests;
    private final Iterable<ManifestFile> addedManifests;

    private Result(
        Iterable<ManifestFile> rewrittenManifests,
        Iterable<ManifestFile> addedManifests) {
      this.rewrittenManifests = rewrittenManifests;
      this.addedManifests = addedManifests;
    }

    /**
     *Returns rewritten manifests. 
     */
    @Override
    public Iterable<ManifestFile> rewrittenManifests() {
      return rewrittenManifests;
    }

    /**
     *Returns added manifests. 
     */
    @Override
    public Iterable<ManifestFile> addedManifests() {
      return addedManifests;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteManifests.Result#rewrittenManifests() rewrittenManifests} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for rewrittenManifests
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteManifests.Result withRewrittenManifests(Iterable<ManifestFile> value) {
      if (this.rewrittenManifests == value) return this;
      Iterable<ManifestFile> newValue = Objects.requireNonNull(value, "rewrittenManifests");
      return new ImmutableRewriteManifests.Result(newValue, this.addedManifests);
    }

    /**
     * Copy the current immutable object by setting a value for the {@link RewriteManifests.Result#addedManifests() addedManifests} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for addedManifests
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableRewriteManifests.Result withAddedManifests(Iterable<ManifestFile> value) {
      if (this.addedManifests == value) return this;
      Iterable<ManifestFile> newValue = Objects.requireNonNull(value, "addedManifests");
      return new ImmutableRewriteManifests.Result(this.rewrittenManifests, newValue);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableRewriteManifests.Result
          && equalTo(0, (ImmutableRewriteManifests.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableRewriteManifests.Result another) {
      return rewrittenManifests.equals(another.rewrittenManifests)
          && addedManifests.equals(another.addedManifests);
    }

    /**
     * Computes a hash code from attributes: {@code rewrittenManifests}, {@code addedManifests}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + rewrittenManifests.hashCode();
      h += (h << 5) + addedManifests.hashCode();
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "rewrittenManifests=" + rewrittenManifests
          + ", addedManifests=" + addedManifests
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link RewriteManifests.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableRewriteManifests.Result copyOf(RewriteManifests.Result instance) {
      if (instance instanceof ImmutableRewriteManifests.Result) {
        return (ImmutableRewriteManifests.Result) instance;
      }
      return ImmutableRewriteManifests.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableRewriteManifests.Result Result}.
     * <pre>
     * ImmutableRewriteManifests.Result.builder()
     *    .rewrittenManifests(Iterable&amp;lt;org.apache.iceberg.ManifestFile&amp;gt;) // required {@link RewriteManifests.Result#rewrittenManifests() rewrittenManifests}
     *    .addedManifests(Iterable&amp;lt;org.apache.iceberg.ManifestFile&amp;gt;) // required {@link RewriteManifests.Result#addedManifests() addedManifests}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableRewriteManifests.Result.Builder builder() {
      return new ImmutableRewriteManifests.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableRewriteManifests.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "RewriteManifests.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_REWRITTEN_MANIFESTS = 0x1L;
      private static final long INIT_BIT_ADDED_MANIFESTS = 0x2L;
      private long initBits = 0x3L;

      private @Nullable Iterable<ManifestFile> rewrittenManifests;
      private @Nullable Iterable<ManifestFile> addedManifests;

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
      public final Builder from(RewriteManifests.Result instance) {
        Objects.requireNonNull(instance, "instance");
        rewrittenManifests(instance.rewrittenManifests());
        addedManifests(instance.addedManifests());
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteManifests.Result#rewrittenManifests() rewrittenManifests} attribute.
       * @param rewrittenManifests The value for rewrittenManifests 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder rewrittenManifests(Iterable<ManifestFile> rewrittenManifests) {
        this.rewrittenManifests = Objects.requireNonNull(rewrittenManifests, "rewrittenManifests");
        initBits &= ~INIT_BIT_REWRITTEN_MANIFESTS;
        return this;
      }

      /**
       * Initializes the value for the {@link RewriteManifests.Result#addedManifests() addedManifests} attribute.
       * @param addedManifests The value for addedManifests 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder addedManifests(Iterable<ManifestFile> addedManifests) {
        this.addedManifests = Objects.requireNonNull(addedManifests, "addedManifests");
        initBits &= ~INIT_BIT_ADDED_MANIFESTS;
        return this;
      }

      /**
       * Builds a new {@link ImmutableRewriteManifests.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableRewriteManifests.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableRewriteManifests.Result(rewrittenManifests, addedManifests);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_REWRITTEN_MANIFESTS) != 0) attributes.add("rewrittenManifests");
        if ((initBits & INIT_BIT_ADDED_MANIFESTS) != 0) attributes.add("addedManifests");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

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
 * {@code ImmutableDeleteOrphanFiles} contains immutable implementation classes generated from
 * abstract value types defined as nested inside {@link DeleteOrphanFiles}.
 * @see ImmutableDeleteOrphanFiles.Result
 */
@Generated(from = "DeleteOrphanFiles", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
public final class ImmutableDeleteOrphanFiles {
  private ImmutableDeleteOrphanFiles() {}

  /**
   * Immutable implementation of {@link DeleteOrphanFiles.Result}.
   * <p>
   * Use the builder to create immutable instances:
   * {@code ImmutableDeleteOrphanFiles.Result.builder()}.
   */
  @Generated(from = "DeleteOrphanFiles.Result", generator = "Immutables")
  @Immutable
  @CheckReturnValue
  public static final class Result implements DeleteOrphanFiles.Result {
    private final Iterable<String> orphanFileLocations;

    private Result(Iterable<String> orphanFileLocations) {
      this.orphanFileLocations = orphanFileLocations;
    }

    /**
     *Returns locations of orphan files. 
     */
    @Override
    public Iterable<String> orphanFileLocations() {
      return orphanFileLocations;
    }

    /**
     * Copy the current immutable object by setting a value for the {@link DeleteOrphanFiles.Result#orphanFileLocations() orphanFileLocations} attribute.
     * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
     * @param value A new value for orphanFileLocations
     * @return A modified copy of the {@code this} object
     */
    public final ImmutableDeleteOrphanFiles.Result withOrphanFileLocations(Iterable<String> value) {
      if (this.orphanFileLocations == value) return this;
      Iterable<String> newValue = Objects.requireNonNull(value, "orphanFileLocations");
      return new ImmutableDeleteOrphanFiles.Result(newValue);
    }

    /**
     * This instance is equal to all instances of {@code Result} that have equal attribute values.
     * @return {@code true} if {@code this} is equal to {@code another} instance
     */
    @Override
    public boolean equals(@Nullable Object another) {
      if (this == another) return true;
      return another instanceof ImmutableDeleteOrphanFiles.Result
          && equalTo(0, (ImmutableDeleteOrphanFiles.Result) another);
    }

    private boolean equalTo(int synthetic, ImmutableDeleteOrphanFiles.Result another) {
      return orphanFileLocations.equals(another.orphanFileLocations);
    }

    /**
     * Computes a hash code from attributes: {@code orphanFileLocations}.
     * @return hashCode value
     */
    @Override
    public int hashCode() {
      @Var int h = 5381;
      h += (h << 5) + orphanFileLocations.hashCode();
      return h;
    }

    /**
     * Prints the immutable value {@code Result} with attribute values.
     * @return A string representation of the value
     */
    @Override
    public String toString() {
      return "Result{"
          + "orphanFileLocations=" + orphanFileLocations
          + "}";
    }

    /**
     * Creates an immutable copy of a {@link DeleteOrphanFiles.Result} value.
     * Uses accessors to get values to initialize the new immutable instance.
     * If an instance is already immutable, it is returned as is.
     * @param instance The instance to copy
     * @return A copied immutable Result instance
     */
    public static ImmutableDeleteOrphanFiles.Result copyOf(DeleteOrphanFiles.Result instance) {
      if (instance instanceof ImmutableDeleteOrphanFiles.Result) {
        return (ImmutableDeleteOrphanFiles.Result) instance;
      }
      return ImmutableDeleteOrphanFiles.Result.builder()
          .from(instance)
          .build();
    }

    /**
     * Creates a builder for {@link ImmutableDeleteOrphanFiles.Result Result}.
     * <pre>
     * ImmutableDeleteOrphanFiles.Result.builder()
     *    .orphanFileLocations(Iterable&amp;lt;String&amp;gt;) // required {@link DeleteOrphanFiles.Result#orphanFileLocations() orphanFileLocations}
     *    .build();
     * </pre>
     * @return A new Result builder
     */
    public static ImmutableDeleteOrphanFiles.Result.Builder builder() {
      return new ImmutableDeleteOrphanFiles.Result.Builder();
    }

    /**
     * Builds instances of type {@link ImmutableDeleteOrphanFiles.Result Result}.
     * Initialize attributes and then invoke the {@link #build()} method to create an
     * immutable instance.
     * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
     * but instead used immediately to create instances.</em>
     */
    @Generated(from = "DeleteOrphanFiles.Result", generator = "Immutables")
    @NotThreadSafe
    public static final class Builder {
      private static final long INIT_BIT_ORPHAN_FILE_LOCATIONS = 0x1L;
      private long initBits = 0x1L;

      private @Nullable Iterable<String> orphanFileLocations;

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
      public final Builder from(DeleteOrphanFiles.Result instance) {
        Objects.requireNonNull(instance, "instance");
        orphanFileLocations(instance.orphanFileLocations());
        return this;
      }

      /**
       * Initializes the value for the {@link DeleteOrphanFiles.Result#orphanFileLocations() orphanFileLocations} attribute.
       * @param orphanFileLocations The value for orphanFileLocations 
       * @return {@code this} builder for use in a chained invocation
       */
      @CanIgnoreReturnValue 
      public final Builder orphanFileLocations(Iterable<String> orphanFileLocations) {
        this.orphanFileLocations = Objects.requireNonNull(orphanFileLocations, "orphanFileLocations");
        initBits &= ~INIT_BIT_ORPHAN_FILE_LOCATIONS;
        return this;
      }

      /**
       * Builds a new {@link ImmutableDeleteOrphanFiles.Result Result}.
       * @return An immutable instance of Result
       * @throws java.lang.IllegalStateException if any required attributes are missing
       */
      public ImmutableDeleteOrphanFiles.Result build() {
        if (initBits != 0) {
          throw new IllegalStateException(formatRequiredAttributesMessage());
        }
        return new ImmutableDeleteOrphanFiles.Result(orphanFileLocations);
      }

      private String formatRequiredAttributesMessage() {
        List<String> attributes = new ArrayList<>();
        if ((initBits & INIT_BIT_ORPHAN_FILE_LOCATIONS) != 0) attributes.add("orphanFileLocations");
        return "Cannot build Result, some of required attributes are not set " + attributes;
      }
    }
  }
}

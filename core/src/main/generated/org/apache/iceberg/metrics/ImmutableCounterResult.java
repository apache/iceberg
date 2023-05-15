package org.apache.iceberg.metrics;

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
 * Immutable implementation of {@link CounterResult}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCounterResult.builder()}.
 */
@Generated(from = "CounterResult", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableCounterResult implements CounterResult {
  private final MetricsContext.Unit unit;
  private final long value;

  private ImmutableCounterResult(MetricsContext.Unit unit, long value) {
    this.unit = unit;
    this.value = value;
  }

  /**
   * @return The value of the {@code unit} attribute
   */
  @Override
  public MetricsContext.Unit unit() {
    return unit;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  public long value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CounterResult#unit() unit} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for unit
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCounterResult withUnit(MetricsContext.Unit value) {
    MetricsContext.Unit newValue = Objects.requireNonNull(value, "unit");
    if (this.unit == newValue) return this;
    return new ImmutableCounterResult(newValue, this.value);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CounterResult#value() value} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCounterResult withValue(long value) {
    if (this.value == value) return this;
    return new ImmutableCounterResult(this.unit, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCounterResult} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCounterResult
        && equalTo(0, (ImmutableCounterResult) another);
  }

  private boolean equalTo(int synthetic, ImmutableCounterResult another) {
    return unit.equals(another.unit)
        && value == another.value;
  }

  /**
   * Computes a hash code from attributes: {@code unit}, {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + unit.hashCode();
    h += (h << 5) + Long.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code CounterResult} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CounterResult{"
        + "unit=" + unit
        + ", value=" + value
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link CounterResult} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CounterResult instance
   */
  public static ImmutableCounterResult copyOf(CounterResult instance) {
    if (instance instanceof ImmutableCounterResult) {
      return (ImmutableCounterResult) instance;
    }
    return ImmutableCounterResult.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCounterResult ImmutableCounterResult}.
   * <pre>
   * ImmutableCounterResult.builder()
   *    .unit(org.apache.iceberg.metrics.MetricsContext.Unit) // required {@link CounterResult#unit() unit}
   *    .value(long) // required {@link CounterResult#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableCounterResult builder
   */
  public static ImmutableCounterResult.Builder builder() {
    return new ImmutableCounterResult.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCounterResult ImmutableCounterResult}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CounterResult", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_UNIT = 0x1L;
    private static final long INIT_BIT_VALUE = 0x2L;
    private long initBits = 0x3L;

    private @Nullable MetricsContext.Unit unit;
    private long value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CounterResult} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CounterResult instance) {
      Objects.requireNonNull(instance, "instance");
      unit(instance.unit());
      value(instance.value());
      return this;
    }

    /**
     * Initializes the value for the {@link CounterResult#unit() unit} attribute.
     * @param unit The value for unit 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder unit(MetricsContext.Unit unit) {
      this.unit = Objects.requireNonNull(unit, "unit");
      initBits &= ~INIT_BIT_UNIT;
      return this;
    }

    /**
     * Initializes the value for the {@link CounterResult#value() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder value(long value) {
      this.value = value;
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCounterResult ImmutableCounterResult}.
     * @return An immutable instance of CounterResult
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCounterResult build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableCounterResult(unit, value);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_UNIT) != 0) attributes.add("unit");
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build CounterResult, some of required attributes are not set " + attributes;
    }
  }
}

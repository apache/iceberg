package org.apache.iceberg.metrics;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link TimerResult}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableTimerResult.builder()}.
 */
@Generated(from = "TimerResult", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableTimerResult implements TimerResult {
  private final TimeUnit timeUnit;
  private final Duration totalDuration;
  private final long count;

  private ImmutableTimerResult(TimeUnit timeUnit, Duration totalDuration, long count) {
    this.timeUnit = timeUnit;
    this.totalDuration = totalDuration;
    this.count = count;
  }

  /**
   * @return The value of the {@code timeUnit} attribute
   */
  @Override
  public TimeUnit timeUnit() {
    return timeUnit;
  }

  /**
   * @return The value of the {@code totalDuration} attribute
   */
  @Override
  public Duration totalDuration() {
    return totalDuration;
  }

  /**
   * @return The value of the {@code count} attribute
   */
  @Override
  public long count() {
    return count;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimerResult#timeUnit() timeUnit} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timeUnit
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimerResult withTimeUnit(TimeUnit value) {
    TimeUnit newValue = Objects.requireNonNull(value, "timeUnit");
    if (this.timeUnit == newValue) return this;
    return new ImmutableTimerResult(newValue, this.totalDuration, this.count);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimerResult#totalDuration() totalDuration} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for totalDuration
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimerResult withTotalDuration(Duration value) {
    if (this.totalDuration == value) return this;
    Duration newValue = Objects.requireNonNull(value, "totalDuration");
    return new ImmutableTimerResult(this.timeUnit, newValue, this.count);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link TimerResult#count() count} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for count
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableTimerResult withCount(long value) {
    if (this.count == value) return this;
    return new ImmutableTimerResult(this.timeUnit, this.totalDuration, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableTimerResult} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableTimerResult
        && equalTo(0, (ImmutableTimerResult) another);
  }

  private boolean equalTo(int synthetic, ImmutableTimerResult another) {
    return timeUnit.equals(another.timeUnit)
        && totalDuration.equals(another.totalDuration)
        && count == another.count;
  }

  /**
   * Computes a hash code from attributes: {@code timeUnit}, {@code totalDuration}, {@code count}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + timeUnit.hashCode();
    h += (h << 5) + totalDuration.hashCode();
    h += (h << 5) + Long.hashCode(count);
    return h;
  }

  /**
   * Prints the immutable value {@code TimerResult} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "TimerResult{"
        + "timeUnit=" + timeUnit
        + ", totalDuration=" + totalDuration
        + ", count=" + count
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link TimerResult} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable TimerResult instance
   */
  public static ImmutableTimerResult copyOf(TimerResult instance) {
    if (instance instanceof ImmutableTimerResult) {
      return (ImmutableTimerResult) instance;
    }
    return ImmutableTimerResult.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableTimerResult ImmutableTimerResult}.
   * <pre>
   * ImmutableTimerResult.builder()
   *    .timeUnit(concurrent.TimeUnit) // required {@link TimerResult#timeUnit() timeUnit}
   *    .totalDuration(java.time.Duration) // required {@link TimerResult#totalDuration() totalDuration}
   *    .count(long) // required {@link TimerResult#count() count}
   *    .build();
   * </pre>
   * @return A new ImmutableTimerResult builder
   */
  public static ImmutableTimerResult.Builder builder() {
    return new ImmutableTimerResult.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableTimerResult ImmutableTimerResult}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "TimerResult", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TIME_UNIT = 0x1L;
    private static final long INIT_BIT_TOTAL_DURATION = 0x2L;
    private static final long INIT_BIT_COUNT = 0x4L;
    private long initBits = 0x7L;

    private @Nullable TimeUnit timeUnit;
    private @Nullable Duration totalDuration;
    private long count;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code TimerResult} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(TimerResult instance) {
      Objects.requireNonNull(instance, "instance");
      timeUnit(instance.timeUnit());
      totalDuration(instance.totalDuration());
      count(instance.count());
      return this;
    }

    /**
     * Initializes the value for the {@link TimerResult#timeUnit() timeUnit} attribute.
     * @param timeUnit The value for timeUnit 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder timeUnit(TimeUnit timeUnit) {
      this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit");
      initBits &= ~INIT_BIT_TIME_UNIT;
      return this;
    }

    /**
     * Initializes the value for the {@link TimerResult#totalDuration() totalDuration} attribute.
     * @param totalDuration The value for totalDuration 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder totalDuration(Duration totalDuration) {
      this.totalDuration = Objects.requireNonNull(totalDuration, "totalDuration");
      initBits &= ~INIT_BIT_TOTAL_DURATION;
      return this;
    }

    /**
     * Initializes the value for the {@link TimerResult#count() count} attribute.
     * @param count The value for count 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder count(long count) {
      this.count = count;
      initBits &= ~INIT_BIT_COUNT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableTimerResult ImmutableTimerResult}.
     * @return An immutable instance of TimerResult
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableTimerResult build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableTimerResult(timeUnit, totalDuration, count);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TIME_UNIT) != 0) attributes.add("timeUnit");
      if ((initBits & INIT_BIT_TOTAL_DURATION) != 0) attributes.add("totalDuration");
      if ((initBits & INIT_BIT_COUNT) != 0) attributes.add("count");
      return "Cannot build TimerResult, some of required attributes are not set " + attributes;
    }
  }
}

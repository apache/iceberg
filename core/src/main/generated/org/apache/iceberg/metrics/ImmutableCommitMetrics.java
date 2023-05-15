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
 * Immutable implementation of {@link CommitMetrics}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableCommitMetrics.builder()}.
 */
@Generated(from = "CommitMetrics", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableCommitMetrics extends CommitMetrics {
  private final MetricsContext metricsContext;
  private transient final Timer totalDuration;
  private transient final Counter attempts;

  private ImmutableCommitMetrics(MetricsContext metricsContext) {
    this.metricsContext = metricsContext;
    this.totalDuration = initShim.totalDuration();
    this.attempts = initShim.attempts();
    this.initShim = null;
  }

  private static final byte STAGE_INITIALIZING = -1;
  private static final byte STAGE_UNINITIALIZED = 0;
  private static final byte STAGE_INITIALIZED = 1;
  @SuppressWarnings("Immutable")
  private transient volatile InitShim initShim = new InitShim();

  @Generated(from = "CommitMetrics", generator = "Immutables")
  private final class InitShim {
    private byte totalDurationBuildStage = STAGE_UNINITIALIZED;
    private Timer totalDuration;

    Timer totalDuration() {
      if (totalDurationBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (totalDurationBuildStage == STAGE_UNINITIALIZED) {
        totalDurationBuildStage = STAGE_INITIALIZING;
        this.totalDuration = Objects.requireNonNull(ImmutableCommitMetrics.super.totalDuration(), "totalDuration");
        totalDurationBuildStage = STAGE_INITIALIZED;
      }
      return this.totalDuration;
    }

    private byte attemptsBuildStage = STAGE_UNINITIALIZED;
    private Counter attempts;

    Counter attempts() {
      if (attemptsBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
      if (attemptsBuildStage == STAGE_UNINITIALIZED) {
        attemptsBuildStage = STAGE_INITIALIZING;
        this.attempts = Objects.requireNonNull(ImmutableCommitMetrics.super.attempts(), "attempts");
        attemptsBuildStage = STAGE_INITIALIZED;
      }
      return this.attempts;
    }

    private String formatInitCycleMessage() {
      List<String> attributes = new ArrayList<>();
      if (totalDurationBuildStage == STAGE_INITIALIZING) attributes.add("totalDuration");
      if (attemptsBuildStage == STAGE_INITIALIZING) attributes.add("attempts");
      return "Cannot build CommitMetrics, attribute initializers form cycle " + attributes;
    }
  }

  /**
   * @return The value of the {@code metricsContext} attribute
   */
  @Override
  public MetricsContext metricsContext() {
    return metricsContext;
  }

  /**
   * @return The computed-at-construction value of the {@code totalDuration} attribute
   */
  @Override
  public Timer totalDuration() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.totalDuration()
        : this.totalDuration;
  }

  /**
   * @return The computed-at-construction value of the {@code attempts} attribute
   */
  @Override
  public Counter attempts() {
    InitShim shim = this.initShim;
    return shim != null
        ? shim.attempts()
        : this.attempts;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CommitMetrics#metricsContext() metricsContext} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for metricsContext
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableCommitMetrics withMetricsContext(MetricsContext value) {
    if (this.metricsContext == value) return this;
    MetricsContext newValue = Objects.requireNonNull(value, "metricsContext");
    return new ImmutableCommitMetrics(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableCommitMetrics} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableCommitMetrics
        && equalTo(0, (ImmutableCommitMetrics) another);
  }

  private boolean equalTo(int synthetic, ImmutableCommitMetrics another) {
    return metricsContext.equals(another.metricsContext)
        && totalDuration.equals(another.totalDuration)
        && attempts.equals(another.attempts);
  }

  /**
   * Computes a hash code from attributes: {@code metricsContext}, {@code totalDuration}, {@code attempts}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + metricsContext.hashCode();
    h += (h << 5) + totalDuration.hashCode();
    h += (h << 5) + attempts.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code CommitMetrics} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "CommitMetrics{"
        + "metricsContext=" + metricsContext
        + ", totalDuration=" + totalDuration
        + ", attempts=" + attempts
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link CommitMetrics} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable CommitMetrics instance
   */
  public static ImmutableCommitMetrics copyOf(CommitMetrics instance) {
    if (instance instanceof ImmutableCommitMetrics) {
      return (ImmutableCommitMetrics) instance;
    }
    return ImmutableCommitMetrics.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableCommitMetrics ImmutableCommitMetrics}.
   * <pre>
   * ImmutableCommitMetrics.builder()
   *    .metricsContext(org.apache.iceberg.metrics.MetricsContext) // required {@link CommitMetrics#metricsContext() metricsContext}
   *    .build();
   * </pre>
   * @return A new ImmutableCommitMetrics builder
   */
  public static ImmutableCommitMetrics.Builder builder() {
    return new ImmutableCommitMetrics.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableCommitMetrics ImmutableCommitMetrics}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CommitMetrics", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_METRICS_CONTEXT = 0x1L;
    private long initBits = 0x1L;

    private @Nullable MetricsContext metricsContext;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code CommitMetrics} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CommitMetrics instance) {
      Objects.requireNonNull(instance, "instance");
      metricsContext(instance.metricsContext());
      return this;
    }

    /**
     * Initializes the value for the {@link CommitMetrics#metricsContext() metricsContext} attribute.
     * @param metricsContext The value for metricsContext 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder metricsContext(MetricsContext metricsContext) {
      this.metricsContext = Objects.requireNonNull(metricsContext, "metricsContext");
      initBits &= ~INIT_BIT_METRICS_CONTEXT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableCommitMetrics ImmutableCommitMetrics}.
     * @return An immutable instance of CommitMetrics
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableCommitMetrics build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableCommitMetrics(metricsContext);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_METRICS_CONTEXT) != 0) attributes.add("metricsContext");
      return "Cannot build CommitMetrics, some of required attributes are not set " + attributes;
    }
  }
}

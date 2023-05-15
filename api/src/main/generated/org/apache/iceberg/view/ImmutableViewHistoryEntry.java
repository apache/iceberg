package org.apache.iceberg.view;

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
 * Immutable implementation of {@link ViewHistoryEntry}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableViewHistoryEntry.builder()}.
 */
@Generated(from = "ViewHistoryEntry", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableViewHistoryEntry implements ViewHistoryEntry {
  private final long timestampMillis;
  private final int versionId;

  private ImmutableViewHistoryEntry(long timestampMillis, int versionId) {
    this.timestampMillis = timestampMillis;
    this.versionId = versionId;
  }

  /**
   *Return the timestamp in milliseconds of the change 
   */
  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  /**
   *Return ID of the new current version 
   */
  @Override
  public int versionId() {
    return versionId;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ViewHistoryEntry#timestampMillis() timestampMillis} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for timestampMillis
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableViewHistoryEntry withTimestampMillis(long value) {
    if (this.timestampMillis == value) return this;
    return new ImmutableViewHistoryEntry(value, this.versionId);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link ViewHistoryEntry#versionId() versionId} attribute.
   * A value equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for versionId
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableViewHistoryEntry withVersionId(int value) {
    if (this.versionId == value) return this;
    return new ImmutableViewHistoryEntry(this.timestampMillis, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableViewHistoryEntry} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableViewHistoryEntry
        && equalTo(0, (ImmutableViewHistoryEntry) another);
  }

  private boolean equalTo(int synthetic, ImmutableViewHistoryEntry another) {
    return timestampMillis == another.timestampMillis
        && versionId == another.versionId;
  }

  /**
   * Computes a hash code from attributes: {@code timestampMillis}, {@code versionId}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + Long.hashCode(timestampMillis);
    h += (h << 5) + versionId;
    return h;
  }

  /**
   * Prints the immutable value {@code ViewHistoryEntry} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "ViewHistoryEntry{"
        + "timestampMillis=" + timestampMillis
        + ", versionId=" + versionId
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link ViewHistoryEntry} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ViewHistoryEntry instance
   */
  public static ImmutableViewHistoryEntry copyOf(ViewHistoryEntry instance) {
    if (instance instanceof ImmutableViewHistoryEntry) {
      return (ImmutableViewHistoryEntry) instance;
    }
    return ImmutableViewHistoryEntry.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableViewHistoryEntry ImmutableViewHistoryEntry}.
   * <pre>
   * ImmutableViewHistoryEntry.builder()
   *    .timestampMillis(long) // required {@link ViewHistoryEntry#timestampMillis() timestampMillis}
   *    .versionId(int) // required {@link ViewHistoryEntry#versionId() versionId}
   *    .build();
   * </pre>
   * @return A new ImmutableViewHistoryEntry builder
   */
  public static ImmutableViewHistoryEntry.Builder builder() {
    return new ImmutableViewHistoryEntry.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableViewHistoryEntry ImmutableViewHistoryEntry}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "ViewHistoryEntry", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TIMESTAMP_MILLIS = 0x1L;
    private static final long INIT_BIT_VERSION_ID = 0x2L;
    private long initBits = 0x3L;

    private long timestampMillis;
    private int versionId;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ViewHistoryEntry} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ViewHistoryEntry instance) {
      Objects.requireNonNull(instance, "instance");
      timestampMillis(instance.timestampMillis());
      versionId(instance.versionId());
      return this;
    }

    /**
     * Initializes the value for the {@link ViewHistoryEntry#timestampMillis() timestampMillis} attribute.
     * @param timestampMillis The value for timestampMillis 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder timestampMillis(long timestampMillis) {
      this.timestampMillis = timestampMillis;
      initBits &= ~INIT_BIT_TIMESTAMP_MILLIS;
      return this;
    }

    /**
     * Initializes the value for the {@link ViewHistoryEntry#versionId() versionId} attribute.
     * @param versionId The value for versionId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder versionId(int versionId) {
      this.versionId = versionId;
      initBits &= ~INIT_BIT_VERSION_ID;
      return this;
    }

    /**
     * Builds a new {@link ImmutableViewHistoryEntry ImmutableViewHistoryEntry}.
     * @return An immutable instance of ViewHistoryEntry
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableViewHistoryEntry build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableViewHistoryEntry(timestampMillis, versionId);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TIMESTAMP_MILLIS) != 0) attributes.add("timestampMillis");
      if ((initBits & INIT_BIT_VERSION_ID) != 0) attributes.add("versionId");
      return "Cannot build ViewHistoryEntry, some of required attributes are not set " + attributes;
    }
  }
}

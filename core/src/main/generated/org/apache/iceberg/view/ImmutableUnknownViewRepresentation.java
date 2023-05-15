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
 * Immutable implementation of {@link UnknownViewRepresentation}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableUnknownViewRepresentation.builder()}.
 */
@Generated(from = "UnknownViewRepresentation", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class ImmutableUnknownViewRepresentation
    implements UnknownViewRepresentation {
  private final String type;

  private ImmutableUnknownViewRepresentation(String type) {
    this.type = type;
  }

  /**
   * @return The value of the {@code type} attribute
   */
  @Override
  public String type() {
    return type;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link UnknownViewRepresentation#type() type} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for type
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableUnknownViewRepresentation withType(String value) {
    String newValue = Objects.requireNonNull(value, "type");
    if (this.type.equals(newValue)) return this;
    return new ImmutableUnknownViewRepresentation(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableUnknownViewRepresentation} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableUnknownViewRepresentation
        && equalTo(0, (ImmutableUnknownViewRepresentation) another);
  }

  private boolean equalTo(int synthetic, ImmutableUnknownViewRepresentation another) {
    return type.equals(another.type);
  }

  /**
   * Computes a hash code from attributes: {@code type}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + type.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code UnknownViewRepresentation} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "UnknownViewRepresentation{"
        + "type=" + type
        + "}";
  }

  /**
   * Creates an immutable copy of a {@link UnknownViewRepresentation} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable UnknownViewRepresentation instance
   */
  public static ImmutableUnknownViewRepresentation copyOf(UnknownViewRepresentation instance) {
    if (instance instanceof ImmutableUnknownViewRepresentation) {
      return (ImmutableUnknownViewRepresentation) instance;
    }
    return ImmutableUnknownViewRepresentation.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableUnknownViewRepresentation ImmutableUnknownViewRepresentation}.
   * <pre>
   * ImmutableUnknownViewRepresentation.builder()
   *    .type(String) // required {@link UnknownViewRepresentation#type() type}
   *    .build();
   * </pre>
   * @return A new ImmutableUnknownViewRepresentation builder
   */
  public static ImmutableUnknownViewRepresentation.Builder builder() {
    return new ImmutableUnknownViewRepresentation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableUnknownViewRepresentation ImmutableUnknownViewRepresentation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "UnknownViewRepresentation", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TYPE = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String type;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code org.apache.iceberg.view.ViewRepresentation} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(ViewRepresentation instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code org.apache.iceberg.view.UnknownViewRepresentation} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(UnknownViewRepresentation instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      @Var long bits = 0;
      if (object instanceof ViewRepresentation) {
        ViewRepresentation instance = (ViewRepresentation) object;
        if ((bits & 0x1L) == 0) {
          type(instance.type());
          bits |= 0x1L;
        }
      }
      if (object instanceof UnknownViewRepresentation) {
        UnknownViewRepresentation instance = (UnknownViewRepresentation) object;
        if ((bits & 0x1L) == 0) {
          type(instance.type());
          bits |= 0x1L;
        }
      }
    }

    /**
     * Initializes the value for the {@link UnknownViewRepresentation#type() type} attribute.
     * @param type The value for type 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder type(String type) {
      this.type = Objects.requireNonNull(type, "type");
      initBits &= ~INIT_BIT_TYPE;
      return this;
    }

    /**
     * Builds a new {@link ImmutableUnknownViewRepresentation ImmutableUnknownViewRepresentation}.
     * @return An immutable instance of UnknownViewRepresentation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableUnknownViewRepresentation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableUnknownViewRepresentation(type);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TYPE) != 0) attributes.add("type");
      return "Cannot build UnknownViewRepresentation, some of required attributes are not set " + attributes;
    }
  }
}

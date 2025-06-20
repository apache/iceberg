/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.io.ObjectStreamException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.iceberg.util.SerializableMap;

/**
 * This was copied from the generated Immutable implementation of {@link StorageCredential} with the
 * one difference that the underlying Map is not unmodifiable but rather a {@link SerializableMap}
 * so that Ser/De properly works with Kryo.
 *
 * <p>Use the builder to create immutable instances: {@code ImmutableStorageCredential.builder()}.
 */
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@Immutable
@CheckReturnValue
public final class ImmutableStorageCredential implements StorageCredential {
  private final String prefix;
  private final Map<String, String> config;

  private ImmutableStorageCredential(String prefix, Map<String, String> config) {
    this.prefix = prefix;
    this.config = config;
  }

  /**
   * @return The value of the {@code prefix} attribute
   */
  @Override
  public String prefix() {
    return prefix;
  }

  /**
   * @return The value of the {@code config} attribute
   */
  @Override
  public Map<String, String> config() {
    return config;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link StorageCredential#prefix()
   * prefix} attribute. An equals check used to prevent copying of the same value by returning
   * {@code this}.
   *
   * @param value A new value for prefix
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableStorageCredential withPrefix(String value) {
    String newValue = Objects.requireNonNull(value, "prefix");
    if (this.prefix.equals(newValue)) return this;
    return validate(new ImmutableStorageCredential(newValue, this.config));
  }

  /**
   * Copy the current immutable object by replacing the {@link StorageCredential#config() config}
   * map with the specified map. Nulls are not permitted as keys or values. A shallow reference
   * equality check is used to prevent copying of the same value by returning {@code this}.
   *
   * @param entries The entries to be added to the config map
   * @return A modified copy of {@code this} object
   */
  public final ImmutableStorageCredential withConfig(Map<String, ? extends String> entries) {
    if (this.config == entries) return this;
    Map<String, String> newValue = createSerializableMap(true, false, entries);
    return validate(new ImmutableStorageCredential(this.prefix, newValue));
  }

  /**
   * This instance is equal to all instances of {@code ImmutableStorageCredential} that have equal
   * attribute values.
   *
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableStorageCredential
        && equalTo(0, (ImmutableStorageCredential) another);
  }

  private boolean equalTo(int synthetic, ImmutableStorageCredential another) {
    return prefix.equals(another.prefix) && config.equals(another.config);
  }

  /**
   * Computes a hash code from attributes: {@code prefix}, {@code config}.
   *
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + prefix.hashCode();
    h += (h << 5) + config.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code StorageCredential} with attribute values.
   *
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "StorageCredential{" + "prefix=" + prefix + ", config=" + config + "}";
  }

  private static ImmutableStorageCredential validate(ImmutableStorageCredential instance) {
    instance.validate();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link StorageCredential} value. Uses accessors to get values to
   * initialize the new immutable instance. If an instance is already immutable, it is returned as
   * is.
   *
   * @param instance The instance to copy
   * @return A copied immutable StorageCredential instance
   */
  public static ImmutableStorageCredential copyOf(StorageCredential instance) {
    if (instance instanceof ImmutableStorageCredential) {
      return (ImmutableStorageCredential) instance;
    }
    return ImmutableStorageCredential.builder().from(instance).build();
  }

  private Object readResolve() throws ObjectStreamException {
    return validate(this);
  }

  /**
   * Creates a builder for {@link ImmutableStorageCredential ImmutableStorageCredential}.
   *
   * <pre>
   * ImmutableStorageCredential.builder()
   *    .prefix(String) // required {@link StorageCredential#prefix() prefix}
   *    .putConfig|putAllConfig(String =&gt; String) // {@link StorageCredential#config() config} mappings
   *    .build();
   * </pre>
   *
   * @return A new ImmutableStorageCredential builder
   */
  public static ImmutableStorageCredential.Builder builder() {
    return new ImmutableStorageCredential.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableStorageCredential ImmutableStorageCredential}.
   * Initialize attributes and then invoke the {@link #build()} method to create an immutable
   * instance.
   *
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or
   * collection, but instead used immediately to create instances.</em>
   */
  @NotThreadSafe
  public static final class Builder {
    private String prefix;
    private Map<String, String> config = new LinkedHashMap<String, String>();

    private Builder() {}

    /**
     * Fill a builder with attribute values from the provided {@code StorageCredential} instance.
     * Regular attribute values will be replaced with those from the given instance. Absent optional
     * values will not replace present values. Collection elements and entries will be added, not
     * replaced.
     *
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder from(StorageCredential instance) {
      Objects.requireNonNull(instance, "instance");
      this.prefix(instance.prefix());
      putAllConfig(instance.config());
      return this;
    }

    /**
     * Initializes the value for the {@link StorageCredential#prefix() prefix} attribute.
     *
     * @param prefix The value for prefix
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder prefix(String prefix) {
      this.prefix = Objects.requireNonNull(prefix, "prefix");
      return this;
    }

    /**
     * Put one entry to the {@link StorageCredential#config() config} map.
     *
     * @param key The key in the config map
     * @param value The associated value in the config map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder putConfig(String key, String value) {
      this.config.put(
          Objects.requireNonNull(key, "config key"),
          Objects.requireNonNull(value, value == null ? "config value for key: " + key : null));
      return this;
    }

    /**
     * Put one entry to the {@link StorageCredential#config() config} map. Nulls are not permitted
     *
     * @param entry The key and value entry
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder putConfig(Map.Entry<String, ? extends String> entry) {
      String k = entry.getKey();
      String v = entry.getValue();
      this.config.put(
          Objects.requireNonNull(k, "config key"),
          Objects.requireNonNull(v, v == null ? "config value for key: " + k : null));
      return this;
    }

    /**
     * Sets or replaces all mappings from the specified map as entries for the {@link
     * StorageCredential#config() config} map. Nulls are not permitted
     *
     * @param entries The entries that will be added to the config map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder config(Map<String, ? extends String> entries) {
      this.config.clear();
      return putAllConfig(entries);
    }

    /**
     * Put all mappings from the specified map as entries to {@link StorageCredential#config()
     * config} map. Nulls are not permitted
     *
     * @param entries The entries that will be added to the config map
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public final Builder putAllConfig(Map<String, ? extends String> entries) {
      for (Map.Entry<String, ? extends String> e : entries.entrySet()) {
        String k = e.getKey();
        String v = e.getValue();
        this.config.put(
            Objects.requireNonNull(k, "config key"),
            Objects.requireNonNull(v, v == null ? "config value for key: " + k : null));
      }
      return this;
    }

    /**
     * Builds a new {@link ImmutableStorageCredential ImmutableStorageCredential}.
     *
     * @return An immutable instance of StorageCredential
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableStorageCredential build() {
      if (null == prefix) {
        throw new IllegalStateException(
            "Cannot build StorageCredential, some of required attributes are not set: prefix");
      }

      return ImmutableStorageCredential.validate(
          new ImmutableStorageCredential(prefix, createSerializableMap(false, false, config)));
    }
  }

  private static <K, V> Map<K, V> createSerializableMap(
      boolean checkNulls, boolean skipNulls, Map<? extends K, ? extends V> map) {
    Map<K, V> linkedMap = new LinkedHashMap<>();
    if (skipNulls || checkNulls) {
      for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
        K k = e.getKey();
        V v = e.getValue();
        if (skipNulls) {
          if (k == null || v == null) continue;
        } else if (checkNulls) {
          Objects.requireNonNull(k, "key");
          Objects.requireNonNull(v, v == null ? "value for key: " + k : null);
        }
        linkedMap.put(k, v);
      }
    } else {
      linkedMap.putAll(map);
    }
    return SerializableMap.copyOf(linkedMap);
  }
}

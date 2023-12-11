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
package org.apache.iceberg.view;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** This is a copy of the Immutables generated class to preserve backwards compatibility */
public final class ImmutableSQLViewRepresentation implements SQLViewRepresentation {
  private final String sql;
  private final String dialect;

  private ImmutableSQLViewRepresentation(String sql, String dialect) {
    this.sql = sql;
    this.dialect = dialect;
  }

  /** The view query SQL text. */
  @Override
  public String sql() {
    return sql;
  }

  /** The view query SQL dialect. */
  @Override
  public String dialect() {
    return dialect;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link SQLViewRepresentation#sql()
   * sql} attribute. An equals check used to prevent copying of the same value by returning {@code
   * this}.
   *
   * @param value A new value for sql
   * @return A modified copy of the {@code this} object
   */
  public ImmutableSQLViewRepresentation withSql(String value) {
    String newValue = Preconditions.checkNotNull(value, "sql");
    if (sql.equals(newValue)) {
      return this;
    }

    return new ImmutableSQLViewRepresentation(newValue, this.dialect);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link
   * SQLViewRepresentation#dialect() dialect} attribute. An equals check used to prevent copying of
   * the same value by returning {@code this}.
   *
   * @param value A new value for dialect
   * @return A modified copy of the {@code this} object
   */
  public ImmutableSQLViewRepresentation withDialect(String value) {
    String newValue = Preconditions.checkNotNull(value, "dialect");
    if (dialect.equals(newValue)) {
      return this;
    }

    return new ImmutableSQLViewRepresentation(sql, newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableSQLViewRepresentation} that have
   * equal attribute values.
   *
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) {
      return true;
    }

    return another instanceof ImmutableSQLViewRepresentation
        && equalTo(0, (ImmutableSQLViewRepresentation) another);
  }

  private boolean equalTo(int synthetic, ImmutableSQLViewRepresentation another) {
    return sql.equals(another.sql) && dialect.equals(another.dialect);
  }

  /**
   * Computes a hash code from attributes: {@code sql}, {@code dialect}.
   *
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    int hashCode = 5381;
    hashCode += (hashCode << 5) + sql.hashCode();
    hashCode += (hashCode << 5) + dialect.hashCode();
    return hashCode;
  }

  /**
   * Prints the immutable value {@code SQLViewRepresentation} with attribute values.
   *
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return "SQLViewRepresentation{" + "sql=" + sql + ", dialect=" + dialect + "}";
  }

  /**
   * Creates an immutable copy of a {@link SQLViewRepresentation} value. Uses accessors to get
   * values to initialize the new immutable instance. If an instance is already immutable, it is
   * returned as is.
   *
   * @param instance The instance to copy
   * @return A copied immutable SQLViewRepresentation instance
   */
  public static ImmutableSQLViewRepresentation copyOf(SQLViewRepresentation instance) {
    if (instance instanceof ImmutableSQLViewRepresentation) {
      return (ImmutableSQLViewRepresentation) instance;
    }
    return ImmutableSQLViewRepresentation.builder().from(instance).build();
  }

  /**
   * Creates a builder for {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
   *
   * <pre>
   * ImmutableSQLViewRepresentation.builder()
   *    .sql(String) // required {@link SQLViewRepresentation#sql() sql}
   *    .dialect(String) // required {@link SQLViewRepresentation#dialect() dialect}
   *    .build();
   * </pre>
   *
   * @return A new ImmutableSQLViewRepresentation builder
   */
  public static ImmutableSQLViewRepresentation.Builder builder() {
    return new ImmutableSQLViewRepresentation.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
   * Initialize attributes and then invoke the {@link #build()} method to create an immutable
   * instance.
   *
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or
   * collection, but instead used immediately to create instances.</em>
   */
  public static final class Builder {
    private static final long INIT_BIT_SQL = 0x1L;
    private static final long INIT_BIT_DIALECT = 0x2L;
    private long initBits = 0x3L;

    private @Nullable String sql;
    private @Nullable String dialect;

    private Builder() {}

    /**
     * Fill a builder with attribute values from the provided {@code SQLViewRepresentation}
     * instance. Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     *
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    public Builder from(SQLViewRepresentation instance) {
      Preconditions.checkNotNull(instance, "instance");
      sql(instance.sql());
      dialect(instance.dialect());
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#sql() sql} attribute.
     *
     * @param newSql The value for sql
     * @return {@code this} builder for use in a chained invocation
     */
    public Builder sql(String newSql) {
      this.sql = Preconditions.checkNotNull(newSql, "sql");
      initBits &= ~INIT_BIT_SQL;
      return this;
    }

    /**
     * Initializes the value for the {@link SQLViewRepresentation#dialect() dialect} attribute.
     *
     * @param newDialect The value for dialect
     * @return {@code this} builder for use in a chained invocation
     */
    public Builder dialect(String newDialect) {
      this.dialect = Preconditions.checkNotNull(newDialect, "dialect");
      initBits &= ~INIT_BIT_DIALECT;
      return this;
    }

    /**
     * Builds a new {@link ImmutableSQLViewRepresentation ImmutableSQLViewRepresentation}.
     *
     * @return An immutable instance of SQLViewRepresentation
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableSQLViewRepresentation build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }

      return new ImmutableSQLViewRepresentation(sql, dialect);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = Lists.newArrayList();
      if ((initBits & INIT_BIT_SQL) != 0) {
        attributes.add("sql");
      }

      if ((initBits & INIT_BIT_DIALECT) != 0) {
        attributes.add("dialect");
      }

      return "Cannot build SQLViewRepresentation, some of required attributes are not set "
          + attributes;
    }
  }
}

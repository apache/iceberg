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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * SQL definition for a view
 */
class BaseViewDefinition implements ViewDefinition {
  private final String sql;
  private final String dialect;
  private final Schema schema;
  private final String defaultCatalog;
  private final List<String> defaultNamespace;
  private final List<String> fieldAliases;
  private final List<String> fieldComments;

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(ViewDefinition that) {
    return builder()
        .sql(that.sql())
        .dialect(that.dialect())
        .schema(that.schema())
        .defaultCatalog(that.defaultCatalog())
        .defaultNamespace(that.defaultNamespace())
        .fieldAliases(that.fieldAliases())
        .fieldComments(that.fieldComments());
  }

  private BaseViewDefinition(
      String sql, String dialect, Schema schema, String defaultCatalog, List<String> defaultNamespace,
      List<String> fieldAliases, List<String> fieldComments) {
    this.sql = Preconditions.checkNotNull(sql, "sql should not be null");
    this.dialect = Preconditions.checkNotNull(dialect, "dialect should not be null");
    this.schema = schema;
    this.defaultCatalog = Preconditions.checkNotNull(defaultCatalog, "default catalog should not null");
    this.defaultNamespace = Preconditions.checkNotNull(defaultNamespace, "default namespace should not be null");
    this.fieldAliases = Preconditions.checkNotNull(fieldAliases, "field aliases should not be null");
    this.fieldComments = Preconditions.checkNotNull(fieldComments, "field comments should not be null");
  }

  @Override
  public String sql() {
    return sql;
  }

  @Override
  public String dialect() {
    return dialect;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public String defaultCatalog() {
    return defaultCatalog;
  }

  @Override
  public List<String> defaultNamespace() {
    return defaultNamespace;
  }

  @Override
  public List<String> fieldAliases() {
    return fieldAliases;
  }

  @Override
  public List<String> fieldComments() {
    return fieldComments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseViewDefinition that = (BaseViewDefinition) o;
    return Objects.equals(sql, that.sql) &&
        Objects.equals(dialect, that.dialect) &&
        Objects.equals(schema.asStruct(), that.schema.asStruct()) &&
        Objects.equals(defaultCatalog, that.defaultCatalog) &&
        Objects.equals(defaultNamespace, that.defaultNamespace) &&
        Objects.equals(fieldAliases, that.fieldAliases) &&
        Objects.equals(fieldComments, that.fieldComments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, dialect, schema.asStruct(), defaultCatalog, defaultNamespace, fieldAliases, fieldComments);
  }

  @Override
  public String toString() {
    return "BaseViewDefinition{" +
        "sql='" + sql + '\'' +
        ", dialect=" + dialect +
        ", schema=" + schema +
        ", defaultCatalog='" + defaultCatalog + '\'' +
        ", defaultNamespace=" + defaultNamespace +
        ", fieldAliases=" + fieldAliases +
        ", fieldComments=" + fieldComments +
        '}';
  }

  public static final class Builder {

    private String sql;
    private String dialect = "";
    private Schema schema = new Schema();
    private String defaultCatalog = "";
    private List<String> defaultNamespace = Collections.emptyList();
    private List<String> fieldAliases = Collections.emptyList();
    private List<String> fieldComments = Collections.emptyList();

    private Builder() {
    }

    public Builder sql(String value) {
      sql = value;
      return this;
    }

    public Builder dialect(String value) {
      dialect = value;
      return this;
    }

    public Builder schema(Schema value) {
      schema = value;
      return this;
    }

    public Builder defaultCatalog(String value) {
      defaultCatalog = value;
      return this;
    }

    public Builder defaultNamespace(List<String> value) {
      defaultNamespace = value;
      return this;
    }

    public Builder fieldAliases(List<String> value) {
      fieldAliases = value;
      return this;
    }

    public Builder fieldComments(List<String> value) {
      fieldComments = value;
      return this;
    }

    public BaseViewDefinition build() {
      return new BaseViewDefinition(
          sql,
          dialect,
          schema,
          defaultCatalog,
          defaultNamespace,
          fieldAliases,
          fieldComments);
    }
  }
}

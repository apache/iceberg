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
package org.apache.iceberg.spark.udf;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Minimal SQL function spec used by analyzer rewrite and tests. */
public class SqlFunctionSpec {
  public static final class Parameter {
    private final String name;
    private final String icebergTypeJson;
    private final String defaultExpr; // optional, may be null
    private final String doc; // optional, may be null

    public Parameter(String name, String icebergTypeJson, String defaultExpr) {
      this(name, icebergTypeJson, defaultExpr, null);
    }

    public Parameter(String name, String icebergTypeJson, String defaultExpr, String doc) {
      this.name = name;
      this.icebergTypeJson = icebergTypeJson;
      this.defaultExpr = defaultExpr;
      this.doc = doc;
    }

    public String name() {
      return name;
    }

    public String icebergTypeJson() {
      return icebergTypeJson;
    }

    public String defaultExpr() {
      return defaultExpr;
    }

    public String doc() {
      return doc;
    }
  }

  private final List<Parameter> parameters;
  private final String returnTypeJson;
  private final String dialect; // e.g., "spark"
  private final String body; // SQL expression text for scalar; query text for UDTF
  private final boolean deterministic;
  private final String doc; // optional, may be null

  public SqlFunctionSpec(
      List<Parameter> parameters,
      String returnTypeJson,
      String dialect,
      String body,
      boolean deterministic) {
    this(parameters, returnTypeJson, dialect, body, deterministic, null);
  }

  public SqlFunctionSpec(
      List<Parameter> parameters,
      String returnTypeJson,
      String dialect,
      String body,
      boolean deterministic,
      String doc) {
    this.parameters = parameters == null ? Collections.emptyList() : parameters;
    this.returnTypeJson = returnTypeJson;
    this.dialect = dialect;
    this.body = body;
    this.deterministic = deterministic;
    this.doc = doc;
  }

  public List<Parameter> parameters() {
    return parameters;
  }

  public String returnTypeJson() {
    return returnTypeJson;
  }

  public String dialect() {
    return dialect;
  }

  public String body() {
    return body;
  }

  public boolean deterministic() {
    return deterministic;
  }

  public String doc() {
    return doc;
  }

  @Override
  public String toString() {
    return "SqlFunctionSpec{"
        + "parameters="
        + parameters.size()
        + ", returnTypeJson='"
        + returnTypeJson
        + '\''
        + ", dialect='"
        + dialect
        + '\''
        + ", body='"
        + (body == null ? "" : body)
        + '\''
        + ", deterministic="
        + deterministic
        + ", doc='"
        + (doc == null ? "" : doc)
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqlFunctionSpec that = (SqlFunctionSpec) o;
    return deterministic == that.deterministic
        && Objects.equals(parameters, that.parameters)
        && Objects.equals(returnTypeJson, that.returnTypeJson)
        && Objects.equals(dialect, that.dialect)
        && Objects.equals(body, that.body)
        && Objects.equals(doc, that.doc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parameters, returnTypeJson, dialect, body, deterministic, doc);
  }
}

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

package org.apache.iceberg.expressions;

import java.io.Serializable;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

/**
 * Evaluates an {@link Expression} on a {@link Schema} to test whether the file with the schema may match.
 */
public class SchemaEvaluator implements Serializable {
  private final Expression expr;

  public SchemaEvaluator(Types.StructType struct, Expression unbound) {
    this(struct, unbound, true);
  }

  public SchemaEvaluator(Types.StructType struct, Expression unbound, boolean caseSensitive) {
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
  }

  public boolean eval(Schema schema) {
    return new EvalVisitor().eval(schema);
  }

  private class EvalVisitor extends ExpressionVisitors.BoundVisitor<Boolean> {
    private Schema schema;

    private boolean eval(Schema evalSchema) {
      this.schema = evalSchema;
      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return true;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(Bound<T> valueExpr) {
      // column exists, it could be null
      // column does not exist, it is null
      return true;
    }

    @Override
    public <T> Boolean notNull(Bound<T> valueExpr) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean isNaN(Bound<T> valueExpr) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean notNaN(Bound<T> valueExpr) {
      // column exists, it could be NaN
      // column does not exist, column value is null. Null could not be NaN.
      return true;
    }

    @Override
    public <T> Boolean lt(Bound<T> valueExpr, Literal<T> lit) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean ltEq(Bound<T> valueExpr, Literal<T> lit) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean gt(Bound<T> valueExpr, Literal<T> lit) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean gtEq(Bound<T> valueExpr, Literal<T> lit) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean eq(Bound<T> valueExpr, Literal<T> lit) {
      // lit could not be null, so it should be return true when column exists
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean notEq(Bound<T> valueExpr, Literal<T> lit) {
      // column exists, it could be not equal to lit
      // column does not exist, column value is null. Null not equal to lit because lit could not be null
      return true;
    }

    @Override
    public <T> Boolean in(Bound<T> valueExpr, Set<T> literalSet) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean notIn(Bound<T> valueExpr, Set<T> literalSet) {
      // column exists, it could be not in literalSet
      // column does not exist, column values it null. Null could not be in literalSet
      return true;
    }

    @Override
    public <T> Boolean startsWith(Bound<T> valueExpr, Literal<T> lit) {
      return columnExists(valueExpr);
    }

    @Override
    public <T> Boolean notStartsWith(Bound<T> valueExpr, Literal<T> lit) {
      // column exists, it could be not start with lit.
      // column does not exist, it is not start with lit.
      return true;
    }

    private <T> boolean columnExists(Bound<T> valueExpr) {
      return schema.findField(valueExpr.ref().fieldId()) != null;
    }
  }
}

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

import com.google.common.base.Preconditions;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.expressions.Expression.Operation.IS_NULL;
import static org.apache.iceberg.expressions.Expression.Operation.NOT_NULL;

public class UnboundPredicate<T> extends Predicate<NamedReference> {
  private final Literal<T> literal;
  private final LiteralSet<T> literalSet;
  private final Set<Literal<T>> literals;

  UnboundPredicate(Operation op, NamedReference namedRef, T value) {
    this(op, namedRef, Literals.from(value));
  }

  UnboundPredicate(Operation op, NamedReference namedRef) {
    super(op, namedRef);
    Preconditions.checkArgument(op == Operation.IS_NULL || op == Operation.NOT_NULL,
        "Cannot create %s predicate without a value", op);
    this.literal = null;
    this.literalSet = null;
    this.literals = null;
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Literal<T> lit) {
    super(op, namedRef);
    Preconditions.checkArgument(op != Operation.IN && op != Operation.NOT_IN,
        "%s predicate does not support a single literal", op);
    this.literal = lit;
    this.literalSet = null;
    this.literals = null;
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Set<Literal<T>> lits) {
    this(op, namedRef, lits, new LiteralSet<>(lits));
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Set<Literal<T>> lits, LiteralSet<T> literalSet) {
    super(op, namedRef);
    Preconditions.checkArgument(op == Operation.IN || op == Operation.NOT_IN,
        "%s predicate does not support a literal set", op);
    this.literal = null;
    this.literalSet = literalSet;
    this.literals = lits;
  }

  @Override
  public Expression negate() {
    switch (op()) {
      case IN:
      case NOT_IN:
        return new UnboundPredicate<>(op().negate(), ref(), literals, literalSet);
    }
    return new UnboundPredicate<>(op().negate(), ref(), literal());
  }

  public Literal<T> literal() {
    Preconditions.checkArgument(op() != Operation.IN && op() != Operation.NOT_IN,
        "%s predicate cannot return a literal", op());
    return literal;
  }

  public Set<Literal<T>> literals() {
    Preconditions.checkArgument(op() == Operation.IN || op() == Operation.NOT_IN,
        "%s predicate cannot return a set of literals", op());
    return literals;
  }

  public LiteralSet<T> literalSet() {
    Preconditions.checkArgument(op() == Operation.IN || op() == Operation.NOT_IN,
        "%s predicate cannot return a literal set", op());
    return literalSet;
  }

  /**
   * Bind this UnboundPredicate, defaulting to case sensitive mode.
   *
   * Access modifier is package-private, to only allow use from existing tests.
   *
   * @param struct The {@link Types.StructType struct type} to resolve references by name.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  Expression bind(Types.StructType struct) {
    return bind(struct, true);
  }

  /**
   * Bind this UnboundPredicate.
   *
   * @param struct The {@link Types.StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    Schema schema = new Schema(struct.fields());
    Types.NestedField field = caseSensitive ?
        schema.findField(ref().name()) :
        schema.caseInsensitiveFindField(ref().name());

    ValidationException.check(field != null,
        "Cannot find field '%s' in struct: %s", ref().name(), schema.asStruct());

    switch (op()) {
      case IN:
        return bindInOperation(field, schema);
      case NOT_IN:
        return bindInOperation(field, schema).negate();
    }

    if (literal() == null) {
      switch (op()) {
        case IS_NULL:
          if (field.isRequired()) {
            return Expressions.alwaysFalse();
          }
          return new BoundPredicate<>(IS_NULL, new BoundReference<>(field.fieldId(),
              schema.accessorForField(field.fieldId())));
        case NOT_NULL:
          if (field.isRequired()) {
            return Expressions.alwaysTrue();
          }
          return new BoundPredicate<>(NOT_NULL, new BoundReference<>(field.fieldId(),
              schema.accessorForField(field.fieldId())));
        default:
          throw new ValidationException("Operation must be IS_NULL or NOT_NULL");
      }
    }

    Literal<T> lit = literal().to(field.type());
    if (lit == null) {
      throw new ValidationException(String.format(
          "Invalid value for comparison inclusive type %s: %s (%s)",
          field.type(), literal().value(), literal().value().getClass().getName()));

    } else if (lit == Literals.aboveMax()) {
      switch (op()) {
        case LT:
        case LT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case GT:
        case GT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    } else if (lit == Literals.belowMin()) {
      switch (op()) {
        case GT:
        case GT_EQ:
        case NOT_EQ:
          return Expressions.alwaysTrue();
        case LT:
        case LT_EQ:
        case EQ:
          return Expressions.alwaysFalse();
      }
    }
    return new BoundPredicate<>(op(), new BoundReference<>(field.fieldId(),
            schema.accessorForField(field.fieldId())), lit);
  }

  @SuppressWarnings("unchecked")
  private Expression bindInOperation(Types.NestedField field, Schema schema) {
    final Set<Literal<T>> lits = literals().stream().map(
        l -> {
          Literal<T> lit = l.to(field.type());
          if (lit == null) {
            throw new ValidationException(String.format(
                "Invalid value for comparison inclusive type %s: %s (%s)",
                field.type(), l.value(), l.value().getClass().getName()));
          }
          return lit;
        })
        .filter(l -> l != Literals.aboveMax() && l != Literals.belowMin())
        .collect(Collectors.toSet());

    if (lits.isEmpty()) {
      return Expressions.alwaysFalse();
    } else if (lits.size() == 1) {
      return new BoundPredicate<>(Operation.EQ, new BoundReference<>(field.fieldId(),
          schema.accessorForField(field.fieldId())), lits.iterator().next());
    } else {
      return new BoundSetPredicate<>(Operation.IN, new BoundReference<>(field.fieldId(),
          schema.accessorForField(field.fieldId())), lits);
    }
  }

  @Override
  public String toString() {
    switch (op()) {
      case IS_NULL:
        return "is_null(" + ref() + ")";
      case NOT_NULL:
        return "not_null(" + ref() + ")";
      case LT:
        return String.valueOf(ref()) + " < " + literal();
      case LT_EQ:
        return String.valueOf(ref()) + " <= " + literal();
      case GT:
        return String.valueOf(ref()) + " > " + literal();
      case GT_EQ:
        return String.valueOf(ref()) + " >= " + literal();
      case EQ:
        return String.valueOf(ref()) + " == " + literal();
      case NOT_EQ:
        return String.valueOf(ref()) + " != " + literal();
      case STARTS_WITH:
        return ref() + " startsWith \"" + literal() + "\"";
      case IN:
        return String.valueOf(ref()) + " in " + literalSet();
      case NOT_IN:
        return String.valueOf(ref()) + " not in " + literalSet();
      default:
        return "Invalid predicate: operation = " + op();
    }
  }
}

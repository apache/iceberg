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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.CharSequenceSet;

public class UnboundPredicate<T> extends Predicate<NamedReference> {
  private static final Joiner COMMA = Joiner.on(", ");

  private final List<Literal<T>> literals;

  UnboundPredicate(Operation op, NamedReference namedRef, T value) {
    this(op, namedRef, Literals.from(value));
  }

  UnboundPredicate(Operation op, NamedReference namedRef) {
    super(op, namedRef);
    this.literals = null;
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Literal<T> lit) {
    super(op, namedRef);
    this.literals = Lists.newArrayList(lit);
  }

  UnboundPredicate(Operation op, NamedReference namedRef, Iterable<T> values) {
    super(op, namedRef);
    this.literals = Lists.newArrayList(Iterables.transform(values, Literals::from));
  }

  private UnboundPredicate(Operation op, NamedReference namedRef, List<Literal<T>> literals) {
    super(op, namedRef);
    this.literals = literals;
  }

  @Override
  public Expression negate() {
    return new UnboundPredicate<>(op().negate(), ref(), literals);
  }

  public Literal<T> literal() {
    Preconditions.checkArgument(op() != Operation.IN && op() != Operation.NOT_IN,
        "%s predicate cannot return a literal", op());
    return literals == null ? null : Iterables.getOnlyElement(literals);
  }

  public List<Literal<T>> literals() {
    return literals;
  }

  /**
   * Bind this UnboundPredicate, defaulting to case sensitive mode.
   *
   * Access modifier is package-private, to only allow use from existing tests.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  Expression bind(StructType struct) {
    return bind(struct, true);
  }

  /**
   * Bind this UnboundPredicate.
   *
   * @param struct The {@link StructType struct type} to resolve references by name.
   * @param caseSensitive A boolean flag to control whether the bind should enforce case sensitivity.
   * @return an {@link Expression}
   * @throws ValidationException if literals do not match bound references, or if comparison on expression is invalid
   */
  public Expression bind(StructType struct, boolean caseSensitive) {
    Schema schema = new Schema(struct.fields());
    Types.NestedField field = caseSensitive ?
        schema.findField(ref().name()) :
        schema.caseInsensitiveFindField(ref().name());

    ValidationException.check(field != null,
        "Cannot find field '%s' in struct: %s", ref().name(), schema.asStruct());

    BoundReference<T> ref = new BoundReference<>(field.fieldId(), schema.accessorForField(field.fieldId()));

    if (literals == null) {
      return bindUnaryOperation(ref, field.isRequired());
    }

    if (op() == Operation.IN || op() == Operation.NOT_IN) {
      return bindInOperation(ref, field.type(), op());
    }

    return bindLiteralOperation(ref, field.type());
  }

  private Expression bindUnaryOperation(BoundReference<T> ref, boolean isRequired) {
    switch (op()) {
      case IS_NULL:
        if (isRequired) {
          return Expressions.alwaysFalse();
        }
        return new BoundUnaryPredicate<>(Operation.IS_NULL, ref);
      case NOT_NULL:
        if (isRequired) {
          return Expressions.alwaysTrue();
        }
        return new BoundUnaryPredicate<>(Operation.NOT_NULL, ref);
      default:
        throw new ValidationException("Operation must be IS_NULL or NOT_NULL");
    }
  }

  private Expression bindLiteralOperation(BoundReference<T> ref, Type type) {
    Literal<T> lit = literal().to(type);

    if (lit == null) {
      throw new ValidationException(String.format(
          "Invalid value for conversion to type %s: %s (%s)",
          type, literal().value(), literal().value().getClass().getName()));

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

    return new BoundLiteralPredicate<>(op(), ref, lit);
  }

  private Expression bindInOperation(BoundReference<T> ref, Type type, Operation op) {
    List<Literal<T>> convertedLiterals = Lists.newArrayList(Iterables.filter(
        Lists.transform(literals, lit -> {
          Literal<T> converted = lit.to(type);
          ValidationException.check(converted != null,
              "Invalid value for conversion to type %s: %s (%s)", type, lit, lit.getClass().getName());
          return converted;
        }),
        lit -> lit != Literals.aboveMax() && lit != Literals.belowMin()));

    if (convertedLiterals.isEmpty()) {
      switch (op) {
        case IN:
          return Expressions.alwaysFalse();
        case NOT_IN:
          return Expressions.alwaysTrue();
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    Set<T> literalSet = setOf(convertedLiterals);
    if (literalSet.size() == 1) {
      switch (op) {
        case IN:
          return new BoundLiteralPredicate<>(Operation.EQ, ref, Iterables.get(convertedLiterals, 0));
        case NOT_IN:
          return new BoundLiteralPredicate<>(Operation.NOT_EQ, ref, Iterables.get(convertedLiterals, 0));
        default:
          throw new ValidationException("Operation must be IN or NOT_IN");
      }
    }

    return new BoundSetPredicate<>(op, ref, literalSet);
  }

  @Override
  public String toString() {
    switch (op()) {
      case IS_NULL:
        return "is_null(" + ref() + ")";
      case NOT_NULL:
        return "not_null(" + ref() + ")";
      case LT:
        return ref() + " < " + literal();
      case LT_EQ:
        return ref() + " <= " + literal();
      case GT:
        return ref() + " > " + literal();
      case GT_EQ:
        return ref() + " >= " + literal();
      case EQ:
        return ref() + " == " + literal();
      case NOT_EQ:
        return ref() + " != " + literal();
      case STARTS_WITH:
        return ref() + " startsWith \"" + literal() + "\"";
      case IN:
        return ref() + " in (" + COMMA.join(literals()) + ")";
      case NOT_IN:
        return ref() + " not in (" + COMMA.join(literals()) + ")";
      default:
        return "Invalid predicate: operation = " + op();
    }
  }

  @SuppressWarnings("unchecked")
  static <T> Set<T> setOf(Iterable<Literal<T>> literals) {
    Literal<T> lit = Iterables.get(literals, 0);
    if (lit instanceof Literals.StringLiteral && lit.value() instanceof CharSequence) {
      Iterable<T> values = Iterables.transform(literals, Literal::value);
      Iterable<CharSequence> charSeqs = Iterables.transform(values, val -> (CharSequence) val);
      return (Set<T>) CharSequenceSet.of(charSeqs);
    } else {
      return Sets.newHashSet(Iterables.transform(literals, Literal::value));
    }
  }
}

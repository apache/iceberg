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

import java.util.List;
import java.util.Locale;
import java.util.function.IntFunction;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

class IcebergFunctions implements FunctionBinder {
  static final IcebergFunctions INSTANCE = new IcebergFunctions();

  private static final String CATALOG = "iceberg_functions";

  private IcebergFunctions() {}

  @Override
  public BoundFunctionReference<?> apply(
      FunctionReference function, List<? extends Expression> arguments) {
    if (!CATALOG.equalsIgnoreCase(function.catalog()) || function.identifier().length() != 1) {
      throw new UnsupportedOperationException(
          "Cannot bind function without a function binder: " + function);
    }

    return switch (function.name().toLowerCase(Locale.ROOT)) {
      case "if_else" -> ifElse(function, arguments);
      case "identity" -> transform(function, Transforms.identity(), arguments);
      case "year" -> transform(function, Transforms.year(), arguments);
      case "month" -> transform(function, Transforms.month(), arguments);
      case "day" -> transform(function, Transforms.day(), arguments);
      case "hour" -> transform(function, Transforms.hour(), arguments);
      case "bucket" -> parameterizedTransform(function, Transforms::bucket, arguments);
      case "truncate" -> parameterizedTransform(function, Transforms::truncate, arguments);
      default -> throw new ValidationException("Unknown function: %s", function);
    };
  }

  static boolean isIfElse(BoundFunctionReference<?> function) {
    return CATALOG.equalsIgnoreCase(function.catalog())
        && function.identifier().length() == 1
        && "if_else".equalsIgnoreCase(function.name());
  }

  private static BoundFunctionReference<?> ifElse(
      FunctionReference function, List<? extends Expression> arguments) {
    checkArgumentCount(function, arguments, 3);
    ValidationException.check(
        isPredicate(arguments.get(0)),
        "Invalid condition for %s: expected a predicate, got %s",
        function,
        arguments.get(0));
    Type whenTrue = valueType(function, arguments.get(1));
    Type whenFalse = valueType(function, arguments.get(2));
    ValidationException.check(
        whenTrue.equals(whenFalse),
        "Invalid arguments for %s: result types differ: %s and %s",
        function,
        whenTrue,
        whenFalse);
    return BoundFunctionReference.of(
        function.catalog(), function.identifier(), whenTrue, IcebergFunctions::evalIfElse);
  }

  private static Object evalIfElse(List<Object> args) {
    return Boolean.TRUE.equals(args.get(0)) ? args.get(1) : args.get(2);
  }

  private static boolean isPredicate(Expression expr) {
    return expr instanceof BoundPredicate
        || expr instanceof And
        || expr instanceof Or
        || expr instanceof Not
        || expr instanceof True
        || expr instanceof False;
  }

  private static BoundFunctionReference<?> transform(
      FunctionReference function, Transform<?, ?> transform, List<? extends Expression> arguments) {
    checkArgumentCount(function, arguments, 1);
    return boundTransform(function, transform, arguments, 0);
  }

  private static BoundFunctionReference<?> parameterizedTransform(
      FunctionReference function,
      IntFunction<Transform<?, ?>> transform,
      List<? extends Expression> arguments) {
    checkArgumentCount(function, arguments, 2);
    int parameter = intParameter(function, arguments.get(0));
    return boundTransform(function, transform.apply(parameter), arguments, 1);
  }

  @SuppressWarnings("unchecked")
  private static <S, T> BoundFunctionReference<T> boundTransform(
      FunctionReference function,
      Transform<S, T> transform,
      List<? extends Expression> arguments,
      int valuePos) {
    Type sourceType = valueType(function, arguments.get(valuePos));
    ValidationException.check(
        transform.canTransform(sourceType),
        "Invalid argument for %s: unsupported type %s",
        function,
        sourceType);
    SerializableFunction<S, T> bound = transform.bind(sourceType);
    return BoundFunctionReference.of(
        function.catalog(),
        function.identifier(),
        transform.getResultType(sourceType),
        args -> bound.apply((S) args.get(valuePos)));
  }

  private static int intParameter(FunctionReference function, Expression argument) {
    if (argument instanceof Literal<?> literal) {
      if (literal.value() instanceof Integer value) {
        return value;
      }

      throw new ValidationException(
          "Invalid parameter for %s: expected an int literal, got %s literal %s",
          function, literal.type(), literal);
    }

    throw new ValidationException(
        "Invalid parameter for %s: expected an int literal, got %s", function, argument);
  }

  private static Type valueType(FunctionReference function, Expression argument) {
    if (argument instanceof BoundValueExpression<?> value) {
      return value.type();
    }

    throw new ValidationException(
        "Invalid argument for %s: expected a value, got %s", function, argument);
  }

  private static void checkArgumentCount(
      FunctionReference function, List<? extends Expression> arguments, int expected) {
    ValidationException.check(
        arguments.size() == expected,
        "Invalid arguments for %s: expected %s arguments, got %s",
        function,
        expected,
        arguments.size());
  }
}

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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * A function applied to unbound arguments.
 *
 * @param <T> the Java type of the function's result
 */
public class UnboundApply<T> implements UnboundTerm<T> {
  private final FunctionReference function;
  private final Object[] arguments;
  private final Type resultType;

  UnboundApply(FunctionReference function, List<Object> arguments) {
    this(function, arguments, null);
  }

  UnboundApply(FunctionReference function, List<Object> arguments, Type resultType) {
    Preconditions.checkArgument(function != null, "Invalid function: null");
    this.function = function;
    this.arguments =
        arguments == null
            ? new Object[0]
            : Lists.transform(arguments, UnboundApply::toArgument).toArray();
    this.resultType = resultType;
  }

  private static Object toArgument(Object argument) {
    Preconditions.checkArgument(argument != null, "Invalid function argument: null");
    if (argument instanceof Term || argument instanceof Expression) {
      return argument;
    }

    return Literals.from(argument);
  }

  public FunctionReference function() {
    return function;
  }

  /** Returns the arguments, each a {@link Term}, an {@link Expression}, or a {@link Literal}. */
  List<Object> arguments() {
    return Arrays.asList(arguments);
  }

  @Override
  public NamedReference<?> ref() {
    throw new UnsupportedOperationException("Cannot determine reference for function: " + function);
  }

  @Override
  public BoundTerm<T> bind(Types.StructType struct, boolean caseSensitive) {
    ValidationException.check(
        resultType != null, "Cannot bind function without a result type: %s", function);

    Object[] boundArguments = new Object[arguments.length];
    for (int i = 0; i < arguments.length; i += 1) {
      boundArguments[i] = bindArgument(arguments[i], struct, caseSensitive);
    }

    return new BoundApply<>(function, boundArguments, resultType);
  }

  private static Object bindArgument(
      Object argument, Types.StructType struct, boolean caseSensitive) {
    if (argument instanceof UnboundTerm<?> term) {
      return term.bind(struct, caseSensitive);
    } else if (argument instanceof Expression expr) {
      return Binder.bind(struct, expr, caseSensitive);
    }

    return argument;
  }

  @Override
  public String toString() {
    return function + "(" + Arrays.toString(arguments) + ")";
  }
}

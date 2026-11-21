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
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;

/**
 * Represents the application of a function, where the name of the function name and the unbound or
 * bound arguments are provided.
 *
 * @param <T> return type
 */
public class UnboundApply<T> implements Apply<T>, Unbound<T, BoundApply<T>> {
  private final FunctionReference functionReference;
  private final List<Expression> arguments;

  UnboundApply(FunctionReference functionReference, List<Expression> arguments) {
    Preconditions.checkArgument(functionReference != null, "Invalid function reference: null");
    Preconditions.checkArgument(arguments != null, "Invalid arguments: null");
    this.functionReference = functionReference;
    this.arguments = arguments;
  }

  public FunctionReference function() {
    return functionReference;
  }

  public List<Expression> arguments() {
    return arguments;
  }

  /**
   * Binds this function call, resolving only functions in the reserved {@code iceberg_functions}
   * catalog.
   *
   * @throws UnsupportedOperationException if this calls a function outside {@code
   *     iceberg_functions}; use {@link #bind(StructType, boolean, FunctionBinder)} to resolve other
   *     functions
   * @throws ValidationException if the arguments are not valid for the function
   */
  @Override
  public BoundApply<T> bind(StructType struct, boolean caseSensitive) {
    return bind(struct, caseSensitive, IcebergFunctions.INSTANCE);
  }

  /**
   * Binds this function call, using a function binder to resolve functions.
   *
   * <p>Arguments that are already bound are used as they are.
   *
   * @param struct the struct type used to resolve references
   * @param caseSensitive whether references are resolved by name using case-sensitive matching
   * @param functionBinder a {@link FunctionBinder} used to resolve this and any nested functions
   * @return the bound function call
   */
  public BoundApply<T> bind(
      StructType struct, boolean caseSensitive, FunctionBinder functionBinder) {
    Preconditions.checkNotNull(functionBinder, "Function binder cannot be null");
    List<Expression> boundArguments = Lists.newArrayListWithExpectedSize(arguments.size());
    for (Expression argument : arguments) {
      boundArguments.add(bindArgument(argument, struct, caseSensitive, functionBinder));
    }

    BoundFunctionReference<?> resolved = functionBinder.apply(functionReference, boundArguments);
    ValidationException.check(resolved != null, "Cannot resolve function: %s", functionReference);
    return new BoundApply<>(resolved, boundArguments);
  }

  private static Expression bindArgument(
      Expression argument,
      StructType struct,
      boolean caseSensitive,
      FunctionBinder functionBinder) {
    if (argument instanceof ValueExpression<?> value) {
      return Binder.bind(struct, value, caseSensitive, functionBinder);
    } else if (argument instanceof Aggregate<?>) {
      throw new UnsupportedOperationException("Cannot bind argument: " + argument);
    } else if (Binder.isBound(argument)) {
      return argument;
    } else {
      return Binder.bind(struct, argument, caseSensitive, functionBinder);
    }
  }

  @Override
  public String toString() {
    return functionReference
        + arguments.stream().map(String::valueOf).collect(Collectors.joining(", ", "(", ")"));
  }
}

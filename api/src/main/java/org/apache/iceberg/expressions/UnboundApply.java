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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * Represents a function application expression. This is the general form for invoking functions on
 * value expressions.
 *
 * @param <T> the Java type of values produced by this expression
 */
public class UnboundApply<T> implements UnboundTerm<T> {
  private final FunctionReference function;
  private final List<Object> arguments;

  UnboundApply(FunctionReference function, List<Object> arguments) {
    Preconditions.checkArgument(function != null, "Invalid function: null");
    this.function = function;
    // not an immutable list so that Kryo can deserialize this class
    this.arguments =
        arguments == null
            ? Lists.newArrayList()
            : Lists.newArrayList(Lists.transform(arguments, UnboundApply::toArgument));
  }

  /**
   * Converts an argument to the type used to represent it, or throws if it cannot be an argument.
   *
   * <p>An argument is either a value expression or a predicate. Value expressions are {@link Term}
   * (a reference or a nested apply) or a constant, which is converted to a {@link Literal}.
   * Predicates are {@link Expression}.
   */
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

  /**
   * Returns the arguments passed to the function.
   *
   * <p>Each argument is a {@link Term} (a value expression), an {@link Expression} (a predicate),
   * or a {@link Literal} (a constant value expression). Java has no union type, so the arguments
   * are typed as {@link Object} and validated when this expression is created.
   */
  public List<Object> arguments() {
    return arguments;
  }

  @Override
  public NamedReference<T> ref() {
    // a function may be called with any number of references, so there is no single reference that
    // this term produces values from
    throw new UnsupportedOperationException("Cannot determine reference for function: " + function);
  }

  @Override
  public BoundTerm<T> bind(Types.StructType struct, boolean caseSensitive) {
    // binding requires a function definition to determine the result type, and function definitions
    // are not available in this API
    throw new UnsupportedOperationException("Cannot bind function: " + function);
  }

  @Override
  public String toString() {
    return function + "(" + arguments + ")";
  }
}

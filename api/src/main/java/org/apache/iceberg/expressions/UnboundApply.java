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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;

/**
 * Represents the application of a function, where the name of the function name and the unbound or
 * bound arguments are provided.
 *
 * @param <T> return type
 */
public class UnboundApply<T> implements Apply<T> {
  private final FunctionReference functionReference;
  private final List<Expression> arguments;

  public UnboundApply(FunctionReference functionReference, List<Expression> arguments) {
    this.functionReference = functionReference;
    this.arguments = arguments;
  }

  public FunctionReference function() {
    return functionReference;
  }

  public List<Expression> arguments() {
    return arguments;
  }

  public BoundApply<T> bind(
      FunctionBinder functionBinder, StructType struct, boolean caseSensitive) {
    List<BoundExpression<?>> boundArguments = Lists.newArrayListWithExpectedSize(arguments.size());
    for (Expression argument : arguments) {
      boundArguments.add(bindArgument(functionBinder, argument, struct, caseSensitive));
    }

    return new BoundApply<>(
        functionBinder.apply(functionReference, boundArguments), boundArguments);
  }

  private static BoundExpression<?> bindArgument(
      FunctionBinder functionBinder,
      Expression argument,
      StructType struct,
      boolean caseSensitive) {
    if (argument instanceof Literal<?> literal) {
      return literal;
    } else if (argument instanceof NamedReference<?> reference) {
      return reference.bind(struct, caseSensitive);
    } else if (argument instanceof UnboundApply<?> apply) {
      return apply.bind(functionBinder, struct, caseSensitive);
    } else {
      throw new UnsupportedOperationException("Cannot bind argument: " + argument);
    }
  }
}

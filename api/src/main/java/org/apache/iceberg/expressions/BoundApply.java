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
import java.util.Objects;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;

/**
 * A function applied to bound arguments.
 *
 * @param <T> the Java type of the function's result
 */
public class BoundApply<T> implements BoundTerm<T> {
  private final FunctionReference function;
  private final Object[] arguments;
  private final Type type;

  BoundApply(FunctionReference function, Object[] arguments, Type type) {
    this.function = function;
    this.arguments = arguments;
    this.type = type;
  }

  public FunctionReference function() {
    return function;
  }

  List<Object> arguments() {
    return Arrays.asList(arguments);
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public BoundReference<?> ref() {
    throw new UnsupportedOperationException("Cannot determine reference for function: " + function);
  }

  @Override
  public T eval(StructLike struct) {
    throw new UnsupportedOperationException("Cannot evaluate " + this);
  }

  @Override
  public boolean isEquivalentTo(BoundTerm<?> other) {
    if (!(other instanceof BoundApply<?> that)) {
      return false;
    }

    if (!Objects.equals(function.catalog(), that.function.catalog())
        || !function.identifier().equals(that.function.identifier())
        || !type.equals(that.type)
        || arguments.length != that.arguments.length) {
      return false;
    }

    for (int i = 0; i < arguments.length; i += 1) {
      if (!isEquivalent(arguments[i], that.arguments[i])) {
        return false;
      }
    }

    return true;
  }

  private static boolean isEquivalent(Object left, Object right) {
    if (left instanceof BoundTerm<?> term && right instanceof BoundTerm<?> otherTerm) {
      return term.isEquivalentTo(otherTerm);
    } else if (left instanceof Expression expr && right instanceof Expression otherExpr) {
      return expr.isEquivalentTo(otherExpr);
    }

    return left.equals(right);
  }

  @Override
  public String toString() {
    return function + "(" + Arrays.toString(arguments) + ")";
  }
}

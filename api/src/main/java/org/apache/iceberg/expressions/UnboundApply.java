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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Types;

/**
 * Represents a function application expression. This is the general form for invoking functions on
 * value expressions. Known Iceberg partition transforms can be converted to {@link
 * UnboundTransform} via {@link #asTransform()}.
 *
 * @param <T> the Java type of values produced by this expression
 */
public class UnboundApply<T> implements UnboundTerm<T> {
  // bucket and truncate take the transform parameter as their first argument, so they cannot be
  // resolved from a name alone
  private static final Set<String> PARAMETERIZED_TRANSFORMS = ImmutableSet.of("bucket", "truncate");

  // the expressions spec defines partition transforms as functions, other than void
  private static final String VOID = "void";

  private static final String ICEBERG_FUNCTIONS_CATALOG = "iceberg_functions";

  private final FunctionReference function;
  private final List<Object> arguments;

  UnboundApply(FunctionReference function, List<Object> arguments) {
    Preconditions.checkArgument(function != null, "Function reference cannot be null");
    this.function = function;
    // not an immutable list so that Kryo can deserialize this class
    this.arguments = arguments == null ? Lists.newArrayList() : Lists.newArrayList(arguments);
  }

  public FunctionReference function() {
    return function;
  }

  public List<Object> arguments() {
    return arguments;
  }

  public boolean isKnownTransform() {
    String catalog = function.catalog();
    if (catalog != null && !catalog.equalsIgnoreCase(ICEBERG_FUNCTIONS_CATALOG)) {
      return false;
    }

    return isTransformName(function.name().toLowerCase(Locale.ROOT));
  }

  private static boolean isTransformName(String name) {
    if (VOID.equals(name)) {
      return false;
    } else if (PARAMETERIZED_TRANSFORMS.contains(name)) {
      return true;
    }

    return !(Transforms.fromString(name) instanceof UnknownTransform);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public UnboundTransform asTransform() {
    Preconditions.checkState(isKnownTransform(), "Not a known Iceberg transform: %s", function);

    String transformName = function.name().toLowerCase(Locale.ROOT);
    boolean parameterized = PARAMETERIZED_TRANSFORMS.contains(transformName);
    int expectedArgs = parameterized ? 2 : 1;
    Preconditions.checkArgument(
        arguments.size() == expectedArgs,
        "Cannot convert %s to a transform: expected %s argument(s), got %s",
        function,
        expectedArgs,
        arguments.size());

    String transformString = transformName;
    if (parameterized) {
      Object parameter = arguments.get(0);
      Preconditions.checkArgument(
          parameter instanceof Number,
          "Cannot convert %s to a transform: first argument must be a number, got %s",
          function,
          parameter);
      transformString = transformName + "[" + ((Number) parameter).intValue() + "]";
    }

    Object valueArg = arguments.get(expectedArgs - 1);
    Preconditions.checkArgument(
        valueArg instanceof NamedReference,
        "Cannot convert %s to a transform: last argument must be a reference, got %s",
        function,
        valueArg);

    return new UnboundTransform((NamedReference) valueArg, Transforms.fromString(transformString));
  }

  @Override
  @SuppressWarnings("unchecked")
  public NamedReference<T> ref() {
    for (Object arg : arguments) {
      if (arg instanceof NamedReference) {
        return (NamedReference<T>) arg;
      } else if (arg instanceof UnboundTerm) {
        return (NamedReference<T>) ((UnboundTerm<?>) arg).ref();
      }
    }

    throw new UnsupportedOperationException("Cannot determine reference for function: " + function);
  }

  @Override
  public BoundTerm<T> bind(Types.StructType struct, boolean caseSensitive) {
    if (isKnownTransform()) {
      return asTransform().bind(struct, caseSensitive);
    }

    throw new UnsupportedOperationException(
        "Cannot bind unknown function: "
            + function
            + ". Only known Iceberg transforms are supported.");
  }

  @Override
  public String toString() {
    return function + "(" + arguments + ")";
  }
}

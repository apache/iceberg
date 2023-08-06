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
package org.apache.iceberg.spark.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * An UnboundFunction wrapper that wraps a system function. The bound function by this wrapper will
 * hide the magic method.
 */
public class UnboundFunctionWrapper implements UnboundFunction {
  private final UnboundFunction unboundFunction;

  public UnboundFunctionWrapper(UnboundFunction unboundFunction) {
    this.unboundFunction = unboundFunction;
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    return new SystemFunctionWrapper<>((ScalarFunction<?>) unboundFunction.bind(inputType));
  }

  @Override
  public String description() {
    return unboundFunction.description();
  }

  @Override
  public String name() {
    return unboundFunction.name();
  }

  @Override
  public String toString() {
    return "Wrapper(" + name() + ")";
  }

  public static class SystemFunctionWrapper<T> implements ScalarFunction<T> {
    private final ScalarFunction<T> boundFunction;

    public SystemFunctionWrapper(ScalarFunction<T> boundFunction) {
      this.boundFunction = boundFunction;
    }

    @Override
    public DataType[] inputTypes() {
      return boundFunction.inputTypes();
    }

    @Override
    public DataType resultType() {
      return boundFunction.resultType();
    }

    @Override
    public String name() {
      return boundFunction.name();
    }

    @Override
    public T produceResult(InternalRow input) {
      return boundFunction.produceResult(input);
    }

    @Override
    public boolean isResultNullable() {
      return boundFunction.isResultNullable();
    }

    @Override
    public boolean isDeterministic() {
      return boundFunction.isDeterministic();
    }

    @Override
    public String canonicalName() {
      return boundFunction.canonicalName();
    }

    @Override
    public String toString() {
      return "Wrapper(" + canonicalName() + ")";
    }
  }
}

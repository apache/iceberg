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

import org.apache.iceberg.restrictions.Action;
import org.apache.iceberg.restrictions.Actions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Spark ScalarFunction implementation for the {@code mask-alphanum} read-restriction action.
 *
 * <p>Replaces alphanumeric code points with {@code x} (letters) or {@code n} (digits) while
 * preserving a small punctuation allow-list. String-typed input only.
 *
 * <p>Delegates to {@link Actions#bind(Action, org.apache.iceberg.types.Type)} for the canonical
 * masking semantics so Flink/Trino and the interpreted path share one implementation.
 */
public class MaskAlphanumFunction implements UnboundFunction {

  private static final int VALUE_ORDINAL = 0;

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() != 1) {
      throw new UnsupportedOperationException("mask-alphanum requires a single string argument");
    }

    StructField valueField = inputType.fields()[VALUE_ORDINAL];
    DataType type = valueField.dataType();
    if (!(type instanceof StringType)) {
      throw new UnsupportedOperationException("mask-alphanum requires STRING type, got " + type);
    }

    return new BoundMaskAlphanum();
  }

  @Override
  public String description() {
    return name()
        + "(col) - Mask alphanumeric characters in a string, preserving structural punctuation";
  }

  @Override
  public String name() {
    return "iceberg_mask_alphanum";
  }

  public static class BoundMaskAlphanum implements ScalarFunction<UTF8String> {
    private static final SerializableFunction<Object, Object> FN =
        Actions.bind(new Action.MaskAlphanum(0), Types.StringType.get());

    /** Magic method used in codegen. */
    public static UTF8String invoke(UTF8String value) {
      if (value == null) {
        return null;
      }
      return UTF8String.fromString((String) FN.apply(value.toString()));
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.StringType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.StringType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.mask_alphanum";
    }

    @Override
    public String name() {
      return "iceberg_mask_alphanum";
    }

    @Override
    public UTF8String produceResult(InternalRow input) {
      if (input.isNullAt(VALUE_ORDINAL)) {
        return null;
      }
      return invoke(input.getUTF8String(VALUE_ORDINAL));
    }
  }
}

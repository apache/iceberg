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
package org.apache.iceberg.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.ZOrderByteUtils;

class ZOrder<T> implements Transform<T, ByteBuffer> {

  @SuppressWarnings("unchecked")
  static <T> ZOrder<T> get(int varTypeSize) {
    return new ZOrder<>(varTypeSize);
  }

  private final int varTypeSize;

  private ZOrder(int varTypeSize) {
    this.varTypeSize = varTypeSize;
  }

  public int varTypeSize() {
    return varTypeSize;
  }

  private boolean isPrimitiveSupported(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case STRING:
      case BINARY:
      case FIXED:
        // todo: decide if we want to support these types
        // case DECIMAL:
        // case UUID:
        return true;
    }
    return false;
  }

  @Override
  public boolean canTransform(Type type) {
    if (type.isPrimitiveType()) {
      return isPrimitiveSupported(type);
    } else if (type.isStructType()) {
      // only all the fields are supported primitive, we can transform the struct
      return type.asStructType().fields().stream().allMatch(f -> isPrimitiveSupported(f.type()));
    }
    return false;
  }

  @Override
  public SerializableFunction<T, ByteBuffer> bind(Type type) {
    Preconditions.checkArgument(canTransform(type), "Cannot transform type: %s", type);
    return new ZOrderFunction<>(varTypeSize, type);
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.BinaryType.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof ZOrder)) {
      return false;
    }

    ZOrder<?> that = (ZOrder<?>) o;
    return varTypeSize == that.varTypeSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(varTypeSize);
  }

  @Override
  public String toString() {
    return "zOrder[" + varTypeSize + "]";
  }

  @Override
  public UnboundPredicate<ByteBuffer> project(String name, BoundPredicate<T> predicate) {
    // todo: support project later, at least we can project predicates on the zOrder transformed
    // term
    return null;
  }

  @Override
  public UnboundPredicate<ByteBuffer> projectStrict(String name, BoundPredicate<T> predicate) {
    // todo: support projectStrict later
    return null;
  }

  // todo: poc implementation, could be optimized
  private static class ZOrderFunction<T> implements SerializableFunction<T, ByteBuffer> {
    private final int varTypeSize;
    private final Type inputType;
    private CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder();
    private byte[][] inputBuffer;
    private int outputSize;

    private ZOrderFunction(int varTypeSize, Type inputType) {
      this.varTypeSize = varTypeSize;
      this.inputType = inputType;
      initByType(inputType);
    }

    private void initPerPrimitiveType(int pos, Type.PrimitiveType type) {
      switch (type.typeId()) {
        case BOOLEAN:
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case DATE:
        case TIME:
        case TIMESTAMP:
          this.inputBuffer[pos] = new byte[ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE];
          this.outputSize += ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;
          break;
        case STRING:
        case BINARY:
        case FIXED:
          this.inputBuffer[pos] = new byte[varTypeSize];
          this.outputSize += varTypeSize;
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported type for ZOrder: " + type.typeId().name());
      }
    }

    private void initByType(Type type) {
      if (type.isPrimitiveType()) {
        this.inputBuffer = new byte[1][];
        this.outputSize = 0;
        initPerPrimitiveType(0, type.asPrimitiveType());
      } else if (type.isStructType()) {
        Types.StructType structType = type.asStructType();
        List<Types.NestedField> fields = structType.fields();
        this.inputBuffer = new byte[fields.size()][];
        this.outputSize = 0;
        for (int i = 0; i < fields.size(); i++) {
          initPerPrimitiveType(i, fields.get(i).type().asPrimitiveType());
        }
      }
    }

    private void primitiveTypeToOrderedBytes(Object value, int pos, Type.PrimitiveType type) {
      ByteBuffer reuse = ByteBuffer.wrap(inputBuffer[pos]);
      if (value == null) {
        // for null input, a ByteBuffer with all zeros should be returned
        ZOrderByteUtils.byteTruncateOrFill(new byte[0], reuse.capacity(), reuse);
        return;
      }

      switch (type.typeId()) {
        case BOOLEAN:
          reuse.put(0, (byte) ((boolean) value ? -127 : 0));
          break;
        case INTEGER:
        case DATE:
          ZOrderByteUtils.intToOrderedBytes((int) value, reuse);
          break;
        case LONG:
        case TIME:
        case TIMESTAMP:
          ZOrderByteUtils.longToOrderedBytes((long) value, reuse);
          break;
        case FLOAT:
          ZOrderByteUtils.floatToOrderedBytes((float) value, reuse);
          break;
        case DOUBLE:
          ZOrderByteUtils.doubleToOrderedBytes((double) value, reuse);
          break;
        case STRING:
          ZOrderByteUtils.stringToOrderedBytes(
              ((CharSequence) value).toString(), varTypeSize, reuse, encoder);
          break;
        case BINARY:
        case FIXED:
          ByteBuffer input = (ByteBuffer) value;
          ZOrderByteUtils.byteTruncateOrFill(ByteBuffers.toByteArray(input), varTypeSize, reuse);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported type for ZOrder: " + inputType.typeId().name() + " in " + inputType);
      }
    }

    private void typeToOrderedBytes(Object value, Type type) {
      if (type.isPrimitiveType()) {
        primitiveTypeToOrderedBytes(value, 0, type.asPrimitiveType());
      } else if (type.isStructType()) {
        Types.StructType structType = type.asStructType();
        List<Types.NestedField> fields = structType.fields();
        StructLike struct = (StructLike) value;
        for (int i = 0; i < fields.size(); i++) {
          Object element = struct.get(i, Objects.class);
          primitiveTypeToOrderedBytes(element, i, fields.get(i).type().asPrimitiveType());
        }
      }
    }

    @Override
    public ByteBuffer apply(T value) {
      if (value == null) {
        return null;
      }
      ByteBuffer reuse = ByteBuffer.allocate(outputSize);
      typeToOrderedBytes(value, inputType);
      byte[] result = ZOrderByteUtils.interleaveBits(inputBuffer, outputSize, reuse);
      return ByteBuffer.wrap(result);
    }
  }
}

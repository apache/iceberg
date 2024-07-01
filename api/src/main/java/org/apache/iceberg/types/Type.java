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
package org.apache.iceberg.types;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.StructLike;

public interface Type extends Serializable {
  enum TypeID {
    BOOLEAN(Boolean.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    DATE(Integer.class),
    TIME(Long.class),
    TIMESTAMP(Long.class),
    TIMESTAMP_NANO(Long.class),
    STRING(CharSequence.class),
    UUID(java.util.UUID.class),
    FIXED(ByteBuffer.class),
    BINARY(ByteBuffer.class),
    DECIMAL(BigDecimal.class),
    STRUCT(StructLike.class),
    LIST(List.class),
    MAP(Map.class);

    private final Class<?> javaClass;

    TypeID(Class<?> javaClass) {
      this.javaClass = javaClass;
    }

    public Class<?> javaClass() {
      return javaClass;
    }
  }

  TypeID typeId();

  default boolean isPrimitiveType() {
    return false;
  }

  default PrimitiveType asPrimitiveType() {
    throw new IllegalArgumentException("Not a primitive type: " + this);
  }

  default Types.StructType asStructType() {
    throw new IllegalArgumentException("Not a struct type: " + this);
  }

  default Types.ListType asListType() {
    throw new IllegalArgumentException("Not a list type: " + this);
  }

  default Types.MapType asMapType() {
    throw new IllegalArgumentException("Not a map type: " + this);
  }

  default boolean isNestedType() {
    return false;
  }

  default boolean isStructType() {
    return false;
  }

  default boolean isListType() {
    return false;
  }

  default boolean isMapType() {
    return false;
  }

  default NestedType asNestedType() {
    throw new IllegalArgumentException("Not a nested type: " + this);
  }

  abstract class PrimitiveType implements Type {
    @Override
    public boolean isPrimitiveType() {
      return true;
    }

    @Override
    public PrimitiveType asPrimitiveType() {
      return this;
    }

    Object writeReplace() throws ObjectStreamException {
      return new PrimitiveHolder(toString());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof PrimitiveType)) {
        return false;
      }

      PrimitiveType that = (PrimitiveType) o;
      return typeId() == that.typeId();
    }

    @Override
    public int hashCode() {
      return Objects.hash(PrimitiveType.class, typeId());
    }
  }

  abstract class NestedType implements Type {
    @Override
    public boolean isNestedType() {
      return true;
    }

    @Override
    public NestedType asNestedType() {
      return this;
    }

    public abstract List<Types.NestedField> fields();

    public abstract Type fieldType(String name);

    public abstract Types.NestedField field(int id);
  }
}

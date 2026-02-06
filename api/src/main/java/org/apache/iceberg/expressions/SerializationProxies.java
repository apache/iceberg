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

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Stand-in classes for expression classes in Java Serialization.
 *
 * <p>These are used so that expression classes are immutable and can use final fields.
 */
class SerializationProxies {
  static class ConstantExpressionProxy implements Serializable {
    private Boolean trueOrFalse = null;

    /** Constructor for Java serialization. */
    ConstantExpressionProxy() {}

    ConstantExpressionProxy(boolean trueOrFalse) {
      this.trueOrFalse = trueOrFalse;
    }

    Object readResolve() throws ObjectStreamException {
      if (trueOrFalse) {
        return True.INSTANCE;
      } else {
        return False.INSTANCE;
      }
    }
  }

  static class BinaryLiteralProxy extends FixedLiteralProxy {
    /** Constructor for Java serialization. */
    BinaryLiteralProxy() {}

    BinaryLiteralProxy(ByteBuffer buffer) {
      super(buffer);
    }

    @Override
    Object readResolve() throws ObjectStreamException {
      return new Literals.BinaryLiteral(ByteBuffer.wrap(bytes()));
    }
  }

  /** Replacement for FixedLiteral in Java Serialization. */
  static class FixedLiteralProxy implements Serializable {
    private byte[] bytes;

    /** Constructor for Java serialization. */
    FixedLiteralProxy() {}

    FixedLiteralProxy(ByteBuffer buffer) {
      this.bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
    }

    Object readResolve() throws ObjectStreamException {
      return new Literals.FixedLiteral(ByteBuffer.wrap(bytes));
    }

    protected byte[] bytes() {
      return bytes;
    }
  }
}

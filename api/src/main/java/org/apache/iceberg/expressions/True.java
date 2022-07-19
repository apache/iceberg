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

/** An {@link Expression expression} that is always true. */
public class True implements Expression {
  static final True INSTANCE = new True();

  private True() {}

  @Override
  public Operation op() {
    return Operation.TRUE;
  }

  @Override
  public Expression negate() {
    return False.INSTANCE;
  }

  @Override
  public boolean isEquivalentTo(Expression other) {
    return other.op() == Operation.TRUE;
  }

  @Override
  public String toString() {
    return "true";
  }

  Object writeReplace() throws ObjectStreamException {
    return new SerializationProxies.ConstantExpressionProxy(true);
  }
}

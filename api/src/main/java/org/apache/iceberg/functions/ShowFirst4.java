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
package org.apache.iceberg.functions;

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/** Preserves the first 4 code points of a string, redacts the rest via mask-alphanum rules. */
public final class ShowFirst4 extends IcebergFunction.BaseFunction<String, String> {
  public ShowFirst4(int fieldId) {
    super(fieldId);
  }

  @Override
  public String name() {
    return SHOW_FIRST_4;
  }

  @Override
  public boolean canBind(Type type) {
    return type.typeId() == Type.TypeID.STRING;
  }

  @Override
  public SerializableFunction<String, String> bind(Type type) {
    Preconditions.checkArgument(canBind(type), "show-first-4 requires STRING type, got %s", type);
    return ShowFirst4Fn.INSTANCE;
  }

  private static final class ShowFirst4Fn
      extends IcebergFunctions.NullSafeFunction<String, String> {
    static final ShowFirst4Fn INSTANCE = new ShowFirst4Fn();

    @Override
    protected String applyNonNull(String input) {
      if (input.codePointCount(0, input.length()) <= 4) {
        return input;
      }
      StringBuilder sb = new StringBuilder(input.length());
      int cpIndex = 0;
      int offset = 0;
      while (offset < input.length()) {
        int cp = input.codePointAt(offset);
        sb.appendCodePoint(cpIndex < 4 ? cp : MaskAlphanum.maskCodePoint(cp));
        offset += Character.charCount(cp);
        cpIndex++;
      }
      return sb.toString();
    }
  }
}

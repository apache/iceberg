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

/** Redacts all but the last 4 code points of a string via mask-alphanum rules. */
public final class ShowLast4 extends Action.BaseAction<String> {
  public ShowLast4(int fieldId) {
    super(fieldId);
  }

  @Override
  public String actionType() {
    return SHOW_LAST_4;
  }

  @Override
  public boolean canBind(Type type) {
    return type.typeId() == Type.TypeID.STRING;
  }

  @Override
  public SerializableFunction<String, String> bind(Type type) {
    Preconditions.checkArgument(canBind(type), "show-last-4 requires STRING type, got %s", type);
    return ShowLast4Fn.INSTANCE;
  }

  private static final class ShowLast4Fn extends Actions.NullSafeFunction<String, String> {
    static final ShowLast4Fn INSTANCE = new ShowLast4Fn();

    @Override
    protected String applyNonNull(String input) {
      // Single pass: walk the string while remembering the last 4 code-point start offsets.
      // When done, everything before the oldest remembered offset is masked; everything from
      // that offset onward is kept verbatim.
      int[] lastFourStarts = new int[4];
      int count = 0;
      int offset = 0;
      while (offset < input.length()) {
        lastFourStarts[count % 4] = offset;
        int cp = input.codePointAt(offset);
        offset += Character.charCount(cp);
        count++;
      }
      if (count <= 4) {
        return input;
      }
      int keepFromOffset = lastFourStarts[count % 4];
      StringBuilder sb = new StringBuilder(input.length());
      int maskOffset = 0;
      while (maskOffset < keepFromOffset) {
        int cp = input.codePointAt(maskOffset);
        sb.appendCodePoint(MaskAlphanum.maskCodePoint(cp));
        maskOffset += Character.charCount(cp);
      }
      sb.append(input, keepFromOffset, input.length());
      return sb.toString();
    }
  }
}

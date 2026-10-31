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

import java.io.ObjectStreamException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/** Redacts every Unicode code point in a string per the mask_alphanum rules. */
final class MaskAlphanum implements IcebergFunction<CharSequence, CharSequence> {
  static final String NAME = "mask_alphanum";

  private static final MaskAlphanum INSTANCE = new MaskAlphanum();

  static MaskAlphanum get() {
    return INSTANCE;
  }

  private MaskAlphanum() {}

  @Override
  public String toString() {
    return NAME;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.MaskAlphanumProxy.get();
  }

  @Override
  public boolean canBind(Type type) {
    return type.typeId() == Type.TypeID.STRING;
  }

  @Override
  public SerializableFunction<CharSequence, CharSequence> bind(Type type) {
    Preconditions.checkArgument(canBind(type), "mask_alphanum requires STRING type, got %s", type);
    return MaskAlphanumFn.INSTANCE;
  }

  /**
   * Maps a code point through the mask_alphanum rules; also used by {@link ShowFirst4} and {@link
   * ShowLast4} on the code points outside their preserved windows:
   *
   * <ul>
   *   <li>ASCII digits (0-9) -&gt; {@code 'n'}
   *   <li>Structural punctuation kept as-is: {@code ( ) , . - @}
   *   <li>Everything else -&gt; {@code 'x'}
   * </ul>
   */
  static int maskCodePoint(int cp) {
    if (cp >= 0x30 && cp <= 0x39) {
      return 'n';
    } else if (cp == '(' || cp == ')' || cp == ',' || cp == '.' || cp == '-' || cp == '@') {
      return cp;
    } else {
      return 'x';
    }
  }

  private static final class MaskAlphanumFn extends NullSafeFunction<CharSequence, CharSequence> {
    static final MaskAlphanumFn INSTANCE = new MaskAlphanumFn();

    @Override
    protected CharSequence applyNonNull(CharSequence input) {
      StringBuilder sb = new StringBuilder(input.length());
      int offset = 0;
      while (offset < input.length()) {
        int cp = Character.codePointAt(input, offset);
        sb.appendCodePoint(maskCodePoint(cp));
        offset += Character.charCount(cp);
      }
      return sb.toString();
    }
  }
}

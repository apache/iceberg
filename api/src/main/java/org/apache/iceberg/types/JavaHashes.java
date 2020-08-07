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

import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;

public class JavaHashes {
  private JavaHashes() {
  }

  public static int hashCode(CharSequence str) {
    int result = 177;
    for (int i = 0; i < str.length(); i += 1) {
      char ch = str.charAt(i);
      result = 31 * result + (int) ch;
    }
    return result;
  }

  static JavaHash<CharSequence> strings() {
    return CharSequenceHash.INSTANCE;
  }

  static JavaHash<StructLike> struct(Types.StructType struct) {
    return new StructLikeHash(struct);
  }

  static JavaHash<List<?>> list(Types.ListType list) {
    return new ListHash(list);
  }

  private static class CharSequenceHash implements JavaHash<CharSequence> {
    private static final CharSequenceHash INSTANCE = new CharSequenceHash();

    private CharSequenceHash() {
    }

    @Override
    public int hash(CharSequence str) {
      if (str == null) {
        return 0;
      }

      return JavaHashes.hashCode(str);
    }
  }

  private static class StructLikeHash implements JavaHash<StructLike> {
    private final JavaHash<Object>[] hashes;

    private StructLikeHash(Types.StructType struct) {
      this.hashes = struct.fields().stream()
          .map(field ->
            "unknown-partition".equals(field.doc()) ?
                (JavaHash<Object>) Objects::hashCode :
                JavaHash.forType(field.type()))
          .toArray((IntFunction<JavaHash<Object>[]>) JavaHash[]::new);
    }

    @Override
    public int hash(StructLike struct) {
      int result = 97;
      int len = hashes.length;
      result = 41 * result + len;
      for (int i = 0; i < len; i += 1) {
        result = 41 * result + hashes[i].hash(struct.get(i, Object.class));
      }
      return result;
    }
  }

  private static class ListHash implements JavaHash<List<?>> {
    private final JavaHash<Object> elementHash;

    private ListHash(Types.ListType list) {
      this.elementHash = JavaHash.forType(list.elementType());
    }

    @Override
    public int hash(List<?> list) {
      int result = 17;
      int len = list.size();
      result = 37 * result + len;
      for (int i = 0; i < len; i += 1) {
        result = 37 * result + elementHash.hash(list.get(i));
      }
      return result;
    }
  }
}

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
import java.util.function.IntFunction;
import org.apache.iceberg.StructLike;

public class JavaHashes {
  private JavaHashes() {}

  public static int hashCode(CharSequence str) {
    int result = 177;
    for (int i = 0; i < str.length(); i += 1) {
      char ch = str.charAt(i);
      result = 31 * result + (int) ch;
    }
    return result;
  }

  /**
   * Computes a hash code for a given file path.
   *
   * <p>This method takes into account only the file name at the end of the path. The hash code is
   * computed by processing characters in reverse order, starting from the end of the sequence until
   * a slash ('/') is encountered, which indicates the start of the file name. This approach is
   * beneficial for computing hash codes of Iceberg file paths as those have common prefixes but
   * unique file names.
   */
  public static int filePathHashCode(CharSequence filePath) {
    int result = 177;
    for (int i = filePath.length() - 1; i >= 0; i--) {
      char ch = filePath.charAt(i);
      if (ch == '/') {
        break;
      }
      result = 31 * result + (int) ch;
    }
    return result;
  }

  static JavaHash<Object> strings() {
    return CharSequenceHash.INSTANCE;
  }

  static JavaHash<StructLike> struct(Types.StructType struct) {
    return new StructLikeHash(struct);
  }

  static JavaHash<List<?>> list(Types.ListType list) {
    return new ListHash(list);
  }

  private static class CharSequenceHash implements JavaHash<Object> {
    private static final CharSequenceHash INSTANCE = new CharSequenceHash();

    private CharSequenceHash() {}

    @Override
    public int hash(Object str) {
      if (str instanceof CharSequence) {
        return JavaHashes.hashCode((CharSequence) str);
      } else if (str != null) {
        // UnknownTransform results are assumed to be string, the most generic type. But there is no
        // guarantee that the
        // values actually are strings so this can receive non-string values to hash. To get a
        // consistent hash code for
        // those values, convert to string an hash the string.
        return JavaHashes.hashCode(str.toString());
      }

      return 0;
    }
  }

  private static class StructLikeHash implements JavaHash<StructLike> {
    private final JavaHash<Object>[] hashes;

    private StructLikeHash(Types.StructType struct) {
      this.hashes =
          struct.fields().stream()
              .map(Types.NestedField::type)
              .map(JavaHash::forType)
              .toArray((IntFunction<JavaHash<Object>[]>) JavaHash[]::new);
    }

    @Override
    public int hash(StructLike struct) {
      if (struct == null) {
        return 0;
      }

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
      if (list == null) {
        return 0;
      }

      int result = 17;
      result = 37 * result + list.size();
      for (Object o : list) {
        result = 37 * result + elementHash.hash(o);
      }
      return result;
    }
  }
}

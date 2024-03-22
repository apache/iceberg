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
package org.apache.iceberg.util;

import java.io.Serializable;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHashes;

/** Wrapper class to adapt CharSequence for use in maps and sets. */
public class CharSequenceWrapper implements CharSequence, Serializable {
  public static CharSequenceWrapper wrap(CharSequence seq) {
    return new CharSequenceWrapper(seq);
  }

  private CharSequence wrapped;
  // lazily computed & cached hashCode
  private transient int hashCode = 0;

  private CharSequenceWrapper(CharSequence wrapped) {
    this.wrapped = wrapped;
  }

  public CharSequenceWrapper set(CharSequence newWrapped) {
    this.wrapped = newWrapped;
    this.hashCode = 0;
    return this;
  }

  public CharSequence get() {
    return wrapped;
  }

  @Override
  @SuppressWarnings("UndefinedEquals")
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof CharSequenceWrapper)) {
      return false;
    }

    CharSequenceWrapper that = (CharSequenceWrapper) other;

    if (hashCode != 0 && that.hashCode != 0 && hashCode != that.hashCode) {
      return false;
    }

    if (wrapped instanceof String && that.wrapped instanceof String) {
      return wrapped.equals(that.wrapped);
    }

    if (null == wrapped && null == that.wrapped) {
      return true;
    }

    if (length() != that.length()) {
      return false;
    }

    return Comparators.charSequences().compare(wrapped, that.wrapped) == 0;
  }

  @Override
  public int hashCode() {
    int hash = hashCode;
    if (hash == 0) {
      hash = JavaHashes.hashCode(wrapped);
      this.hashCode = hash;
    }

    return hash;
  }

  @Override
  public int length() {
    return wrapped.length();
  }

  @Override
  public char charAt(int index) {
    return wrapped.charAt(index);
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return wrapped.subSequence(start, end);
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}

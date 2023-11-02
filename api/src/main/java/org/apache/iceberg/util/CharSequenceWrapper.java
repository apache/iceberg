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
import java.util.Comparator;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.JavaHashes;

/** Wrapper class to adapt CharSequence for use in maps and sets. */
public class CharSequenceWrapper implements CharSequence, Serializable {

  public static CharSequenceWrapper wrap(CharSequence seq) {
    return wrap(seq, Comparators.charSequences());
  }

  public static CharSequenceWrapper wrap(CharSequence seq, Comparator<CharSequence> comparator) {
    return new CharSequenceWrapper(seq, comparator);
  }

  private CharSequence wrapped;
  private final transient Comparator<CharSequence> comparator;

  private CharSequenceWrapper(CharSequence wrapped, Comparator<CharSequence> comparator) {
    this.wrapped = wrapped;
    this.comparator = comparator;
  }

  public CharSequenceWrapper set(CharSequence newWrapped) {
    this.wrapped = newWrapped;
    return this;
  }

  public CharSequence get() {
    return wrapped;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof CharSequenceWrapper)) {
      return false;
    }

    CharSequenceWrapper that = (CharSequenceWrapper) other;
    return comparator.compare(wrapped, that.wrapped) == 0;
  }

  @Override
  public int hashCode() {
    return JavaHashes.hashCode(wrapped);
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

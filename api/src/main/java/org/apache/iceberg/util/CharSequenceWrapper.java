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

/**
 * Wrapper class to adapt CharSequence for use in maps and sets.
 */
public class CharSequenceWrapper implements Serializable {
  public static CharSequenceWrapper wrap(CharSequence seq) {
    return new CharSequenceWrapper(seq);
  }

  private CharSequence wrapped;

  private CharSequenceWrapper(CharSequence wrapped) {
    this.wrapped = wrapped;
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
    }
    if (other == null) {
      return false;
    }

    if (other instanceof CharSequence) {
      return Comparators.charSequences().compare(wrapped, (CharSequence) other) == 0;
    } else if (other instanceof CharSequenceWrapper) {
      CharSequenceWrapper that = (CharSequenceWrapper) other;
      return Comparators.charSequences().compare(wrapped, that.wrapped) == 0;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }
}

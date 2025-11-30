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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class CharSequenceSet extends WrapperSet<CharSequence> {
  private static final ThreadLocal<CharSequenceWrapper> WRAPPERS =
      ThreadLocal.withInitial(() -> CharSequenceWrapper.wrap(null));

  private CharSequenceSet() {
    // needed for serialization
  }

  private CharSequenceSet(Iterable<? extends CharSequence> charSequences) {
    super(
        Iterables.transform(
            charSequences,
            obj -> {
              Preconditions.checkNotNull(obj, "Invalid object: null");
              return CharSequenceWrapper.wrap(obj);
            }));
  }

  public static CharSequenceSet of(Iterable<? extends CharSequence> charSequences) {
    return new CharSequenceSet(charSequences);
  }

  public static CharSequenceSet empty() {
    return new CharSequenceSet();
  }

  @Override
  protected Wrapper<CharSequence> wrapper() {
    return WRAPPERS.get();
  }

  @Override
  protected Wrapper<CharSequence> wrap(CharSequence file) {
    return CharSequenceWrapper.wrap(file);
  }

  @Override
  protected Class<CharSequence> elementClass() {
    return CharSequence.class;
  }

  @Override
  public boolean add(CharSequence charSequence) {
    // method is needed to not break API compatibility
    return super.add(charSequence);
  }
}

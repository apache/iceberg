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

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A {@link Term} over an ordered list of column references.
 *
 * <p>Shared base of multi-column expressions such as {@link Zorder} and {@link Hilbert}, which
 * differ only in how an engine combines the referenced columns.
 */
public abstract class MultiColumnTerm implements Term {
  private final List<NamedReference<?>> refs;

  protected MultiColumnTerm(List<NamedReference<?>> refs) {
    this.refs = ImmutableList.copyOf(refs);
  }

  public List<NamedReference<?>> refs() {
    return refs;
  }
}

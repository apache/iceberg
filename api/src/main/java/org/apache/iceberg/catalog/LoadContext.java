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
package org.apache.iceberg.catalog;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Caller-supplied details about a load request, such as the chain of views that reference the table
 * or view being loaded.
 *
 * <p>A catalog may use these fields to make authorization, credential-scoping, and auditing
 * decisions. All fields are optional; {@link #empty()} carries none.
 */
public final class LoadContext {
  private static final LoadContext EMPTY = builder().build();

  private final List<TableIdentifier> referencedBy;

  private LoadContext(Builder builder) {
    this.referencedBy =
        builder.referencedBy != null
            ? ImmutableList.copyOf(builder.referencedBy)
            : ImmutableList.of();
  }

  /**
   * Returns the view identifiers that form the reference chain, outermost first.
   *
   * <p>For example, if view A references view B which references table C, loading C would have
   * {@code referencedBy = [A, B]}.
   *
   * @return an unmodifiable list of referencing view identifiers, or an empty list if none
   */
  public List<TableIdentifier> referencedBy() {
    return referencedBy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static LoadContext empty() {
    return EMPTY;
  }

  public static final class Builder {
    private List<TableIdentifier> referencedBy;

    private Builder() {}

    public Builder referencedBy(List<TableIdentifier> views) {
      this.referencedBy = views;
      return this;
    }

    public LoadContext build() {
      return new LoadContext(this);
    }
  }
}

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

package org.apache.iceberg.rest.requests;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.immutables.value.Value;

/**
 * A REST request to create a namespace, with an optional set of properties.
 *
 * Note that Immutable classes by definition don't have a default no-arg constructor (which is required for Jackson),
 * therefore the @{@link JsonSerialize}/@{@link JsonDeserialize} annotations on the class will generate what's
 * necessary for Jackson-binding to properly work with this class.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableCreateNamespaceRequest.class)
@JsonDeserialize(as = ImmutableCreateNamespaceRequest.class)
public abstract class CreateNamespaceRequest {

  public abstract Namespace namespace();

  public abstract Map<String, String> properties();

  public static CreateNamespaceRequest of(Namespace namespace) {
    return ImmutableCreateNamespaceRequest.builder().namespace(namespace).build();
  }

  public static CreateNamespaceRequest of(Namespace namespace, Map<String, String> properties) {
    return ImmutableCreateNamespaceRequest.builder().namespace(namespace).properties(properties).build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace())
        .add("properties", properties())
        .toString();
  }
}

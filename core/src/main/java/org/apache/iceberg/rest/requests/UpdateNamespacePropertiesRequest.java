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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.immutables.value.Value;

/**
 * A REST request to set and/or remove properties on a namespace.
 *
 * Note that Immutable classes by definition don't have a default no-arg constructor (which is required for Jackson),
 *  therefore the @{@link JsonSerialize}/@{@link JsonDeserialize} annotations on the class will generate what's
 *  necessary for Jackson-binding to properly work with this class.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableUpdateNamespacePropertiesRequest.class)
@JsonDeserialize(as = ImmutableUpdateNamespacePropertiesRequest.class)
public abstract class UpdateNamespacePropertiesRequest {

  public abstract List<String> removals();

  public abstract Map<String, String> updates();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("removals", removals())
        .add("updates", updates())
        .toString();
  }
}


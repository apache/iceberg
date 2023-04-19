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
package org.apache.iceberg.dell.ecs;

import java.net.URI;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/** An immutable record class of ECS location */
class EcsURI {

  private static final Set<String> VALID_SCHEME = ImmutableSet.of("ecs", "s3", "s3a", "s3n");

  private final String location;
  private final String bucket;
  private final String name;

  EcsURI(String location) {
    Preconditions.checkNotNull(location == null, "Location %s can not be null", location);

    this.location = location;

    URI uri = URI.create(location);
    ValidationException.check(
        VALID_SCHEME.contains(uri.getScheme().toLowerCase()), "Invalid ecs location: %s", location);
    this.bucket = uri.getHost();
    this.name = uri.getPath().replaceAll("^/*", "");
  }

  /** The leading slashes of name will be ignored. */
  EcsURI(String bucket, String name) {
    this.bucket = bucket;
    this.name = name.replaceAll("^/*", "");
    this.location = String.format("ecs://%s/%s", bucket, name);
  }

  /** Returns ECS bucket name. */
  public String bucket() {
    return bucket;
  }

  /** Returns ECS object name. */
  public String name() {
    return name;
  }

  /** Returns original location. */
  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }
}

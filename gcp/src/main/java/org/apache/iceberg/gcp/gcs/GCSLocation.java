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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.BlobId;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This class represents a fully qualified location in GCS expressed as a URI. This class allows for
 * URIs with only a bucket and no path specified, unlike with {@link BlobId#fromGsUtilUri(String)}.
 */
class GCSLocation {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";

  private static final String EXPECTED_SCHEME = "gs";

  private final String bucket;
  private final String prefix;

  /**
   * Creates a new GCSLocation with the form of scheme://bucket/path?query#fragment
   *
   * @param location fully qualified URI
   */
  GCSLocation(String location) {
    Preconditions.checkArgument(location != null, "Invalid location: null");

    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(
        schemeSplit.length == 2, "Invalid GCS URI, cannot determine scheme: %s", location);

    String scheme = schemeSplit[0];
    ValidationException.check(
        EXPECTED_SCHEME.equals(scheme), "Invalid GCS URI, invalid scheme: %s", scheme);

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);

    this.bucket = authoritySplit[0];

    // Strip query and fragment if they exist
    String path = authoritySplit.length > 1 ? authoritySplit[1] : "";
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.prefix = path;
  }

  /** Returns GCS bucket name. */
  public String bucket() {
    return bucket;
  }

  /** Returns GCS object name prefix. */
  public String prefix() {
    return prefix;
  }
}

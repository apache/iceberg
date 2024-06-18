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
package org.apache.iceberg.aliyun.oss;

import com.aliyun.oss.internal.OSSUtils;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

/**
 * This class represents a fully qualified location in OSS for input/output operations expressed as
 * as URI. This implementation is provided to ensure compatibility with Hadoop Path implementations
 * that may introduce encoding issues with native URI implementation.
 *
 * <p>Note: Path-style access is deprecated and not supported by this implementation.
 */
public class OSSURI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMES = ImmutableSet.of("https", "oss");
  private final String location;
  private final String bucket;
  private final String key;

  /**
   * Creates a new OSSURI based on the bucket and key parsed from the location The location in
   * string form has the syntax as below, which refers to RFC2396: [scheme:][//bucket][object
   * key][#fragment] [scheme:][//bucket][object key][?query][#fragment]
   *
   * <p>It specifies precisely which characters are permitted in the various components of a URI
   * reference in Aliyun OSS documentation as below: Bucket:
   * https://help.aliyun.com/document_detail/257087.html Object:
   * https://help.aliyun.com/document_detail/273129.html Scheme: https or oss
   *
   * <p>Supported access styles are https and oss://... URIs.
   *
   * @param location fully qualified URI.
   */
  public OSSURI(String location) {
    Preconditions.checkNotNull(location, "OSS location cannot be null.");

    this.location = location;
    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(schemeSplit.length == 2, "Invalid OSS location: %s", location);

    String scheme = schemeSplit[0];
    ValidationException.check(
        VALID_SCHEMES.contains(scheme.toLowerCase(Locale.ROOT)),
        "Invalid scheme: %s in OSS location %s",
        scheme,
        location);

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(
        authoritySplit.length == 2, "Invalid bucket or key in OSS location: %s", location);
    ValidationException.check(
        !authoritySplit[1].trim().isEmpty(), "Missing key in OSS location: %s", location);
    this.bucket = authoritySplit[0];
    OSSUtils.ensureBucketNameValid(bucket);

    // Strip query and fragment if they exist
    String path = authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = path;
    OSSUtils.ensureObjectKeyValid(key);
  }

  /** Return OSS bucket name. */
  public String bucket() {
    return bucket;
  }

  /** Return OSS object key name. */
  public String key() {
    return key;
  }

  /** Return original, unmodified OSS URI location. */
  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }
}

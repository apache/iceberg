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
package org.apache.iceberg.huaweicloud.obs;

import com.obs.services.internal.utils.ServiceUtils;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class OBSURI {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";
  private static final String QUERY_DELIM = "\\?";
  private static final String FRAGMENT_DELIM = "#";
  private static final Set<String> VALID_SCHEMES = ImmutableSet.of("https", "obs");
  private final String location;
  private final String bucket;
  private final String key;

  public OBSURI(String location) {
    Preconditions.checkNotNull(location, "OBS location cannot be null.");

    this.location = location;
    String[] schemeSplit = location.split(SCHEME_DELIM, -1);
    ValidationException.check(schemeSplit.length == 2, "Invalid OBS location: %s", location);

    String scheme = schemeSplit[0];
    ValidationException.check(
        VALID_SCHEMES.contains(scheme.toLowerCase()),
        "Invalid scheme: %s in OBS location %s",
        scheme,
        location);

    String[] authoritySplit = schemeSplit[1].split(PATH_DELIM, 2);
    ValidationException.check(
        authoritySplit.length == 2, "Invalid bucket or key in OBS location: %s", location);
    ValidationException.check(
        !authoritySplit[1].trim().isEmpty(), "Missing key in OBS location: %s", location);
    this.bucket = authoritySplit[0];
    if (!ServiceUtils.isBucketNameValidDNSName(bucket)) {
      throw new IllegalArgumentException("BucketNameInvalid:" + bucket);
    }

    // Strip query and fragment if they exist
    String path = authoritySplit[1];
    path = path.split(QUERY_DELIM, -1)[0];
    path = path.split(FRAGMENT_DELIM, -1)[0];
    this.key = path;
    if (!ServiceUtils.isValid(key)) {
      throw new IllegalArgumentException("ObjectKeyInvalid:" + key);
    }
  }

  /** Return OBS bucket name. */
  public String bucket() {
    return bucket;
  }

  /** Return OBS object key name. */
  public String key() {
    return key;
  }

  /** Return original, unmodified OBS URI location. */
  public String location() {
    return location;
  }

  @Override
  public String toString() {
    return location;
  }
}

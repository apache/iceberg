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
package org.apache.iceberg.azure.adlsv2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** This class represents a fully qualified location in Azure expressed as a URI. */
class ADLSv2Location {
  private static final Pattern URI_PATTERN =
      Pattern.compile("^abfss?://(.+?)@(.+?\\.dfs\\.core\\.windows\\.net)(/.*)?$");

  private final String storageAccountUrl;
  private final String container;
  private final String path;

  /**
   * Creates a new ADLSv2Location with the form of
   * scheme://container@storage_url/path?query#fragment
   *
   * @param location fully qualified URI
   */
  ADLSv2Location(String location) {
    Preconditions.checkArgument(location != null, "Invalid location: null");

    Matcher matcher = URI_PATTERN.matcher(location);

    ValidationException.check(matcher.matches(), "Invalid ADLSv2 URI: %s", location);

    this.container = matcher.group(1);
    this.storageAccountUrl = "https://" + matcher.group(2);

    String uriPath = matcher.group(3);
    uriPath = uriPath == null ? "" : uriPath.substring(1);
    this.path = uriPath.split("\\?", -1)[0].split("#", -1)[0];
  }

  /** Returns Azure storage account URL. */
  public String storageAccountUrl() {
    return storageAccountUrl;
  }

  /** Returns Azure container name. */
  public String container() {
    return container;
  }

  /** Returns ADLSv2 path. */
  public String path() {
    return path;
  }
}

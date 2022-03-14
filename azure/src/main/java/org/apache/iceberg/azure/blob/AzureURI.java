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

package org.apache.iceberg.azure.blob;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureURI {

  private static final Logger LOG = LoggerFactory.getLogger(AzureURI.class);
  private static final String AUTHORITY_DELIMITER = "@";
  private static final String ENDPOINT_DELIMITER = "\\.";
  private static final String PATH_DELIMITER = "/";
  private static final String ABFS_SCHEME = "abfs";
  private static final String EXPECTED_URI_FORM = String.format(
      "%s://[<container name>%s]<account name>.dfs.core.windows.net/<file path>",
      ABFS_SCHEME,
      AUTHORITY_DELIMITER);
  private static final String INVALID_URI_MESSAGE = "Invalid Azure URI. Expected form is '%s': %s";

  private final String location;
  private final String container;
  private final String storageAccount;
  private final String path;

  private AzureURI(String location) {
    Preconditions.checkNotNull(location, "Location cannot be null.");
    final URI uri;
    try {
      uri = new URI(location);
    } catch (URISyntaxException e) {
      throw new ValidationException("Invalid Azure URI: %s", location);
    }
    this.location = location;

    ValidationException.check(ABFS_SCHEME.equals(uri.getScheme()),
        "Invalid Azure URI scheme, Expected scheme is 'afbs': %s",
        location);

    final String rawAuthority = uri.getRawAuthority();
    ValidationException.check(rawAuthority != null && !rawAuthority.isEmpty(),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    final String[] authoritySplit = rawAuthority.split(AUTHORITY_DELIMITER, 2);
    ValidationException.check(authoritySplit.length == 2 && !authoritySplit[0].isEmpty(),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    this.container = authoritySplit[0];

    final String endpoint = authoritySplit[1];
    final String[] endpointSplit = endpoint.split(ENDPOINT_DELIMITER, 2);
    ValidationException.check(endpointSplit.length == 2 && !endpointSplit[0].isEmpty(),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    this.storageAccount = endpointSplit[0];

    final String rawPath = uri.getRawPath();
    ValidationException.check(rawPath != null && !rawPath.isEmpty() && !rawPath.equals(PATH_DELIMITER),
        "Invalid Azure URI, empty path: %s",
        location);
    this.path = rawPath;

    LOG.debug("Parsed AzureURI: {}", this);
  }

  @Override
  public String toString() {
    return String.format("AzureURI(location=%s, container=%s, storageAccount=%s, path=%s)",
        this.location,
        this.container,
        this.storageAccount,
        this.path);
  }

  public String location() {
    return location;
  }

  public String container() {
    return container;
  }

  public String storageAccount() {
    return storageAccount;
  }

  public String path() {
    return path;
  }

  public static AzureURI from(String location) {
    return new AzureURI(location);
  }
}

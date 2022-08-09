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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents an immutable fully qualified location in Azure Blob Storage for input/output operations
 * expressed as URI. This implementation is provided to ensure compatibility with Hadoop Path implementations.
 */
public class AzureURI {

  private static final Logger LOG = LoggerFactory.getLogger(AzureURI.class);
  private static final String AUTHORITY_DELIMITER = "@";
  private static final String ENDPOINT_DELIMITER = "\\.";
  private static final String PATH_DELIMITER = "/";
  private static final String ABFS_SCHEME = "abfs://";
  private static final String EXPECTED_URI_FORM = String.format(
      "%s[<container name>%s]<account name>.dfs.core.windows.net/<file path>",
      ABFS_SCHEME,
      AUTHORITY_DELIMITER);
  private static final String INVALID_URI_MESSAGE = "Invalid Azure URI. Expected form is '%s': %s";

  private final String location;
  private final String container;
  private final String storageAccount;
  private final String path;

  private AzureURI(String location) {
    Preconditions.checkNotNull(location, "Location cannot be null");
    Preconditions.checkArgument(location.startsWith(ABFS_SCHEME), INVALID_URI_MESSAGE, EXPECTED_URI_FORM, location);
    // abfs://<container-name>@<storage-account-name>.dfs.core.windows.net/<blob-path>
    this.location = location;

    // <container-name>@<storage-account-name>.dfs.core.windows.net/<blob-path>
    String locationWithoutScheme = location.substring(ABFS_SCHEME.length());

    // first index if "/"
    int authorityEndIndex = locationWithoutScheme.indexOf(PATH_DELIMITER);
    Preconditions.checkArgument(authorityEndIndex > 0, INVALID_URI_MESSAGE, EXPECTED_URI_FORM, location);

    // <container-name>@<storage-account-name>.dfs.core.windows.net
    String authority = locationWithoutScheme.substring(0, authorityEndIndex + 1);
    String[] authoritySplit = authority.split(AUTHORITY_DELIMITER, 2);
    Preconditions.checkArgument(
        authoritySplit.length == 2 && !authoritySplit[0].isEmpty(),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    this.container = authoritySplit[0];

    String endpoint = authoritySplit[1];
    String[] endpointSplit = endpoint.split(ENDPOINT_DELIMITER, 2);
    Preconditions.checkArgument(
        endpointSplit.length == 2 && !endpointSplit[0].isEmpty(),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    this.storageAccount = endpointSplit[0];

    // /<file-path>
    String filePath = locationWithoutScheme.substring(authorityEndIndex);
    Preconditions.checkArgument(
        !filePath.isEmpty() && !filePath.equals(PATH_DELIMITER),
        INVALID_URI_MESSAGE,
        EXPECTED_URI_FORM,
        location);
    this.path = filePath;

    LOG.debug("Parsed AzureURI: {}", this);
  }

  /**
   * Creates a new {@link AzureURI} based on the storage account, container and blob path parsed from the location.
   * <p>
   * Locations follow the conventions used by ABFS URI:
   * https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri
   * that follow the following convention
   * <pre>{@code abfs://<container-name>@<storage-account-name>.dfs.core.windows.net/<blob_path>}</pre>
   *
   * @param location fully qualified ABFS URI.
   * @return AzureURI
   */
  public static AzureURI from(String location) {
    return new AzureURI(location);
  }

  @Override
  public String toString() {
    return String.format(
        "AzureURI(location=%s, container=%s, storageAccount=%s, path=%s)",
        this.location,
        this.container,
        this.storageAccount,
        this.path);
  }

  /**
   * Returns the original, unmodified ABFS URI location.
   */
  public String location() {
    return location;
  }

  /**
   * Returns the azure storage container name.
   */
  public String container() {
    return container;
  }

  /**
   * Returns the azure storage account name.
   */
  public String storageAccount() {
    return storageAccount;
  }

  /**
   * Returns the azure storage blob path
   */
  public String path() {
    return path;
  }
}

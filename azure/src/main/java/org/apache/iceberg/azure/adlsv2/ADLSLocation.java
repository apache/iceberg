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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This class represents a fully qualified location in Azure Data Lake Storage, expressed as a URI.
 *
 * <p>Locations follow the conventions used by Hadoop's Azure support, i.e.
 *
 * <pre>{@code abfs[s]://[<container>@]<storageAccount>.dfs.core.windows.net/<path>}</pre>
 *
 * or
 *
 * <pre>{@code wasb[s]://<container>@<storageAccount>.blob.core.windows.net/<path>}</pre>
 *
 * For compatibility, paths using the wasb scheme are also accepted but will be processed via the
 * Azure Data Lake Storage Gen2 APIs and not the Blob Storage APIs.
 *
 * <p>See <a
 * href="https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri#uri-syntax">Hadoop
 * Azure Support</a>
 */
class ADLSLocation {
  private static final Pattern URI_PATTERN = Pattern.compile("^(abfss?|wasbs?)://([^/?#]+)(.*)?$");

  private final String storageAccount;
  private final String container;
  private final String path;

  /**
   * Creates a new ADLSLocation from a fully qualified URI.
   *
   * @param location fully qualified URI
   */
  ADLSLocation(String location) {
    Preconditions.checkArgument(location != null, "Invalid location: null");

    Matcher matcher = URI_PATTERN.matcher(location);

    ValidationException.check(matcher.matches(), "Invalid ADLS URI: %s", location);

    try {
      URI uri = new URI(location);
      this.container = uri.getUserInfo();
      // storage account name is the first part of the host
      int accountSplit = uri.getHost().indexOf('.');
      String storageAccountName = uri.getHost().substring(0, accountSplit);
      this.storageAccount = String.format("%s.dfs.core.windows.net", storageAccountName);
      this.path = uri.getPath().length() > 1 ? uri.getRawPath().substring(1) : "";
    } catch (URISyntaxException e) {
      throw new ValidationException("Invalid URI: %s", location);
    }
  }

  /** Returns Azure storage account. */
  public String storageAccount() {
    return storageAccount;
  }

  /** Returns Azure container name. */
  public Optional<String> container() {
    return Optional.ofNullable(container);
  }

  /** Returns ADLS path. */
  public String path() {
    return path;
  }
}

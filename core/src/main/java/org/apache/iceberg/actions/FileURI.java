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
package org.apache.iceberg.actions;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

/**
 * A normalized file identifier that also retains its original URI string.
 *
 * <p>Use {@link FileIdentifier} when only location matching is required so that the original URI is
 * not retained or transferred unnecessarily.
 */
public class FileURI extends FileIdentifier {

  private String uriAsString;

  public FileURI(String scheme, String authority, String path, String uriAsString) {
    super(scheme, authority, path);
    this.uriAsString = uriAsString;
  }

  public FileURI(URI uri, Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
    super(uri, equalSchemes, equalAuthorities);
    this.uriAsString = uri.toString();
  }

  public FileURI() {}

  public String getUriAsString() {
    return uriAsString;
  }

  public void setUriAsString(String uriAsString) {
    this.uriAsString = uriAsString;
  }

  public boolean schemeMatch(FileURI another) {
    return super.schemeMatch(another);
  }

  public boolean authorityMatch(FileURI another) {
    return super.authorityMatch(another);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("scheme", getScheme())
        .add("authority", getAuthority())
        .add("path", getPath())
        .add("uriAsString", uriAsString)
        .toString();
  }
}

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
package org.apache.iceberg.io;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Extension for {@link FileIO} implementations that can obtain pre-signed URLs for reading their
 * locations.
 *
 * <p>The URLs are signed for GET. They are not locations and are not stored in table metadata. The
 * signer decides which locations it signs.
 */
public interface SupportsPreSigning {

  /**
   * Obtains a pre-signed URL for reading each of {@code locations}.
   *
   * @param locations native locations this FileIO handles
   * @return one URL per distinct location, keyed by location
   */
  Map<String, URI> preSign(Collection<String> locations);

  default URI preSign(String location) {
    return preSign(List.of(location)).get(location);
  }
}

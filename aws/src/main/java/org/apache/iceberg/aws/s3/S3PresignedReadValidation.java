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
package org.apache.iceberg.aws.s3;

import java.net.URI;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;

/**
 * Restricts catalog-vended pre-signed reads to HTTPS URLs on operator-trusted hosts, before {@link
 * S3FileIO} fetches them over HTTP with no S3 credentials. This bounds a compromised or malicious
 * catalog to naming files on the expected storage hosts rather than turning the reader into a
 * general-purpose fetcher of arbitrary URLs.
 */
class S3PresignedReadValidation {
  private S3PresignedReadValidation() {}

  /**
   * Ensures {@code url} uses HTTPS and its host equals, or is a dotted subdomain of, one of {@code
   * allowedHostSuffixes}. Error messages carry the host only, never the full URL, since a
   * pre-signed URL's query string is a bearer secret.
   *
   * @param url an HTTP(S) URL, as classified by {@link
   *     org.apache.iceberg.io.http.HttpUrlHelper#isHttpUrl(String)}
   * @param allowedHostSuffixes lower-case host suffixes permitted for pre-signed reads
   * @throws ValidationException if the scheme is not HTTPS, the URL has no host, or the host is not
   *     allow-listed
   */
  static void checkTrustedHttpsUrl(String url, Set<String> allowedHostSuffixes) {
    URI uri;
    try {
      uri = URI.create(url);
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Cannot read pre-signed URL: malformed URL");
    }

    ValidationException.check(
        "https".equalsIgnoreCase(uri.getScheme()),
        "Cannot read pre-signed URL over %s: only https is allowed",
        uri.getScheme());

    String host = uri.getHost();
    ValidationException.check(host != null, "Cannot read pre-signed URL: URL has no host");

    String normalized = host.toLowerCase(Locale.ROOT);
    boolean trusted =
        allowedHostSuffixes.stream()
            .anyMatch(suffix -> normalized.equals(suffix) || normalized.endsWith("." + suffix));
    ValidationException.check(
        trusted,
        "Cannot read pre-signed URL: host %s is not allowed (configure %s)",
        host,
        S3FileIOProperties.PRESIGNED_READ_ALLOWED_HOSTS);
  }
}

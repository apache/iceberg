/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class LocationUtils {

  private LocationUtils() {
  }

  private static final Set<String> VALID_SCHEME = ImmutableSet.of("ecs", "s3", "s3a", "s3n");

  public static String toString(String bucket, String name) {
    return String.format("ecs://%s/%s", bucket, name);
  }

  public static Optional<EcsURI> parseLocation(String location) {
    URI uri = URI.create(location);
    if (!VALID_SCHEME.contains(uri.getScheme().toLowerCase())) {
      return Optional.empty();
    }

    String bucket = uri.getHost();
    String name = uri.getPath().replaceAll("^/*", "");
    return Optional.of(new EcsURI(bucket, name));
  }

  public static EcsURI checkAndParseLocation(String location) {
    return parseLocation(location).orElseThrow(() -> new ValidationException("Invalid ecs location: %s", location));
  }
}

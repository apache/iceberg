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

package org.apache.iceberg.exceptions;

import com.google.errorprone.annotations.FormatMethod;

/**
 * Exception for a failure to confirm either affirmatively or negatively that a commit was applied. The client
 * cannot take any further action without possibly corrupting the table.
 */
public class CommitStateUnknownException extends RuntimeException {

  private static final String commonInfo =
      "\nCannot determine whether the commit was successful or not, the underlying data files may or " +
      "may not be needed. Manual intervention via the Remove Orphan Files Action can remove these " +
      "files when a connection to the Catalog can be re-established if the commit was actually unsuccessful." +
      "Please check to see whether or not your commit was successful when the catalog is again reachable." +
      "At this time no files will be deleted at this time including possibly unused manifest lists.";

  @FormatMethod
  public CommitStateUnknownException(String message, Object... args) {
    super(String.format(message, args) + commonInfo);
  }

  @FormatMethod
  public CommitStateUnknownException(Throwable cause, String message, Object... args) {
    super(String.format(message, args) + commonInfo, cause);
  }
}

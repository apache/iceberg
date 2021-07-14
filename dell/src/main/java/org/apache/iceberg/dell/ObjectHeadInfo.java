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

import java.util.Map;

/**
 * object head info
 * <p>
 * it's the basic info of an object in the object storage.
 */
public interface ObjectHeadInfo {

  /**
   * content length of object
   *
   * @return length in bytes
   */
  long getContentLength();

  /**
   * eTag of object
   *
   * @return eTag
   */
  String getETag();

  /**
   * user metadata of object
   *
   * @return user metadata
   */
  Map<String, String> getUserMetadata();
}

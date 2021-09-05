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

package org.apache.iceberg.dell.impl;

import java.util.List;
import org.apache.iceberg.dell.ObjectBaseKey;
import org.apache.iceberg.dell.ObjectKeys;

/**
 * The implementation of {@link ObjectKeys}
 */
public class ObjectKeysImpl implements ObjectKeys {

  private final ObjectBaseKey baseKey;

  /**
   * A cached lazy results of {@link #baseKeyParts()}
   */
  private volatile List<String> lazyBaseKeyParts;

  public ObjectKeysImpl(ObjectBaseKey baseKey) {
    this.baseKey = baseKey;
  }

  @Override
  public ObjectBaseKey baseKey() {
    return baseKey;
  }

  /**
   * Use field to cache result parts.
   */
  @Override
  public List<String> baseKeyParts() {
    // code isn't full thread safe. but creating instances in multiple times is fine
    if (lazyBaseKeyParts == null) {
      lazyBaseKeyParts = ObjectKeys.super.baseKeyParts();
    }
    return lazyBaseKeyParts;
  }
}

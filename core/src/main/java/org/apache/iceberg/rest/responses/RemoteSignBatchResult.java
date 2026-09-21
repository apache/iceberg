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
package org.apache.iceberg.rest.responses;

import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.SignStatus;
import org.immutables.value.Value;

/** A completed result carries the signed URI and headers; a failed result carries the error. */
@Value.Immutable
public interface RemoteSignBatchResult {
  SignStatus status();

  @Nullable
  URI uri();

  Map<String, List<String>> headers();

  @Nullable
  ErrorResponse error();

  @Value.Check
  default void check() {
    switch (status()) {
      case COMPLETED:
        if (null == uri()) {
          throw new IllegalArgumentException("Invalid completed result: no uri");
        }

        break;
      case FAILED:
        if (null == error()) {
          throw new IllegalArgumentException("Invalid failed result: no error");
        }

        if (null != uri()) {
          throw new IllegalArgumentException("Invalid failed result: carries a signature");
        }

        break;
    }
  }
}

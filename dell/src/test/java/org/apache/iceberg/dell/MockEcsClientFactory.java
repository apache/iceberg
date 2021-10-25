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

import com.emc.object.s3.S3Client;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

import static org.mockito.Mockito.mock;

public interface MockEcsClientFactory {

  Map<String, String> MOCK_ECS_CLIENT_PROPERTIES = ImmutableMap.of(
      EcsClientProperties.ECS_CLIENT_FACTORY,
      MockEcsClientFactory.class.getName() + "#create"
  );

  static S3Client create(Map<String, String> properties) {
    return mock(S3Client.class);
  }
}

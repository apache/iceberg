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

/**
 * Property constants of catalog
 */
public interface EcsClientProperties {

  /**
   * Access key id
   */
  String ACCESS_KEY_ID = "ecs.s3.access.key.id";

  /**
   * Secret access key
   */
  String SECRET_ACCESS_KEY = "ecs.s3.secret.access.key";

  /**
   * S3 endpoint
   */
  String ENDPOINT = "ecs.s3.endpoint";

  /**
   * Factory method of {@link S3Client}.
   * <p>
   * The method should be static and use format like: "org.apache.iceberg.dell.EcsClientFactory#create"
   * <p>
   * The method must have only one parameter. And return exact {@link S3Client} type.
   */
  String ECS_CLIENT_FACTORY = "ecs.client.factory";
}

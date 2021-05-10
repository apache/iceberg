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

package org.apache.iceberg.dell.emc.ecs;

import java.util.Map;

/**
 * property constants of catalog
 */
public interface EcsCatalogProperties {

    /**
     * static access key id
     */
    String ACCESS_KEY_ID = "s3.access.key.id";

    /**
     * static secret access key
     */
    String SECRET_ACCESS_KEY = "s3.secret.access.key";

    /**
     * s3 endpoint
     */
    String ENDPOINT = "s3.endpoint";

    /**
     * s3 region
     */
    String REGION = "s3.region";

    /**
     * base key which is like "bucket/key"
     * <p>
     * In current version, the bucket of base must be present.
     * <p>
     * In future, we'll support list buckets in catalog
     */
    String BASE_KEY = "s3.base.key";

    /**
     * get object base key from properties
     *
     * @param properties is property
     * @return instance of ObjectBaseKey
     */
    static ObjectBaseKey getObjectBaseKey(Map<String, String> properties) {
        String baseKey = properties.get(BASE_KEY);
        if (baseKey == null) {
            throw new IllegalArgumentException(String.format("missing property %s", BASE_KEY));
        }
        String[] baseKeySplits = baseKey.split(ObjectKeys.DELIMITER, 2);
        if (baseKeySplits.length == 1) {
            return new ObjectBaseKey(baseKeySplits[0], null);
        } else {
            return new ObjectBaseKey(baseKeySplits[0], baseKeySplits[1]);
        }
    }
}

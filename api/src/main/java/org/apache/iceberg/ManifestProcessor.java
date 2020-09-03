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

package org.apache.iceberg;

import java.io.Serializable;
import java.util.function.BiFunction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

public abstract class ManifestProcessor implements Serializable {
    public abstract <T extends ContentFile<T>> Iterable<CloseableIterable<ManifestEntry<T>>> readManifests(final Iterable<ManifestFile> fromIterable,
        BiFunction<ManifestFile, FileIO, CloseableIterable<ManifestEntry<T>>> reader);

    /**
     * A Helper interface for making lambdas transform into the correct type for the ManfiestProcessor
     * @param <T> The ManifestEntry Type being read from Manifest files
     */
    public interface Func<T extends ContentFile<T>> extends BiFunction<ManifestFile, FileIO,
        CloseableIterable<ManifestEntry<T>>>, Serializable {}

}

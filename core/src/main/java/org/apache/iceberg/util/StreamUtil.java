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

package org.apache.iceberg.util;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtil {

  private StreamUtil() {
  }

  public static <T> Stream<T> streamOf(Iterator<T> iterator) {
    Iterable<T> iterable = () -> iterator;
    return streamOf(iterable);
  }

  public static <T> Stream<T> streamOf(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}

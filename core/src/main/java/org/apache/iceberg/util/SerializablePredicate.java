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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

  @Override
  default SerializablePredicate<T> and(Predicate<? super T> other) {
    Objects.requireNonNull(other);
    Preconditions.checkArgument(other instanceof SerializablePredicate);
    return (T x) -> this.test(x) && other.test(x);
  }
}
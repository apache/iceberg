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

package org.apache.iceberg.flink.sink;

import java.io.Serializable;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/**
 * Context object with optional arguments for a Flink sink.
 */
class SinkContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final ConfigOption<String> EQUALITY_FIELD_COLUMNS =
      ConfigOptions.key("equality-field-columns").stringType().defaultValue("");

  private final String equalityFieldColumns;

  private SinkContext(String equalityFieldColumns) {
    this.equalityFieldColumns = equalityFieldColumns;
  }

  String equalityFieldColumns() {
    return equalityFieldColumns;
  }


  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private String equalityFieldColumns = EQUALITY_FIELD_COLUMNS.defaultValue();

    private Builder() {
    }

    Builder equalityFieldColumns(String newEqualityFieldColumns) {
      this.equalityFieldColumns = newEqualityFieldColumns;
      return this;
    }

    Builder fromProperties(Map<String, String> properties) {
      if (properties == null) {
        return this;
      }
      Configuration config = new Configuration();
      properties.forEach(config::setString);

      return this.equalityFieldColumns(config.get(EQUALITY_FIELD_COLUMNS));
    }

    public SinkContext build() {
      return new SinkContext(equalityFieldColumns);
    }
  }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

public enum ArrayEncoding {
  ARRAY("array"),
  DOCUMENT("document");

  private final String value;

  ArrayEncoding(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Determine if the supplied value is one of the predefined options.
   *
   * @param value the configuration property value; may not be null
   * @return the matching option, or null if no match is found
   */
  public static ArrayEncoding parse(String value) {
    if (value == null) {
      return null;
    }
    value = value.trim();
    for (ArrayEncoding option : ArrayEncoding.values()) {
      if (option.getValue().equalsIgnoreCase(value)) {
        return option;
      }
    }
    return null;
  }

  /**
   * Determine if the supplied value is one of the predefined options.
   *
   * @param value the configuration property value; may not be null
   * @param defaultValue the default value; may be null
   * @return the matching option, or null if no match is found and the non-null default is invalid
   */
  public static ArrayEncoding parse(String value, String defaultValue) {
    ArrayEncoding mode = parse(value);
    if (mode == null && defaultValue != null) {
      mode = parse(defaultValue);
    }
    return mode;
  }
}

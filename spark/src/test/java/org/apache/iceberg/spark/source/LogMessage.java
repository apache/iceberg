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

package org.apache.iceberg.spark.source;

import java.util.concurrent.atomic.AtomicInteger;

public class LogMessage {
  private static AtomicInteger idCounter = new AtomicInteger(0);

  static LogMessage debug(String date, String message) {
    return new LogMessage(idCounter.getAndIncrement(), date, "DEBUG", message);
  }

  static LogMessage info(String date, String message) {
    return new LogMessage(idCounter.getAndIncrement(), date, "INFO", message);
  }

  static LogMessage error(String date, String message) {
    return new LogMessage(idCounter.getAndIncrement(), date, "ERROR", message);
  }

  static LogMessage warn(String date, String message) {
    return new LogMessage(idCounter.getAndIncrement(), date, "WARN", message);
  }

  private int id;
  private String date;
  private String level;
  private String message;

  private LogMessage(int id, String date, String level, String message) {
    this.id = id;
    this.date = date;
    this.level = level;
    this.message = message;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}

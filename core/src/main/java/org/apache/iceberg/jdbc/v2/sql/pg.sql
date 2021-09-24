/*
 *    Copyright 2021 Two Sigma Open Source, LLC
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

CREATE TABLE IF NOT EXISTS Namespaces (
  namespace_name VARCHAR(255),
  properties VARCHAR(65535),
  location VARCHAR(511),
  owner_group VARCHAR(127),
  created_by_user VARCHAR(127),
  last_updated_by_user VARCHAR(127),
  last_updated_timestamp TIMESTAMP,
  PRIMARY KEY(namespace_name)
);

CREATE TABLE IF NOT EXISTS Tables (
  namespace_name VARCHAR(255),
  table_name VARCHAR(511),
  table_pointer VARCHAR(2047),
  created_timestamp TIMESTAMP,
  last_updated_by_user VARCHAR(127),
  last_updated_timestamp TIMESTAMP,
  PRIMARY KEY(namespace_name, table_name)
);
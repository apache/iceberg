<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->
# Vendor packages

Some packages we want to maintain in the repository itself, because there is no good 3rd party alternative.

## FB303 Thrift client

fb303 is a base Thrift service and a common set of functionality for querying stats, options, and other information from a service.

```bash
rm -f /tmp/fb303.thrift
rm -rf fb303
curl -s https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift > /tmp/fb303.thrift
rm -rf /tmp/gen-py/
thrift -gen py -o /tmp/ /tmp/fb303.thrift
mv /tmp/gen-py/fb303 fb303
```

# Hive Metastore Thrift definition

The thrift definition require the fb303 service as a dependency

```bash
rm -rf /tmp/hive
mkdir -p /tmp/hive/share/fb303/if/
curl -s https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift > /tmp/hive/share/fb303/if/fb303.thrift
curl -s https://raw.githubusercontent.com/apache/hive/master/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift > /tmp/hive/hive_metastore.thrift
thrift -gen py -o /tmp/hive /tmp/hive/hive_metastore.thrift
mv /tmp/hive/gen-py/hive_metastore hive_metastore
```

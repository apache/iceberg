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

# Roadmap Overview

This roadmap outlines projects that the Iceberg community is working on, their priority, and a rough size estimate.
This is based on the latest [community priority discussion](https://lists.apache.org/thread.html/r84e80216c259c81f824c6971504c321cd8c785774c489d52d4fc123f%40%3Cdev.iceberg.apache.org%3E).
Each high-level item links to a Github project board that tracks the current status.
Related design docs will be linked on the planning boards.

# Priority 1

* API: [Iceberg 1.0.0](https://github.com/apache/iceberg/projects/3) [medium]
* Spark: [Merge-on-read plans](https://github.com/apache/iceberg/projects/11) [large]
* Maintenance: [Delete file compaction](https://github.com/apache/iceberg/projects/10) [medium]
* Flink: [Upgrade to 1.13.2](https://github.com/apache/iceberg/projects/12) (document compatibility) [medium]
* Python: [Pythonic refactor](https://github.com/apache/iceberg/projects/7) [medium]

# Priority 2

* ORC: [Support delete files stored as ORC](https://github.com/apache/iceberg/projects/13) [small]
* Spark: [DSv2 streaming improvements](https://github.com/apache/iceberg/projects/2) [small]
* Flink: [Inline file compaction](https://github.com/apache/iceberg/projects/14) [small]
* Flink: [Support UPSERT](https://github.com/apache/iceberg/projects/15) [small]
* Flink: [FLIP-27 based Iceberg source](https://github.com/apache/iceberg/projects/23) [large]
* Views: [Spec](https://github.com/apache/iceberg/projects/6) [medium]
* Spec: [Z-ordering / Space-filling curves](https://github.com/apache/iceberg/projects/16) [medium]
* Spec: [Snapshot tagging and branching](https://github.com/apache/iceberg/projects/4) [small]
* Spec: [Secondary indexes](https://github.com/apache/iceberg/projects/17) [large]
* Spec v3: [Encryption](https://github.com/apache/iceberg/projects/5) [large]
* Spec v3: [Relative paths](https://github.com/apache/iceberg/projects/18) [large]
* Spec v3: [Default field values](https://github.com/apache/iceberg/projects/19) [medium]

# Priority 3

* Docs: [versioned docs](https://github.com/apache/iceberg/projects/20) [medium]
* IO: [Support Aliyun OSS/DLF](https://github.com/apache/iceberg/projects/21) [medium]
* IO: [Support Dell ECS](https://github.com/apache/iceberg/projects/22) [medium]

# External

* PrestoDB: [Iceberg PrestoDB Connector](https://github.com/apache/iceberg/projects/9)
* Trino: [Iceberg Trino Connector](https://github.com/apache/iceberg/projects/8)

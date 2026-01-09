---
date: 2025-11-20
title: Row-Level Updates and Deletes at Scale
authors:
  - iceberg-contributors
categories:
  - features
  - announcement
---

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

Apache Iceberg's support for row-level updates and deletes continues to improve, making it easier to handle data modifications efficiently.

<!-- more -->

## Position Deletes Optimization

Recent optimizations to position delete handling have reduced the overhead of tracking deleted rows:

- **Compact Delete Files**: Smaller delete files that consume less storage
- **Faster Scans**: Improved scan performance when reading tables with deletes
- **Better Compaction**: Smarter strategies for rewriting data files

## Copy-on-Write Mode Improvements

For workloads that prefer copy-on-write semantics, we've optimized the rewrite process:

1. **Vectorized Processing**: Up to 2x faster for large updates
2. **Memory Efficiency**: Reduced memory footprint during rewrites
3. **Transactional Safety**: Enhanced conflict detection and resolution

## Real-World Impact

Production users have reported:

- 50% reduction in delete overhead
- 30% faster merge operations
- Significant cost savings on cloud storage

Try it out and let us know your experience in the [community forums](/community/).

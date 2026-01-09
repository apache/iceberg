---
date: 2025-11-20
authors:
  - iceberg-contributors
categories:
  - features
  - announcement
---

# Row-Level Updates and Deletes at Scale

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

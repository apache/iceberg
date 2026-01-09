---
date: 2025-12-15
authors:
  - iceberg-contributors
categories:
  - announcement
  - performance
---

# Catalog Performance Improvements

We're excited to announce significant performance improvements to Apache Iceberg's catalog operations in the latest release.

<!-- more -->

## Enhanced Metadata Caching

The new metadata caching layer reduces the number of round trips to the catalog, improving query planning performance by up to 40% in production workloads.

Key improvements include:

- **Smart Cache Invalidation**: Automatic cache invalidation based on metadata changes
- **Reduced Latency**: Up to 40% faster query planning
- **Lower Costs**: Fewer API calls to cloud storage services

## Parallel Metadata Loading

Catalog operations now support parallel metadata loading, dramatically reducing the time needed to load table metadata for large tables.

## What's Next

We're continuing to invest in catalog performance and reliability. Stay tuned for more updates!

Learn more in our [documentation](/docs/latest/) or join the discussion in our [community](/community/).

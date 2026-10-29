---
date: 2026-10-29
title: Apache Iceberg Terraform Provider 0.2.0 Release
slug: apache-iceberg-terraform-provider-0.2.0-release
authors:
  - iceberg-pmc
categories:
  - release
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

The Apache Iceberg community is pleased to announce version 0.2.0 of the [Apache Iceberg Terraform provider](https://github.com/apache/terraform-provider-iceberg).

This is the first release published to both the [Terraform Registry](https://registry.terraform.io/providers/apache/iceberg/0.2.0) and the [OpenTofu Registry](https://search.opentofu.org/provider/apache/iceberg/v0.2.0). It covers about two months of development since the 0.1.0 release in September 2026. See the [full changelog](https://github.com/apache/terraform-provider-iceberg/compare/v0.1.0...v0.2.0) for the complete list of changes.

The Terraform provider lets you manage Iceberg namespaces and tables in any [Iceberg REST catalog](https://iceberg.apache.org/rest-catalog-spec/) with [Terraform](https://www.terraform.io/) or [OpenTofu](https://opentofu.org/). It is built on [iceberg-go](https://github.com/apache/iceberg-go)

<!-- more -->

## Getting Started

Add the provider to your configuration and point it at your REST catalog. The same `source` works with both Terraform and OpenTofu:

```terraform
terraform {
  required_providers {
    iceberg = {
      source  = "apache/iceberg"
      version = "~> 0.2"
    }
  }
}

provider "iceberg" {
  catalog_uri = "https://catalog.example.com"
  warehouse   = "analytics"

  auth = {
    oauth2 = {
      credential = var.iceberg_credential # "client_id:client_secret"
    }
  }
}

resource "iceberg_namespace" "analytics" {
  name = ["analytics"]

  user_properties = {
    owner = "data-platform"
  }
}

resource "iceberg_table" "events" {
  namespace = iceberg_namespace.analytics.name
  name      = "events"

  schema = {
    fields = [
      { name = "event_id", type = "long", required = true },
      { name = "event_type", type = "string", required = true },
      { name = "event_time", type = "timestamp", required = true },
    ]
  }

  user_properties = {
    "write.format.default" = "parquet"
  }
}
```

Running `terraform apply` creates the namespace and table in the catalog. Later changes to the schema, partition spec, sort order, or table properties evolve the table in place rather than recreating it. Existing tables can be imported with `terraform import`. See the [provider documentation](https://registry.terraform.io/providers/apache/iceberg/0.2.0/docs) for the full reference.

## Release Highlights

### New Data Sources

A new [`iceberg_tables` data source](https://github.com/apache/terraform-provider-iceberg/pull/71) lists the tables in a namespace. Its output can drive `for_each`, validate namespace contents, or wire up downstream resources:

```terraform
data "iceberg_tables" "raw" {
  namespace = ["analytics", "raw"]
}

output "raw_tables" {
  value = data.iceberg_tables.raw.identifiers
}
```

### Authentication

0.1.0 supported a static bearer token and custom HTTP headers. This release adds two authentication methods under a new `auth` block:

- **OAuth2 client credentials**: [`auth.oauth2`](https://github.com/apache/terraform-provider-iceberg/pull/127) fetches and refreshes tokens automatically using iceberg-go's AuthManager
- **AWS SigV4**: [`auth.sigv4`](https://github.com/apache/terraform-provider-iceberg/pull/118) signs requests for catalogs that require it using the support from iceberg-go.

### Safer Concurrent Table Updates

Commits [now include the full set of Iceberg update requirements](https://github.com/apache/terraform-provider-iceberg/pull/134), so the catalog rejects a commit that was planned against stale metadata.

### Spec Conformance

- **Reserved field IDs**: [Field IDs in Iceberg's reserved range are rejected](https://github.com/apache/terraform-provider-iceberg/pull/124) by the provider instead of relying on the catalog to catch them
- **Multi-source partition fields**: [Every source ID of a v3 multi-source partition field is preserved](https://github.com/apache/terraform-provider-iceberg/pull/145) when reading table state, instead of only the first

### Upstreaming to iceberg-go

The goal is for the provider to be a thin layer over iceberg-go, so logic written for the provider is [contributed back upstream](https://github.com/apache/terraform-provider-iceberg/issues/143) where other iceberg-go users can benefit from it. So far this includes:

- [Schema compatibility checks](https://github.com/apache/iceberg-go/pull/2073)
- [`AssignFreshSchemaIDsWithBase`](https://github.com/apache/iceberg-go/pull/2076) for assigning field IDs to an evolved schema while preserving existing ones
- [Update requirements](https://github.com/apache/iceberg-go/pull/2220) for building optimistic-concurrency checks on table commits

The provider will switch to the upstream implementations in a future iceberg-go release.

### Documentation and Release Process

- [Complete provider configuration docs and examples](https://github.com/apache/terraform-provider-iceberg/pull/106), including catalog URI, REST prefix, and warehouse semantics
- Release artifacts are [signed with the Iceberg project key in CI](https://github.com/apache/terraform-provider-iceberg/pull/133), as required for publishing to the Terraform and OpenTofu registries

## Getting Involved

The Apache Iceberg Terraform provider welcomes all contributions! We use GitHub [issues](https://github.com/apache/terraform-provider-iceberg/issues) for tracking work and the [Apache Iceberg Community Slack](https://iceberg.apache.org/community/#slack) for discussions.

For more information, visit the [terraform-provider-iceberg repository](https://github.com/apache/terraform-provider-iceberg).

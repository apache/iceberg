<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Security Threat Model

This document describes Apache Iceberg's detailed security threat model for
maintainers and automated security triage.

It complements the shorter public-facing security model in
[`site/docs/security.md`](site/docs/security.md) by making Iceberg's trust
assumptions, security boundaries, and recurring non-security bug classes more
explicit.

## Purpose

Apache Iceberg is often deployed as a library and integration layer inside
larger systems that provide their own authentication, authorization, and
credential management. Because of that deployment model, many bug classes that
look security-relevant in the abstract are not actually security
vulnerabilities in Iceberg itself.

This model is intended to answer:

- what Iceberg generally treats as a security vulnerability
- what Iceberg generally treats as correctness, hardening, or deployment work
- which boundaries are primarily owned by Iceberg versus the surrounding
  catalog, engine, or service
- which issue classes should be downgraded by default by scanners

## Scope

This model is scoped to the Apache Iceberg project itself:

- the table format implementation
- client libraries
- engine integrations
- catalog-related components shipped in the Iceberg repository

It is not a general threat model for every deployment that embeds Iceberg.

In particular, it does not attempt to define the complete security model for:

- query engines or applications that embed Iceberg
- storage-level authorization enforced outside Iceberg

## Security Goals

Iceberg should:

- avoid exposing secrets or delegated credentials to principals that were not
  already trusted with them
- avoid creating new unauthorized capabilities in Iceberg-owned components or
  integrations
- avoid violating trust boundaries that Iceberg itself owns, such as leaking
  signer, auth, or credential-bearing state across catalog or session
  boundaries in the same process

Iceberg does not aim to be the primary enforcement point for:

- user-to-user authorization inside a query engine
- storage-level authorization
- service-side credential scoping performed by an external catalog

## Roles

### Operator

The operator deploys and configures the catalog, metastore, REST service,
engine, and storage integration around Iceberg. This role is trusted to choose
endpoints, warehouses, and storage integrations, configure credentials, and
decide which users may create tables, read tables, or invoke maintenance
actions.

### Catalog control plane

The catalog control plane is responsible for resolving tables and supplying
metadata, locations, configuration, and delegated credentials to Iceberg. This
role may be implemented by a REST catalog server, a metastore-backed catalog,
or another catalog implementation. Regardless of how it is implemented, it
should not expose secrets to unintended principals or leak credential-bearing
state across unintended boundaries.

Iceberg assumes a trusted catalog or metastore, which is outside its primary
security boundary.

### REST catalog server

In REST deployments, part of the catalog control plane is implemented by a
server that returns metadata, configuration, and possibly delegated
credentials to the client. This server is generally treated as a trusted
control-plane component.

### REST catalog client

In REST deployments, the client-side catalog object consumes server-provided
metadata, configuration, and credentials. Where the client and server are
meaningfully distinct, client-side bugs in routing, caching, or reuse may
still be security-relevant. This is especially true when the Iceberg-owned
client implementation leaks credential-bearing state across catalog, session,
or principal boundaries it is expected to preserve.

### Engine or embedding application

Query engines and applications may expose only a subset of Iceberg
capabilities to users. They are responsible for their own user-facing
authorization boundaries unless Iceberg explicitly documents otherwise.

### Table writer or maintainer

This role may already have legitimate power to write or replace table
metadata, write or delete files, choose paths under an allowed warehouse or
table location, and invoke destructive maintenance operations. If a report
only shows a new way to achieve the same effect this role can already cause
legitimately, it is usually not a security issue in Iceberg.

## Trust Boundaries

### Boundary 1: operator-trusted configuration

The following are generally treated as trusted operator or deployment inputs:

- catalog properties
- endpoint configuration
- warehouse and storage roots
- metastore wiring
- REST catalog server configuration

If a report depends on the attacker controlling those values directly, it is
usually not a vulnerability in Iceberg itself.

### Boundary 2: catalog-supplied metadata

Iceberg often accepts metadata locations, table properties, namespace
properties, and related control-plane information from a catalog or
metastore. By default, Iceberg treats those sources as trusted.

This means a malicious catalog supplying incorrect or malicious metadata is
usually not an Iceberg vulnerability by itself.

### Boundary 3: REST catalog server-supplied configuration and delegated storage access

In REST deployments, Iceberg may also accept service endpoints,
configuration, and delegated storage access from the REST catalog server. By
default, those are treated as trusted control-plane inputs unless Iceberg
explicitly documents a stronger guarantee.

This means a malicious REST catalog server sending dangerous endpoints is
usually not an Iceberg vulnerability by itself. It also means many client-side
credential-selection bugs are often correctness or specification issues rather
than security boundary failures.

The major exception is secret exposure. If Iceberg surfaces credentials or
secrets to a new audience that was not already trusted with them, that is
security-relevant.

### Boundary 4: storage-level authorization

Object store permissions are enforced by the storage provider and the
credentials the surrounding deployment chooses to hand to Iceberg. Iceberg is
not the root authority for bucket- or object-level authorization.

Reports that depend primarily on over-broad IAM or permissive storage ACLs are
usually deployment-sensitive rather than product-security issues in Iceberg.

### Boundary 5: engine-level user authorization

Iceberg integrations may surface data and operations through a query engine or
application, but Iceberg is not a complete user-authorization framework for
those systems.

## In-Scope Security Vulnerabilities

The following categories are generally security-relevant in Iceberg when the
report is credible and reproducible.

### 1. Secret or credential disclosure to a new audience

Examples include:

- catalog secrets exposed through a user-visible engine surface
- one catalog's credentials or auth state leaking into another catalog or
  session

### 2. Iceberg-owned trust-boundary violations

Security issues exist when Iceberg itself is expected to separate catalogs,
principals, or sessions and fails to do so.

Examples include:

- process-global signer or auth state crossing catalog instances
- secret-bearing state from one principal reused for another principal within
  an Iceberg-owned boundary

## Usually Out of Scope or Non-Security by Default

These categories may still be real bugs worth fixing, but they are not usually
security vulnerabilities in Iceberg itself.

### 1. Correctness bugs

Examples:

- wrong byte offsets
- stale or corrupted decoded values
- ambiguous prefix matching
- race conditions or logic bugs that do not create a new trust-boundary
  violation

### 2. Parser hardening and malformed-input robustness

Malformed-input crashes, raw runtime exceptions, and memory amplification are
usually treated as robustness or hardening work rather than security issues in
Iceberg itself.

### 3. Malicious catalog, metastore, or external service scenarios

Reports that require a malicious catalog, metastore, or other external service
are usually outside Iceberg's primary security boundary.

### 4. Equivalent-harm reports

If the actor already has a legitimate capability that can cause the same harm,
the new path is usually not a security issue. This often applies to writers or
maintainers who already control metadata layout, file layout, or destructive
maintenance operations.

## Scanner Calibration Rules

A scanner targeting Iceberg should treat a finding as higher-confidence only if
it plausibly shows one of the following:

- exposure of a secret or delegated credential to a new audience
- creation of a new unauthorized capability in an Iceberg-owned component
- violation of an Iceberg-owned trust boundary rather than a surrounding
  catalog, engine, service, or operator boundary

A finding should be downgraded or rejected by default if it instead depends
primarily on:

- malformed-input robustness or denial-of-service behavior
- a malicious catalog, metastore, or external service
- a principal that already has equivalent power through legitimate write or
  maintenance capabilities

# Business Requirements Document (BRD)

**Project Title:** Enterprise Banking Data Lakehouse & AI/BI Analytics Enablement  
**Platform:** Databricks (Unity Catalog, Delta Lake, Databricks Asset Bundles, Genie, AI/BI)  
**Document Version:** 1.1  
**Status:** Draft for stakeholder review  
**Target Architecture:** Medallion Architecture (Bronze / Silver / Gold) with Data Mesh Governance

## 1. Executive Summary & Business Drivers

### 1.1 Business Context

Modern retail banking operations require historical visibility into customer spending behavior, cross-channel transaction patterns, and multi-year financial trends. Legacy BI systems can suffer from performance bottlenecks, fragmented governance, and delayed reporting cycles. This proof of concept (POC) will demonstrate a governed Databricks lakehouse workflow using an agreed sample dataset. Micro-batch ingestion is in scope; a real-time service level is not implied.

### 1.2 Strategic Objectives

- **Unified Lakehouse Architecture:** Centralize JSON transaction events and Parquet customer profiles in a Delta Lake foundation.
- **Multi-Year Historical Analysis:** Process the stated transaction horizon of January 2024 through October 2026 (approximately 100,000 records) and 500 customer profiles to demonstrate year-over-year (YoY) analysis. The 2026 period is partial and must be compared on an aligned date range.
- **Automated Medallion Pipelines:** Build raw-to-aggregated Delta processing with schema evolution, deduplication, and automated data-quality checks.
- **Data Mesh & Unified Governance:** Demonstrate domain-oriented ownership and fine-grained access through Unity Catalog catalogs, schemas, tables, and approved policies.
- **Self-Service AI/BI & Conversational Analytics:** Demonstrate Databricks AI/BI dashboards and a Genie Space over governed, curated data.

## 2. Ingestion & Source Data Specifications

Inputs are expected in an approved landing location represented by a Unity Catalog Volume. Exact paths, naming conventions, permissions, and retention are deployment decisions.

| Source system | Format | Ingestion pattern | Primary key | Dataset horizon / scale | Attributes and description |
| --- | --- | --- | --- | --- | --- |
| Transaction Stream | JSON | Micro-batch with Auto Loader | `tx_id` | January 2024 through October 2026; approximately 100,000 records | `tx_id` (String), `cust_id` (String), `amount` (Double in source), `channel` (String: POS/ATM/WEB/MOBILE), `timestamp` (Timestamp) |
| Customer Master | Parquet | Batch snapshot | `cust_id` | 500 profiles described as active; registered during 2023-2025 | `cust_id` (String), `customer_name` (String), `tier` (String: Bronze/Silver/Gold/Platinum), `country` (String: MY/SG/TH/ID), `registered_date` (String) |

The stated transaction interval is not a complete 36-month period, and October 2026 may be partial. Confirm the precise inclusive/exclusive cutoff and expected record counts. The customer source has no explicit active-status field; the meaning of “active” must be confirmed. No currency field is specified, so Gold-layer monetary totals must not combine or label amounts as a single-currency measure until denomination is established.

## 3. Detailed Medallion Architecture Requirements

### 3.1 Bronze: Raw Ingestion

- Ingest transaction JSON with Auto Loader using a repeatable micro-batch job and managed schema/checkpoint locations approved for the POC.
- Load customer Parquet snapshots as batch inputs and record the source snapshot identifier or ingestion timestamp.
- Persist source records in append-oriented Delta Bronze tables, retaining source payload fidelity where supported.
- Add ingestion metadata such as source file, ingestion timestamp, and run identifier without overwriting source values.
- Capture malformed or unexpected fields using a rescued-data approach and make schema changes observable.
- Make reruns idempotent or document the replay behavior and checkpoint reset procedure.

### 3.2 Silver: Validated and Conformed Data

- Normalize names and types to documented conventions while retaining traceability to Bronze inputs.
- Parse `timestamp` using an agreed timezone convention and parse `registered_date` from String to Date; retain raw values for diagnosis.
- Convert `amount` from source Double to an agreed fixed-precision decimal representation for financial calculations. Confirm scale, rounding, sign convention, and currency before treating it as a monetary measure.
- Validate required identifiers and fields, allowed channel/tier/country values, timestamp parseability, and agreed amount constraints.
- Deduplicate transactions by `tx_id` using a deterministic rule agreed with the data owner. Record duplicate counts and the selected survivor rule.
- Validate transaction-to-customer references. Quarantine or flag unmatched `cust_id` records rather than silently dropping them.
- Separate accepted and rejected records with rule identifiers and run metadata. Do not discard rejected records without an approved retention policy.
- Support additive schema evolution under controlled review; incompatible changes must fail or be quarantined and raise an observable error.

### 3.3 Gold: Business-Ready Data Products

- Publish governed, documented Delta tables or views for analytics, for example a transaction fact and customer dimension.
- Provide agreed aggregates by date, channel, customer tier, and country, subject to the approved use case and privacy review.
- Define business metrics centrally, including transaction count and any amount-based metric after currency and amount semantics are confirmed.
- Support YoY comparisons for 2024, 2025, and 2026 only on comparable periods; label 2026 as year-to-date/partial through the confirmed data cutoff.
- Apply suitable filtering or aggregation to prevent exposure of individual customer details in broad self-service outputs.
- Expose only reviewed Gold products to dashboard and Genie consumers; do not use raw Bronze data for business self-service.

### 3.4 Orchestration and Deployment

- Package jobs, parameters, and environment-specific settings as Databricks Asset Bundles (DABs).
- Demonstrate deployment and execution in a designated non-production POC target. Production targets and promotion approvals are out of scope until defined.
- Keep environment configuration separate from code and store secrets only in approved secret-management facilities.
- Record job status, input/output counts, data-quality outcomes, and failure details for each run.

## 4. Functional Requirements

| ID | Requirement | Priority |
| --- | --- | --- |
| FR-01 | Ingest the approved JSON transaction files through a repeatable Auto Loader micro-batch workflow. | Must |
| FR-02 | Load the approved Parquet customer snapshot and identify each input snapshot. | Must |
| FR-03 | Store both source datasets in governed Delta Bronze tables with ingestion lineage. | Must |
| FR-04 | Produce Silver datasets with documented types, validation, deduplication, and reject handling. | Must |
| FR-05 | Publish Gold datasets for approved banking analytics and aligned-period YoY comparisons. | Must |
| FR-06 | Provide a Databricks AI/BI dashboard using reviewed Gold datasets and approved metric definitions. | Should |
| FR-07 | Provide a Genie Space scoped to approved Gold data, with example questions and verified answers. | Should |
| FR-08 | Deploy the POC workflow through Databricks Asset Bundles to the approved non-production target. | Must |
| FR-09 | Capture run-level operational and data-quality metrics sufficient to explain successful and failed records. | Must |

## 5. Data Quality and Reconciliation

Required checks include:

- Required/non-null `tx_id`, `cust_id`, transaction `timestamp`, and required customer keys.
- Uniqueness of `tx_id` after application of the approved deduplication rule.
- Parse success for timestamps and registration dates.
- Allowed values for channel, tier, and country using the source specification.
- Reconciliation of source, accepted, duplicate, rejected, and Gold record counts per run.
- Referential integrity checks between transactions and customer profiles, with unmatched records measured and surfaced.
- Schema drift detection and a visible outcome for both compatible and incompatible changes.

The POC must report counts and rule failures. Numeric pass thresholds, null tolerances, treatment of negative/zero amounts, duplicate survivor logic, and whether unmatched transactions block a run are TBD pending data-owner approval.

## 6. Governance, Security, and Data Mesh

- Use Unity Catalog as the governance boundary for approved catalogs, schemas, volumes, tables, and permissions.
- Assign a named business/data owner to each data product and document its definition, refresh cadence, classification, and support contact.
- Demonstrate least-privilege role-based access for platform/data engineering, analyst, and business consumer personas. Exact groups and grants are environment-specific and TBD.
- Restrict customer-identifying fields such as `customer_name` to explicitly authorized users. Prefer synthetic or properly de-identified POC data; any identifiable data requires approval from data owners and security/privacy controls before use.
- Keep secrets, tokens, workspace configuration, and customer data out of source control.
- Confirm audit, retention, deletion, residency, encryption, and masking requirements with the organization's control owners.
- Use approved storage credentials and external locations; do not embed cloud credentials or unrestricted storage paths in notebooks or bundles.
- Treat this POC as non-production. No POC output may be used for customer-impacting decisions or claimed as a regulatory report.

## 7. AI/BI and Analytical Requirements

- The dashboard should show approved trends over time, transaction counts by channel, and agreed customer-segment breakdowns.
- Amount-based charts and totals are blocked until currency, sign convention, and aggregation semantics are confirmed.
- YoY visualizations must use aligned date ranges and visibly identify the partial 2026 period.
- Genie must use curated, governed Gold tables/views, include business descriptions and metric definitions, and be tested against a stakeholder-approved set of representative questions.
- Genie responses must not expose restricted customer-level attributes or imply that generated answers are authoritative without verification.
- Dashboard and Genie access must follow Unity Catalog and workspace permissions.

## 8. Non-Functional Requirements

| Area | POC requirement |
| --- | --- |
| Scale | Demonstrate the stated approximate 100,000 transaction records and 500 customer profiles, subject to sample-data reconciliation. |
| Reliability | Make ingestion and transformations rerunnable with documented checkpoint and recovery behavior. |
| Observability | Surface job state, row counts, quality results, schema changes, and actionable error details. |
| Performance | Agree POC-sized runtime targets before acceptance; production throughput and latency SLAs are out of scope. |
| Security | Enforce least privilege and approved data handling; do not commit secrets or identifiable data. |
| Cost | Record selected compute configuration and major cost drivers; production cost estimates require representative workloads. |
| Maintainability | Store code, bundle configuration, and documentation in version control with reviewable changes. |
| Compatibility | Confirm workspace cloud, Databricks Runtime, Unity Catalog availability, and required feature entitlements before implementation. |

## 9. Assumptions and Dependencies

- An authorized Databricks workspace with Unity Catalog, Delta Lake, Auto Loader, Asset Bundles, and AI/BI/Genie features enabled is available.
- The landing Volume, catalog/schema names, storage locations, service identities, and access grants will be provided by the platform owner.
- Sample files are available in the stated formats and are synthetic, public, or approved and de-identified before ingestion.
- The stated 100,000 transaction and 500 customer figures are planning estimates until reconciled with delivered files.
- The business/data owner will confirm definitions and sign conventions before amount-based analysis is published.
- Workspace/cloud vendor, deployment environments, schedule, budget, and named approvers remain TBD.

## 10. Out of Scope

- Production deployment, production SLAs, disaster recovery, high availability, and ongoing operational support.
- Core banking system integration or customer-facing transaction processing.
- Unapproved live customer data, payment card data, or secrets.
- Real-time streaming guarantees; Auto Loader micro-batches do not establish a real-time SLA.
- Credit decisions, fraud detection, customer scoring, automated actions, or regulatory reporting.
- Enterprise-wide Data Mesh rollout or migration of all legacy BI workloads.

## 11. Acceptance Criteria

The POC is ready for stakeholder acceptance when:

1. Approved input data is ingested into Bronze with source and run lineage.
2. Silver processing demonstrates schema handling, agreed deduplication, quality checks, and observable rejected-record handling.
3. Source-to-Silver-to-Gold counts are reconciled, with differences explained by duplicates, rejects, or documented rules.
4. Gold data supports stakeholder-approved queries and an aligned-period YoY view; 2026 is clearly identified as partial.
5. The AI/BI dashboard and Genie Space answer an agreed test set correctly against approved Gold data, subject to feature availability.
6. Unity Catalog permissions are demonstrated for the agreed user personas, including restrictions on customer-identifying fields.
7. The bundle can deploy and run in the designated non-production target with secrets/configuration managed outside source control.
8. Run instructions, data definitions, quality results, cost observations, limitations, and production gaps are documented.

Numerical quality thresholds, runtime targets, dashboard metrics, Genie evaluation questions, and approval authorities must be agreed before formal acceptance.

## 12. Risks and Open Questions

| Item | Impact / required action |
| --- | --- |
| No currency field is specified | Confirm currency and any multi-currency conversion policy before publishing amount totals or trends. |
| No active-status field is specified | Confirm how the 500 “active” profiles are identified or whether the source snapshot itself defines the population. |
| `customer_name` may be identifying information | Confirm classification and approval; use synthetic or de-identified values unless access is explicitly approved. |
| 2026 horizon ends in October | Confirm the exact cutoff and compare only aligned periods for YoY analysis. |
| Source `amount` is Double | Approve decimal precision, scale, rounding, sign convention, and treatment of invalid amounts. |
| Transaction duplicate policy is unspecified | Define which record wins when the same `tx_id` occurs more than once. |
| Databricks feature availability is unknown | Confirm workspace entitlements and cloud-specific support for Unity Catalog, Auto Loader, DAB, Genie, and AI/BI. |
| Data Mesh ownership model is unspecified | Name domain owners, data-product approvers, and support contacts for the POC. |
| Performance, cost, and quality targets are unspecified | Agree measurable thresholds and a POC budget before execution. |

## 13. Stakeholders and Approval

| Role | Responsibility | Name / status |
| --- | --- | --- |
| Business sponsor / product owner | Confirm outcomes, priority, and acceptance. | TBD |
| Banking data owner | Approve source data, field definitions, and quality rules. | TBD |
| Data engineering lead | Own pipeline implementation and technical documentation. | TBD |
| Databricks / cloud platform owner | Provide workspace, storage, identity, catalog, and deployment prerequisites. | TBD |
| Security / privacy / compliance | Review data classification, controls, and permitted use. | TBD |
| Analytics / business users | Validate dashboard metrics and Genie test questions. | TBD |

This version is a proposed POC baseline. It becomes approved only after the required stakeholders confirm the open decisions and acceptance thresholds above.
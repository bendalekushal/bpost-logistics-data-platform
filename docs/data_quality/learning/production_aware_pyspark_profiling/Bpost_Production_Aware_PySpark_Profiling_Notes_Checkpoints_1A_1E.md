# Bpost Enterprise Data Engineering Mentorship
## Production-Aware PySpark Raw-Data Profiling — Session Notes
### Checkpoints 1A–1E

> Project continuation: **Enterprise Logistics Data Engineering & Operations Intelligence Platform**
>
> Current profiling relationship:
>
> `dim_vehicle.vehicle_id` → `fact_telematics_raw.vehicle_id`
>
> Important constraint: the local synthetic dataset is only a development/test representation. All architecture and performance decisions assume production-scale workloads of billions of records, TB/PB historical storage, late-arriving data, duplicates, corrupt records, schema evolution, and incremental processing.

---

# 0. Where We Are in the Project

Completed before this session:

- Project/business scope
- Enterprise 24-table synthetic data model
- Dataset generation and profiling baseline
- Git/GitHub setup
- Decision to move production-oriented profiling from Pandas to PySpark

Current phase:

**Production-aware PySpark raw-data profiling framework**

Current relationship:

```text
dim_vehicle
    |
    | 1
    |
    | N
    ↓
fact_telematics_raw
```

Relevant fields:

```text
dim_vehicle
-----------
vehicle_id        ← Parent / PK

fact_telematics_raw
-------------------
event_id          ← Event/business key
vehicle_id        ← Foreign key
timestamp_utc
latitude
longitude
speed_kmh
odometer_km
ignition_status
```

The current local dataset is approximately 917K rows across 24 tables, but this is not the architecture constraint. Production assumptions are much larger.

---

# 1. Production Workload Model

Current engineering assumptions discussed:

```text
Flow A
10–15 GB every 15 minutes

Flow B
~1 GB every 5 minutes
```

Approximate throughput:

```text
Flow A:
10 GB / 15 min  → 40 GB/hour  → 960 GB/day
15 GB / 15 min  → 60 GB/hour  → 1.44 TB/day

Flow B:
1 GB / 5 min    → 12 GB/hour  → 288 GB/day

Combined:
~1.25–1.73 TB/day
~37–52 TB/month
~456–631 TB/year
```

These are workload assumptions used to test whether a design is production-worthy. They are not a claim about the exact final production dataset.

## Core engineering principle

Do not design around:

```text
153,000 telemetry rows locally
```

Design around:

```text
billions of records
TB/PB historical storage
continuous incremental arrivals
```

The local dataset is the test harness.

---

# 2. Checkpoint 1A — Production Workload Analysis

## Business Problem

A naive profiler might scan the entire historical telemetry dataset every 5–15 minutes.

Example:

```text
500 TB historical telemetry
        ↓
new 12 GB batch arrives
        ↓
scan all 500 TB again
        ↓
run DQ
```

This creates unnecessary:

- I/O
- CPU
- network movement
- execution time
- cluster usage
- operational cost

## Production Principle

**Profile the newly arrived/affected data incrementally.**

Do not repeatedly profile all historical data unless a historical audit is actually required.

---

# 3. Incremental Profiling vs Historical Deep Profiling

## Incremental / Ingestion-Time Profiling

Runs for each newly arrived batch.

Example:

```text
10:00–10:15
12 GB
   ↓
incremental DQ
```

Typical checks:

- schema
- row count
- required fields
- null-like values
- business/event-key checks
- FK validity
- malformed timestamp checks
- numeric/range checks
- duplicate checks

Goal:

> Decide whether the newly arrived data can safely continue downstream.

## Historical / Deep Profiling

Runs less frequently:

- daily
- weekly
- on demand
- after major incidents

Can perform:

- historical completeness checks
- quality trends
- data distribution drift
- historical duplicate analysis
- partition health
- historical FK audits
- reconciliation

Architecture:

```text
                 PROFILING
                    |
          +---------+---------+
          |                   |
          ↓                   ↓
     Incremental          Deep Audit
     every batch          periodic
          |                   |
      lightweight         expensive
      low latency        comprehensive
```

---

# 4. Partitioning Strategy

## Why?

Suppose historical telemetry grows to:

```text
500 TB
```

and the new batch is:

```text
12 GB
```

We do not want:

```text
500 TB → scan → find today's 12 GB
```

We want:

```text
500 TB stored
      ↓
partition pruning
      ↓
relevant partition
      ↓
12 GB
```

Conceptual layout:

```text
fact_telematics_raw/
    event_date=2026-08-21/
        hour=00/
        hour=01/
        ...

    event_date=2026-08-22/
        hour=00/
        ...

    event_date=2026-08-23/
        hour=10/
        hour=11/
```

Exact production partitioning will depend on query patterns and file sizing.

## Key idea

**Partitioning is valuable because it allows Spark to avoid reading unrelated data.**

This is partition pruning.

---

# 5. Column Pruning

For FK validation, we do not need the entire telemetry row.

We need:

```text
event_id
vehicle_id
```

We do not need:

```text
timestamp_utc
latitude
longitude
speed_kmh
odometer_km
ignition_status
```

Likewise, from `dim_vehicle`, FK validation only needs:

```text
vehicle_id
```

Therefore:

```text
fact_telematics_raw
      ↓
event_id + vehicle_id

dim_vehicle
      ↓
vehicle_id
```

## Production principle

**Do not move/read data that a particular DQ rule does not require.**

At TB/PB scale, projection pruning can materially reduce I/O and processing cost.

---

# 6. Production Storage Consideration

The local project uses CSV as a synthetic raw-data representation.

Production analytical processing should generally use a more efficient columnar/lake format such as:

- Parquet
- or a table format such as Iceberg/Delta/Hudi depending on architecture

Reason:

A columnar format allows Spark to read only required columns instead of repeatedly processing entire rows.

CSV remains useful here because this project is deliberately modeling messy raw source data.

---

# 7. Checkpoint 1B — Production FK Validation

## Business Problem

We need to determine:

> Does each `fact_telematics_raw.vehicle_id` exist in `dim_vehicle.vehicle_id`?

Example:

```text
dim_vehicle
-----------
V001
V002
V003

telemetry
---------
E001 V001
E002 V002
E003 V999
```

`V999` is an orphan foreign key.

## Production boundary

Do not validate:

```text
500 TB historical data
```

for every micro-batch.

Validate:

```text
new 12 GB batch
```

and perform periodic historical audits separately.

---

# 8. Batch Identity

Every processing unit should have a stable identity:

```text
batch_id = 20260823_1015
```

This supports:

- auditability
- retryability
- idempotency
- lineage
- reproducibility
- reprocessing
- DQ result tracking

The DQ framework should be able to answer:

> Which exact batch produced this DQ result?

---

# 9. Parent PK Validation vs FK Validation

These are separate DQ rules.

## Parent PK

```text
dim_vehicle.vehicle_id
```

Question:

> Is each vehicle ID unique in the parent dimension?

## FK

```text
fact_telematics_raw.vehicle_id
```

Question:

> Does the child vehicle ID exist in the parent key set?

Even if we create a deduplicated reference-key set for the FK check, we must still report duplicate parent keys separately.

Example:

```text
dim_vehicle
V001
V002
V002
V003
```

This is a PK violation.

But for an existence check:

```text
Does V002 exist?
```

the answer is still yes.

Therefore:

```text
PK uniqueness ≠ FK validity
```

---

# 10. LEFT ANTI JOIN

For the FK rule, the logical question is:

> Return telemetry records whose `vehicle_id` does not exist in the valid vehicle key set.

This maps naturally to:

```text
LEFT ANTI JOIN
```

Conceptual result:

```text
telemetry
---------
E001 V001
E002 V002
E003 V999

vehicle keys
------------
V001
V002

LEFT ANTI result
----------------
E003 V999
```

The result contains only orphan records.

## Production principle

**Compute the smallest result necessary to answer the DQ question.**

We do not need to materialize a complete joined telemetry dataset just to find FK violations.

---

# 11. Broadcast vs Shuffle

## Shuffle-oriented join

Conceptually:

```text
Telemetry
   ↓
partition by vehicle_id
   ↓
SHUFFLE
   ↓
join

Vehicle
   ↓
partition by vehicle_id
   ↓
SHUFFLE
```

Shuffle can involve:

- serialization
- network transfer
- repartitioning
- disk spill
- additional stages

As data volume grows, network movement can become a major bottleneck.

## Broadcast join

If the reference dataset is sufficiently small:

```text
dim_vehicle keys
      ↓
broadcast
   /   |   \
  ↓    ↓    ↓
E1    E2    E3
```

Each executor receives the small reference set and can perform local matching against its telemetry partition.

Potential physical operator:

```text
BroadcastHashJoin
```

## Important production rule

Do NOT memorize:

> "Always broadcast dimensions."

Instead ask:

- How large is the reference data?
- How much executor memory is available?
- How many executors are running?
- What is the size after serialization?
- How often is the broadcast performed?
- Is the reference data changing?
- Would broadcast be cheaper than the alternative join strategy?

A dimension table being logically "small" does not automatically mean it is safe to broadcast.

---

# 12. Late-Arriving Reference Data

A failed FK does not necessarily mean permanent bad data.

Example:

```text
10:15
telemetry references V999
V999 not yet in dim_vehicle
        ↓
temporary orphan
```

At:

```text
10:25
dim_vehicle receives V999
```

Now the child may be valid.

Therefore the production design can use:

```text
orphan
  ↓
quarantine
  ↓
reference data arrives
  ↓
revalidation
  ↓
accepted
```

This is especially important because the platform assumes late-arriving data and multiple source systems.

---

# 13. DQ Result Design

The profiler should not only print:

```text
Orphan count = 2,847
```

It should create structured results.

Conceptual DQ summary:

```text
dq_run_id
batch_id
table_name
rule_name
column_name
check_type
records_checked
records_failed
failure_rate
status
severity
execution_timestamp
```

Example:

```text
batch_id            20260823_1015
table_name          fact_telematics_raw
rule_name           FK_vehicle_id_exists
column_name         vehicle_id
records_checked     100,000,000
records_failed      2,847
failure_rate        0.002847%
status              FAIL
severity            ERROR
```

Potentially store violating records separately:

```text
batch_id
event_id
vehicle_id
failure_reason
```

This creates:

```text
DQ summary
    +
DQ violations
```

---

# 14. DQ Detection vs DQ Policy

The profiler detects the problem.

The business policy decides what to do with it.

Example:

```text
100,000,000 records
2,847 orphan records
```

Possible policy:

```text
failure rate < 1%
    ↓
accept valid records
quarantine invalid records
```

Another policy could be:

```text
failure rate >= 1%
    ↓
reject/quarantine entire batch
```

Therefore separate:

```text
Detection
   ↓
Measurement
   ↓
Decision / Policy
```

---

# 15. Checkpoint 1D — Production Row-Count Strategy

## Why row count is not one universal operation

Possible questions include:

- How many records arrived?
- How many landed?
- How many passed DQ?
- How many were quarantined?
- How many records exist historically?
- Is volume trending normally?

These questions do not necessarily require the same counting technique.

---

# 16. Naive Row Counting

Example:

```text
500 TB
   ↓
COUNT(*)
```

This can require a large scan.

Doing it repeatedly every 15 minutes is usually unjustified.

The issue isn't:

> `count()` is always bad.

The issue is:

> **The scope and frequency of the exact computation must justify its cost.**

---

# 17. Ingestion Metadata

An ingestion process may already provide:

```text
batch_id
file_count
bytes_received
source_record_count
arrival_timestamp
source_system
```

Example:

```text
batch_id              20260823_1015
files_received        24
bytes_received        12.4 GB
source_record_count   100,023,451
```

This can answer some operational questions without scanning the underlying data.

But source metadata should not automatically be treated as truth.

We may need to independently validate the landed batch.

---

# 18. Important Count Distinctions

The framework should distinguish:

```text
source_record_count
        ↓
records_landed
        ↓
records_validated
        ↓
records_accepted
        ↓
records_quarantined
```

Example:

```text
source_record_count = 100,000,000
records_landed      =  99,998,500
records_validated   =  99,998,500
records_accepted    =  99,997,100
records_quarantined =       1,400
```

This is far more informative than one generic count.

---

# 19. Bytes vs Records

Important:

```text
bytes_received ≠ records_received
```

Why?

Because file size depends on:

- encoding
- row width
- compression
- file format
- column values
- serialization

Therefore byte volume cannot replace record count.

Use each metric for the question it actually answers.

---

# 20. Exact Count for Incremental Batches

For a newly arrived 12 GB batch:

```text
12 GB
  ↓
partition pruning
  ↓
exact COUNT(*)
```

This may be appropriate because we intentionally restrict the scan to the new data.

The optimization is not that `COUNT(*)` is free.

The optimization is:

> **We count only the data that needs to be counted.**

---

# 21. Historical Counts Through Metadata

Instead of repeatedly scanning 500 TB:

```text
partition-level metadata

2026-08-20   98,234,112
2026-08-21  102,193,481
2026-08-22   99,882,103
```

Then:

```text
historical_count
=
SUM(partition_counts)
```

This is an incrementally maintained aggregate.

Periodic exact reconciliation can validate that the metadata remains correct.

---

# 22. Approximate Count

Exact counts are not always required.

Examples where approximate volume can be acceptable:

- operational trend monitoring
- exploratory analysis
- rough scale estimates

Examples where exactness may be required:

- ingestion reconciliation
- regulatory metrics
- financial reporting
- strict DQ thresholds
- contractual controls

Principle:

> **Accuracy is driven by the business requirement.**

---

# 23. Avoiding Repeated Scans Across DQ Rules

Bad conceptual design:

```text
COUNT
 ↓ scan

NULL CHECK
 ↓ scan

RANGE CHECK
 ↓ scan

TIMESTAMP CHECK
 ↓ scan

DUPLICATE CHECK
 ↓ scan/shuffle

FK CHECK
 ↓ join
```

Better direction:

```text
Incremental batch
       ↓
required columns only
       ↓
combine compatible metrics where possible
       ↓
avoid redundant scans
       ↓
separate expensive joins/aggregations where necessary
```

Not every DQ rule can be physically combined into a single scan, but redundant work should be deliberately minimized.

---

# 24. Row Count by Scale

```text
Local ~100 MB
    → exact count is fine

100 GB
    → start caring about scan efficiency

1 TB
    → partitioning + shuffle + I/O matter

10 TB
    → incremental processing and join strategy matter heavily

100 TB+
    → metadata and periodic reconciliation become increasingly important

PB scale
    → ask "why am I scanning this data at all?"
```

---

# 25. Checkpoint 1E — Production Schema Inspection

## Business Problem

Schema validation is not simply:

```text
"What schema did Spark infer?"
```

It is:

```text
"What schema arrived, and is it allowed by the pipeline contract?"
```

---

# 26. Schema Metadata vs Data Content

Two different categories:

```text
                SCHEMA QUALITY
                     |
            +--------+--------+
            |                 |
            ↓                 ↓
      Metadata check      Data check
            |                 |
      columns/types/etc.   values/content
            |                 |
        relatively cheap    potentially expensive
```

Schema metadata can often be inspected without scanning billions of records.

Value validation such as:

```text
speed_kmh >= 0
```

requires actual data evaluation.

---

# 27. Expected Schema vs Observed Schema

Expected:

```text
event_id          STRING
vehicle_id        STRING
timestamp_utc     STRING
latitude          DOUBLE
longitude         DOUBLE
speed_kmh         DOUBLE
odometer_km       DOUBLE
ignition_status   STRING
```

Observed:

```text
speed_kmh         STRING
```

The framework should produce:

```text
Schema violation

column: speed_kmh
expected: DOUBLE
observed: STRING
status: FAIL
```

This is what makes schema inspection a quality rule rather than a debugging print statement.

---

# 28. Metadata-Driven Schema Registry

Do not hard-code every table's rules independently.

Conceptual metadata:

```text
table_name
column_name
expected_data_type
nullable
required
ordinal_position
allowed_evolution
schema_version
effective_from
```

Example:

```text
fact_telematics_raw | speed_kmh  | DOUBLE
fact_telematics_raw | vehicle_id | STRING
```

Benefits:

- maintainability
- consistency
- governance
- schema versioning
- reuse across 24 tables
- easier automation

Schema metadata being cheap is a separate concern from metadata-driven design.

---

# 29. Schema Validation Should Be an Early Gate

Preferred flow:

```text
New batch
   ↓
Batch metadata
   ↓
Schema validation
   ↓
PASS / WARNING / FAIL
   ↓
only if allowed:
   ↓
expensive row-level DQ
   ↓
FK
duplicates
nulls
timestamps
ranges
```

Why?

Suppose the batch contains:

```text
vehicle_id → STRUCT
```

when the contract expects:

```text
vehicle_id → STRING
```

There is little reason to spend cluster resources performing expensive downstream DQ on structurally invalid data.

This is **fail-fast validation**.

---

# 30. Schema Evolution

Not every schema difference is automatically an error.

Example:

```text
Old:
event_id
vehicle_id
speed_kmh

New:
event_id
vehicle_id
speed_kmh
engine_temp
```

Adding a nullable/optional field may be acceptable.

Possible policy:

```text
No change
    → PASS

Compatible added field
    → WARNING or PASS

Removed required field
    → FAIL

Breaking type change
    → FAIL / review

Renamed field
    → FAIL / explicit migration
```

Therefore:

```text
Observed schema
      ↓
Schema contract
      ↓
Evolution policy
      ↓
PASS / WARNING / FAIL
```

Schema validation is policy-driven, not just string equality.

---

# 31. Schema Problem vs Value Problem

Example:

```text
speed_kmh = "ABC"
```

This does not automatically mean the whole schema is wrong.

If raw ingestion intentionally preserves source values as strings:

```text
schema:
speed_kmh = STRING
```

may be structurally acceptable.

Then:

```text
Schema validation
        ↓
PASS

Content validation
        ↓
"ABC" is not valid numeric value
        ↓
record-level DQ failure
```

Important distinction:

```text
Schema problem
= structure/type/columns are wrong

Value problem
= structure is allowed, but a particular value is invalid
```

This distinction will matter later for timestamps and numeric ranges.

---

# 32. Schema Inference vs Explicit Production Contract

The local raw dataset is CSV.

The generator writes CSV files for the 24 synthetic tables.

In production, relying blindly on:

```text
inferSchema = true
```

is risky because the interpreted schema can become dependent on the incoming data.

Production direction:

```text
schema registry / contract
        ↓
expected schema
        ↓
controlled ingestion/read
        ↓
schema validation
```

The raw layer can remain permissive while downstream layers apply explicit contracts.

---

# 33. Raw Data Philosophy

The project follows:

```text
Preserve raw source
        ↓
Do not "fix" raw data just to simplify code
        ↓
Profile / validate downstream
        ↓
Quarantine invalid records
```

This preserves source fidelity and supports replay, lineage, and forensic analysis.

---

# 34. Current Production Profiling Architecture

```text
                     SOURCE
                       |
                       ↓
                Batch arrives
                       |
                       ↓
                 batch_id
                       |
                       ↓
              Raw / partitioned data
                       |
                       ↓
              Incremental profiler
                       |
        +--------------+--------------+
        |                             |
        ↓                             ↓
   Row-count strategy           Schema contract
        |                             |
        +--------------+--------------+
                       |
                       ↓
                  Row-level DQ
                       |
         +-------------+-------------+
         |             |             |
         ↓             ↓             ↓
        PK            FK           Nulls
         |             |             |
         +-------------+-------------+
                       |
                       ↓
             Duplicate/timestamp/
               numeric checks
                       |
                       ↓
                 DQ decision
                  /        \
                 ↓          ↓
              Accept    Quarantine
                              |
                              ↓
                         Revalidation
```

---

# 35. Important Production Principles Established

1. Never design around the local sample size.
2. Incremental processing should be the default operational pattern.
3. Historical deep profiling should be separate.
4. Partition pruning reduces unnecessary scans.
5. Projection pruning reduces unnecessary column I/O.
6. Broadcast is a decision, not a universal rule.
7. Shuffle is expensive because of network/data movement.
8. PK and FK validation are separate rules.
9. LEFT ANTI JOIN is appropriate for orphan detection.
10. Late-arriving reference data may require revalidation.
11. Row counts should be driven by business needs.
12. Ingestion metadata can reduce unnecessary scanning.
13. Exact counts are appropriate when reconciliation/accuracy matters.
14. Historical counts can be maintained from incremental metadata.
15. Schema inspection should happen before expensive row-level DQ.
16. Schema evolution must be governed by policy.
17. Schema validation and value validation are different.
18. Metadata-driven rules are necessary for an enterprise framework.
19. DQ results must be persisted as structured metadata.
20. Raw data should remain source-faithful; bad data is detected downstream.
21. Do not write the profiler until the production execution strategy is understood.

---

# 36. Interview Questions

### Q1. Why shouldn't you run full-table profiling every 15 minutes?

Because the historical dataset may be hundreds of TB or PB while only a small incremental batch changed. Repeatedly scanning historical data creates unnecessary I/O, CPU, execution time, shuffle and infrastructure cost.

### Q2. What is the difference between incremental and historical profiling?

Incremental profiling validates newly arrived/affected data during ingestion. Historical profiling performs deeper and less frequent audits over already stored data.

### Q3. Why use partitioning?

To allow partition pruning so Spark reads only relevant data rather than scanning the entire historical dataset.

### Q4. When would you consider a broadcast join?

When the reference dataset is sufficiently small relative to cluster resources and broadcasting it is cheaper and safer than redistributing the large dataset.

### Q5. Why LEFT ANTI JOIN for FK validation?

Because the question is "which child records have no matching parent key?" LEFT ANTI directly returns those unmatched child records.

### Q6. Why maintain batch_id?

For auditability, idempotency, retries, lineage, reprocessing and precise DQ-result tracking.

### Q7. Why isn't source_record_count always enough?

Because the source count may differ from the records actually landed in the raw layer due to ingestion errors, corruption, filtering or partial delivery.

### Q8. Why is schema validation early?

Because structural validation is relatively cheap and can fail-fast before expensive joins, aggregations, timestamp processing and other row-level checks.

### Q9. Is every schema change a failure?

No. Compatible schema evolution may be allowed by policy. Breaking changes such as removal of required columns or incompatible type changes may fail the batch.

### Q10. What is the difference between schema invalidity and invalid data values?

Schema invalidity concerns structure/type/columns. Invalid values concern individual records whose values do not satisfy content/semantic rules.

---

# 37. Common Mistakes

### Mistake 1
"Spark can process PB data, so full scans are fine."

Wrong. Distributed processing makes large scans possible; it does not make unnecessary scans cheap.

### Mistake 2
"Always broadcast dimension tables."

Wrong. Broadcast must fit cluster/resource constraints and should be compared with the alternative join strategy.

### Mistake 3
"COUNT(*) is bad."

Too simplistic. Exact counting is appropriate when its scope and business purpose justify it.

### Mistake 4
"Schema equals printSchema()."

Wrong. Production schema validation requires comparison with an expected contract and evolution policy.

### Mistake 5
"FK failure means permanently bad record."

Not necessarily. Late-arriving reference data can temporarily make a record appear orphaned.

### Mistake 6
"PK duplicate check and FK check are the same."

They are separate quality rules.

---

# 38. Checkpoint Progress

```text
Phase 0 / Production-aware Profiling

[✓] Workload analysis
[✓] Incremental vs historical profiling
[✓] Production FK validation design
[✓] Broadcast vs shuffle reasoning
[✓] Production row-count strategy
[✓] Production schema inspection
[ ] Primary-key uniqueness
[ ] Duplicate business/event-key strategy
[ ] Null-like values
[ ] Malformed timestamps
[ ] Numeric range validation
[ ] Complete physical plan experiments
[ ] Metadata-driven framework
[ ] Generalize to 24 tables
```

## Current checkpoint

**1E complete**

## Next checkpoint

**1F — Production Primary-Key Uniqueness**

Main questions:

```text
How do we prove key uniqueness at billions of records?

What does GROUP BY/DISTINCT physically cost?

How much shuffle is created?

Can incremental uniqueness reduce the cost?

How do ingestion/idempotency strategies affect duplicate detection?

When do we need a full historical uniqueness audit?
```

---

# 39. Final Mental Model

The key progression learned so far:

```text
Business problem
      ↓
Production workload
      ↓
Incremental boundary
      ↓
Partition pruning
      ↓
Projection pruning
      ↓
Cheap metadata/schema gates
      ↓
Targeted row-level DQ
      ↓
Efficient joins / aggregations
      ↓
DQ measurement
      ↓
DQ policy
      ↓
Accept / quarantine / revalidate
```

The most important mindset:

> **Do not ask only "Can Spark do this?"**
>
> Ask:
>
> **"What data must Spark touch, how much data will move, how many times will we scan it, what will shuffle, what can be avoided, and what does the business actually require?"**

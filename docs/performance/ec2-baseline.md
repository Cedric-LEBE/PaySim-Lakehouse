# EC2 performance baseline — complete PaySim replay

Measured from 128 user-provided pipeline reports. All batches passed and cover source rows 1–6,362,620 contiguously, without overlapping requested intervals. These reports do not independently re-read the dataset; they record the pipeline checks already executed.

Total recorded execution time: **96,207.10 seconds (26.72 hours)**. This is the sum of four replay campaigns, not a timestamp-based wall-clock duration. Pauses between campaigns are excluded. All reports used rate 1,000 events/s and max_offsets 5,000. The final batch contains 12,620 rows; all other batches contain 50,000.

Infrastructure recorded separately: m7i-flex.large, 2 vCPU, 8 GiB RAM and encrypted 100 GiB gp3 EBS. Reports do not provide peak RAM, CPU, I/O, disk growth, or billed costs.

| Stage | Recorded seconds | Share of execution time |
| --- | ---: | ---: |
| verify | 27,859.45 | 28.96% |
| star | 22,753.50 | 23.65% |
| silver | 13,527.76 | 14.06% |
| gold | 11,643.10 | 12.10% |
| producer | 7,631.30 | 7.93% |
| bronze | 6,871.42 | 7.14% |
| bronze-drain | 3,412.76 | 3.55% |
| kafka-start | 1,028.35 | 1.07% |
| preflight | 762.66 | 0.79% |
| kafka-topic | 596.48 | 0.62% |
| kafka-stop | 120.20 | 0.12% |

## Growth across equal-sized batches

Median duration of the first ten full batches (rows 1–500,000) compared with the last ten full batches (rows 5,850,001–6,350,000). The shorter final batch is excluded. These are observations across one replay, not repeated controlled benchmarks.

| Stage | First ten median seconds | Last ten median seconds |
| --- | ---: | ---: |
| producer | 54.91 | 64.16 |
| bronze | 46.59 | 58.36 |
| silver | 56.62 | 153.24 |
| gold | 45.02 | 132.25 |
| star | 56.41 | 310.57 |
| verify | 46.86 | 375.20 |

## Explanation supported by code inspection

`tests/integration/replay_check.py` streams the whole CSV prefix and sorted Silver rows into Python for every batch, then checks Gold totals and star uniqueness/references. The requested prefixes sum to **412,762,620 row comparisons** across this replay, although the source contains only 6,362,620 unique rows. This is repeated validation work, not duplicated ingestion.

`star_incremental.py` validates the full Silver snapshot and published tables/references at each incremental publication. Incremental writes therefore still include full-table validation work. Timings attribute costs to whole stages; they cannot isolate each check, shuffle, merge or disk operation.

## Next controlled experiment

Measure validation substeps first. Then test bounded per-batch verification plus explicit periodic/final full reconciliation on isolated data and checkpoints. Keep deterministic identities, CDF checks, quarantine handling, publication consistency and restart tests. Compare identical input intervals, resources, rate and offset limits. Do not skip full checks in the existing production replay or promise a speedup before an equivalent before/after test.

[Per-batch measurements](ec2-baseline.csv). No raw transactions, credentials or host paths are included.


## Instrumentation before optimization

The full checks remain enabled. Star incremental emits structured `TIMING:` JSON lines for `validate_silver_snapshot`, `merge_dimensions`, `merge_facts`, and `validate_published_tables`. Prefix verification emits timings for `compare_csv_silver_and_expected_totals`, `reconcile_gold`, `reconcile_star_facts`, and `reconcile_dimensions`. Status is recorded on success and on exceptions; original failures still propagate. The no-change path does not execute these star spans.

Timings go to the existing stage logs, without changing checkpoint or publication formats. They include wall time and lazy Spark actions executed inside each span; they are not independent CPU timings and cannot be summed across different processes as elapsed calendar time. The comparison span includes Spark iterator preparation, row transfer, Python CSV comparison and expected aggregate construction. Snapshot caching is populated by actions, so later phase costs can depend on earlier phases.

Do not run a production replay to collect these measurements. First exercise the synthetic integration scenarios; then benchmark identical bounded input prefixes in separate data roots/checkpoints on the same machine, with fixed rate/offset settings. Record cold/warm dependencies and retain full verification. Only after collecting these substep timings should an optimization experiment change validation scheduling.

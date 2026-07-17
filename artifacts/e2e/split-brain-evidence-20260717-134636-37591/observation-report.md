# Split-Brain Evidence Observation Report

| Scenario ID | Script | Expected Outcome | Observed Outcome | Result | Log |
| --- | --- | --- | --- | --- | --- |
| SB-001 | partition_reconvergence.sh | Reject invalid schema/WAL state and recover to ready with convergence progression | Stage completed successfully with non-zero test execution and suite pass | Pass | /Users/samcolak/Source Code/rust/distdb/distdb/artifacts/e2e/split-brain-evidence-20260717-134636-37591/SB-001.log |
| SB-002 | split_brain_dual_primary.sh | Deterministic conflict behavior and no partial durability leakage | Stage completed successfully with non-zero test execution and suite pass | Pass | /Users/samcolak/Source Code/rust/distdb/distdb/artifacts/e2e/split-brain-evidence-20260717-134636-37591/SB-002.log |
| SB-003 | unilateral_write_delayed_heal.sh | Stream-aware catch-up and deterministic delayed-heal recovery | Stage completed successfully with non-zero test execution and suite pass | Pass | /Users/samcolak/Source Code/rust/distdb/distdb/artifacts/e2e/split-brain-evidence-20260717-134636-37591/SB-003.log |
| SB-004 | repeated_partition_heal_cycles.sh | Stable repeated-cycle convergence and conflict safety | Stage completed successfully with non-zero test execution and suite pass | Pass | /Users/samcolak/Source Code/rust/distdb/distdb/artifacts/e2e/split-brain-evidence-20260717-134636-37591/SB-004.log |

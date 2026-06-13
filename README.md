## Status

| Component | Status | Week | Notes |
|---|---|---|---|
| ChronicleGraph | Done | 1 | DAG registration, traversal, cycle detection |
| BlastRadiusAnalyzer | Done | 1 | Forward taint with real Spark row counts |
| CausalityTracer structural | Done | 1 | Topology-based fallback |
| CausalityTracer CDF | Done | 2 | Real timestamp correlation algorithm |
| Lineage path finder | Done | 2 | BFS source-to-target path |
| Cycle detection | Done | 2 | DFS validate() check |
| Integration tests | Done | 2 | Real Spark + Delta, 17 tests |
| Interactive UI | Planned | 6 | React + D3 timeline + blast radius graph |
| GDPR propagator | Planned | 3 | Cascading forget() across DAG |
| PyPI release | Planned | 8 | pip install delta-chronicle |

## Test Coverage

| Suite | Tests | Runner |
|---|---|---|
| Unit (no Spark) | 32 | docker compose run --rm test |
| Integration (real Spark) | 17 | docker compose run --rm test-integration |
| End-to-end demo | manual | docker compose run --rm demo |
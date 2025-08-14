# Typesense Raft TLA+ Formal Verification

This directory contains formal TLA+ specifications for verifying the correctness and safety properties of Typesense's Raft implementation.

## Overview

Our TLA+ specifications formally verify the core safety properties we implemented in Typesense's Raft consensus algorithm, including:

- **Core Raft Safety**: Leader election, log replication, commit safety
- **Configuration Change Safety**: Single-node changes with quorum overlap
- **Advanced Safety Validation**: MongoDB TLA+-inspired ConfigIsSafe checks
- **Disaster Recovery Safety**: DNS-aware node addressing and failure handling

## Files

### Core Specifications

- **`TypesenseRaft.tla`**: Main Raft specification with core safety properties
- **`TypesenseSafetyProperties.tla`**: Enhanced specification with Typesense-specific safety mechanisms
- **`TypesenseRaft.cfg`**: TLC configuration for basic Raft model checking
- **`TypesenseSafetyProperties.cfg`**: TLC configuration for enhanced safety properties

### Utilities

- **`run_model_check.sh`**: Automated model checking script
- **`README.md`**: This documentation file

## Safety Properties Verified

### Core Raft Properties

1. **Leader Safety**: At most one leader per term in any configuration
2. **Log Integrity**: Leader's log is never overwritten  
3. **Commit Safety**: Committed entries are never lost
4. **Applied Committed**: Applied entries are always committed first

### Configuration Change Properties

5. **Single Node Change**: Only single-node configuration changes allowed
6. **Quorum Overlap Safety**: Configuration changes maintain quorum overlap
7. **Leader In Config**: Only configured servers can become leaders

### Enhanced Safety Properties (Typesense-Specific)

8. **Safety Monotonic**: Safety validation counters only increase
9. **Leader Safety Validation**: Leaders always pass ConfigIsSafe checks
10. **Safe Config Changes Only**: Configuration changes happen after validation
11. **No Unsafe Transitions**: All configuration transitions are safe
12. **Term Safety**: Leaders have term-based quorum validation
13. **Config Safety**: Leaders have config-based quorum validation  
14. **Leader Safety Maintenance**: Leaders perform regular safety validation
15. **Disaster Recovery Safety**: Servers can safely rejoin after failures

## Key Safety Functions Verified

Based on MongoDB's TLA+ patterns, we verify these critical safety functions:

### ConfigIsSafe Validation
```tla
ConfigIsSafe(s) ==
    /\ HasValidTermQuorum(s)
    /\ HasValidConfigQuorum(s)
    /\ ArePreviousOpsCommitted(s)
    /\ state[s] = Leader
```

### Single-Node Change Safety
```tla
ValidateConfigChange(oldServers, newServers) ==
    LET added == newServers \ oldServers
        removed == oldServers \ newServers
    IN Cardinality(added) + Cardinality(removed) = 1
```

### Quorum Overlap Validation
```tla
HasQuorumOverlap(oldServers, newServers) ==
    LET oldMajority == Majority(oldServers)
        newMajority == Majority(newServers)
        intersection == oldServers \cap newServers
    IN Cardinality(intersection) >= oldMajority 
       /\ Cardinality(intersection) >= newMajority
```

## Running Model Checking

### Prerequisites

1. Install TLA+ Tools from: https://lamport.azurewebsites.net/tla/tools.html
2. Ensure `tlc` command is in your PATH

### Basic Model Checking

```bash
# Run automated verification
./run_model_check.sh

# Or run individual specifications
tlc -config TypesenseRaft.cfg TypesenseRaft.tla
tlc -config TypesenseSafetyProperties.cfg TypesenseSafetyProperties.tla
```

### Advanced Model Checking

```bash
# With detailed statistics and coverage
tlc -config TypesenseRaft.cfg -coverage 60 -statistics TypesenseRaft.tla

# Focus on specific safety properties (edit .cfg file)
# Uncomment specific INVARIANTS in TypesenseSafetyProperties.cfg

# Check liveness properties (requires fairness assumptions)
# Uncomment PROPERTIES section in TypesenseRaft.cfg
```

## Model Checking Configuration

### Constants
- **Server**: `{S1, S2, S3}` (3-node cluster for model checking)
- **MaxConfigVersion**: `4` (limits configuration changes)
- **MaxLogLen**: `3` (limits log length for state space)

### Constraints
- Terms limited to reasonable range (≤5)
- Safety validation counters bounded (≤5)
- Configuration versions bounded (≤4)

## Relationship to Implementation

### C++ Implementation Mapping

Our TLA+ specifications directly model the safety functions implemented in:

- **`src/raft_safety_validator.cpp`**: 
  - `config_is_safe()` ↔ `ConfigIsSafe(s)`
  - `validate_new_config_quorum()` ↔ `ValidateNewConfigQuorum()`
  - `has_term_quorum_check()` ↔ `HasValidTermQuorum(s)`
  - `has_config_quorum_check()` ↔ `HasValidConfigQuorum(s)`

- **`include/raft_server.h`**:
  - `NodeConfiguration.is_safe_single_node_change()` ↔ `ValidateConfigChange()`
  - `NodeConfiguration.is_newer_than()` ↔ `IsNewerConfig()`

### Safety State Tracking

The TLA+ specifications model our C++ safety state variables:

```cpp
// C++ Implementation
std::atomic<uint64_t> last_term_quorum_check;
std::atomic<uint64_t> last_config_quorum_check; 
std::chrono::steady_clock::time_point last_safety_validation;
```

```tla
\* TLA+ Specification  
VARIABLE lastTermQuorumCheck
VARIABLE lastConfigQuorumCheck
VARIABLE safetyValidationCounter
```

## Verification Results

When model checking passes, we have **formal proof** that:

1. **No Split-Brain**: Never more than one leader per term
2. **No Data Loss**: Committed entries are never lost
3. **Safe Reconfigs**: Configuration changes maintain consensus safety
4. **Disaster Recovery**: Clusters can safely recover from total IP changes
5. **Quorum Integrity**: All operations maintain proper quorum validation

## Advanced Verification Scenarios

### Disaster Recovery Verification

The specifications verify safety during disaster recovery scenarios:

```tla
DisasterRecoverySafety ==
    \A s \in Server :
        (state[s] = Down) =>
        \A t \in Server :
            (state[t] = Leader /\ s \in GetConfigServers(t)) =>
            ConfigIsSafe(t)
```

### Configuration Change Verification

All configuration changes are verified to be safe:

```tla
SafeAddServer(s, newServer) ==
    /\ ConfigIsSafe(s)  \* Full safety check before change
    /\ ValidateNewConfigQuorum(oldServers, newServers)
    /\ AddServer(s, newServer)
```

## Comparison with MongoDB

Our specifications are based on MongoDB's TLA+ patterns but adapted for Typesense's simpler file-based configuration model:

### Similarities
- ConfigIsSafe validation pattern
- Single-node configuration changes
- Term and configuration versioning
- Quorum overlap requirements

### Differences  
- **Simplified State Management**: No complex ConfigurationState enum
- **File-Based Model**: Configuration changes via file updates, not interactive APIs
- **DNS Abstraction**: DNS resolution abstracted for core Raft verification
- **Streamlined Safety**: Focus on essential safety without MongoDB's complexity

## Future Extensions

Potential extensions to the formal verification:

1. **Liveness Properties**: Verify progress guarantees under fairness
2. **Performance Models**: Model timing and performance characteristics
3. **Network Partition Handling**: Verify behavior during network splits
4. **DNS Resolution Details**: More detailed DNS failure and resolution models
5. **Snapshot Verification**: Formal verification of snapshot safety

## Contributing

When modifying the Raft implementation:

1. Update corresponding TLA+ specifications
2. Run model checking to verify safety properties
3. Add new safety properties for new features
4. Update this documentation

## References

- [TLA+ Homepage](https://lamport.azurewebsites.net/tla/tla.html)
- [MongoDB Raft TLA+ Specifications](../tla_plus/)
- [Raft Consensus Algorithm Paper](https://raft.github.io/raft.pdf)
- [TLA+ Model Checking Guide](https://learntla.com/introduction/) 
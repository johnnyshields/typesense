---- MODULE TypesenseSafetyProperties ----
\* Focused specification for Typesense's safety properties
\* Validates the specific safety mechanisms we implemented:
\* 1. ConfigIsSafe validation
\* 2. Term/Config quorum checks  
\* 3. Single-node change safety
\* 4. Configuration version comparison

EXTENDS TypesenseRaft

----
\* Safety state tracking (mirrors our C++ implementation)

\* Track last term where quorum was validated
VARIABLE lastTermQuorumCheck

\* Track last config where quorum was validated  
VARIABLE lastConfigQuorumCheck

\* Track safety validation timestamps (abstracted as counters)
VARIABLE safetyValidationCounter

safetyVars == <<lastTermQuorumCheck, lastConfigQuorumCheck, safetyValidationCounter>>

----
\* Safety validation functions (mirror our C++ implementation)

\* Check if current term has valid quorum (ConfigIsSafe)
HasValidTermQuorum(s) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    /\ LET configServers == GetConfigServers(s)
           liveServers == {t \in configServers : state[t] # Down}
       IN HasMajority(s, liveServers)

\* Check if current configuration has valid quorum
HasValidConfigQuorum(s) ==
    /\ InCurrentConfig(s)
    /\ LET configServers == GetConfigServers(s)
           activeServers == {t \in configServers : state[t] \in {Follower, Candidate, Leader}}
       IN HasMajority(s, activeServers)

\* Check if previous operations are committed in current config
ArePreviousOpsCommitted(s) ==
    /\ state[s] = Leader
    /\ \A i \in 1..commitIndex[s] :
        \E entry \in committedEntries :
            /\ entry.index = i
            /\ entry.configVersion = configVersion[s]

\* Validate new configuration has valid quorum (single-node change safety)
\* Matches C++ validate_new_config_quorum() implementation
ValidateNewConfigQuorum(oldServers, newServers) ==
    /\ ValidateConfigChange(oldServers, newServers)
    /\ HasQuorumOverlap(oldServers, newServers)
    /\ Cardinality(newServers) >= 1
    /\ LET newMajority == Majority(newServers)
       IN newMajority >= 1 /\ newMajority <= Cardinality(newServers)

\* Comprehensive safety validation
ConfigIsSafe(s) ==
    /\ HasValidTermQuorum(s)
    /\ HasValidConfigQuorum(s)
    /\ ArePreviousOpsCommitted(s)
    /\ state[s] = Leader  \* Only leaders can initiate reconfigs

----
\* Enhanced actions with safety validation

\* Enhanced configuration change with full safety validation
SafeAddServer(s, newServer) ==
    /\ ConfigIsSafe(s)  \* Full safety check before change
    /\ newServer \notin GetConfigServers(s)
    /\ LET oldServers == GetConfigServers(s)
           newServers == oldServers \cup {newServer}
       IN /\ ValidateNewConfigQuorum(oldServers, newServers)
          /\ AddServer(s, newServer)
    /\ lastTermQuorumCheck' = [lastTermQuorumCheck EXCEPT ![s] = currentTerm[s]]
    /\ lastConfigQuorumCheck' = [lastConfigQuorumCheck EXCEPT ![s] = configVersion[s]]
    /\ safetyValidationCounter' = [safetyValidationCounter EXCEPT ![s] = safetyValidationCounter[s] + 1]

\* Enhanced configuration change with full safety validation  
SafeRemoveServer(s, oldServer) ==
    /\ ConfigIsSafe(s)  \* Full safety check before change
    /\ oldServer \in GetConfigServers(s)
    /\ oldServer # s  \* Cannot remove self
    /\ LET oldServers == GetConfigServers(s)
           newServers == oldServers \ {oldServer}
       IN /\ ValidateNewConfigQuorum(oldServers, newServers)
          /\ RemoveServer(s, oldServer)
    /\ lastTermQuorumCheck' = [lastTermQuorumCheck EXCEPT ![s] = currentTerm[s]]
    /\ lastConfigQuorumCheck' = [lastConfigQuorumCheck EXCEPT ![s] = configVersion[s]]
    /\ safetyValidationCounter' = [safetyValidationCounter EXCEPT ![s] = safetyValidationCounter[s] + 1]

\* Periodic safety validation (mirrors our refresh_nodes safety checks)
PeriodicSafetyValidation(s) ==
    /\ InCurrentConfig(s)
    /\ ConfigIsSafe(s) \/ state[s] # Leader  \* Leaders must pass safety, others just validate
    /\ lastTermQuorumCheck' = [lastTermQuorumCheck EXCEPT ![s] = currentTerm[s]]
    /\ lastConfigQuorumCheck' = [lastConfigQuorumCheck EXCEPT ![s] = configVersion[s]]
    /\ safetyValidationCounter' = [safetyValidationCounter EXCEPT ![s] = safetyValidationCounter[s] + 1]
    /\ UNCHANGED <<serverVars, logVars, configVars, leaderVars, globalVars, dnsResolution>>

----
\* Enhanced next state relation with safety validation

SafetyNext ==
    \/ \E s \in Server : PeriodicSafetyValidation(s)
    \/ \E s \in Server, newServer \in Server : SafeAddServer(s, newServer)  
    \/ \E s \in Server, oldServer \in Server : SafeRemoveServer(s, oldServer)
    \/ Next  \* Original actions

----
\* Enhanced specification with safety validation

SafetyInit ==
    /\ Init
    /\ lastTermQuorumCheck = [s \in Server |-> 1]
    /\ lastConfigQuorumCheck = [s \in Server |-> 1]
    /\ safetyValidationCounter = [s \in Server |-> 0]

SafetySpec == SafetyInit /\ [][SafetyNext]_<<vars, safetyVars>>

----
\* Advanced Safety Properties (specific to our implementation)

\* Safety validation is monotonic (terms/configs only increase)
SafetyMonotonic ==
    \A s \in Server :
        /\ lastTermQuorumCheck[s] <= currentTerm[s]
        /\ lastConfigQuorumCheck[s] <= configVersion[s]

\* Leaders always pass ConfigIsSafe when performing reconfigs
LeaderSafetyValidation ==
    \A s \in Server : 
        (state[s] = Leader /\ safetyValidationCounter[s] > 0) => ConfigIsSafe(s)

\* Configuration changes only happen after safety validation
SafeConfigChangesOnly ==
    \A i \in 2..Len(configs) :
        \E s \in Server :
            /\ lastTermQuorumCheck[s] = configs[i].term
            /\ lastConfigQuorumCheck[s] >= configs[i-1].version

\* No unsafe configuration transitions (single-node + quorum overlap)
NoUnsafeTransitions ==
    \A i \in 1..(Len(configs)-1) :
        /\ ValidateConfigChange(configs[i].servers, configs[i+1].servers)
        /\ HasQuorumOverlap(configs[i].servers, configs[i+1].servers)

\* Term-based safety: no leader without term quorum validation
TermSafety ==
    \A s \in Server :
        (state[s] = Leader /\ currentTerm[s] > 1) =>
        lastTermQuorumCheck[s] >= currentTerm[s] - 1

\* Config-based safety: no reconfig without config quorum validation  
ConfigSafety ==
    \A s \in Server :
        (state[s] = Leader /\ configVersion[s] > 1) =>
        lastConfigQuorumCheck[s] >= configVersion[s] - 1

\* Safety validation happens regularly for leaders
LeaderSafetyMaintenance ==
    \A s \in Server :
        (state[s] = Leader) => safetyValidationCounter[s] > 0

\* Disaster recovery safety: servers can rejoin safely
DisasterRecoverySafety ==
    \A s \in Server :
        (state[s] = Down) =>
        \A t \in Server :
            (state[t] = Leader /\ s \in GetConfigServers(t)) =>
            ConfigIsSafe(t)

----
\* Comprehensive safety invariant combining all our safety properties
TypesenseComprehensiveSafety ==
    /\ LeaderSafety
    /\ LogIntegrity  
    /\ CommitSafety
    /\ AppliedCommitted
    /\ SingleNodeChange
    /\ QuorumOverlapSafety
    /\ LeaderInConfig
    /\ SafetyMonotonic
    /\ LeaderSafetyValidation
    /\ SafeConfigChangesOnly
    /\ NoUnsafeTransitions
    /\ TermSafety
    /\ ConfigSafety
    /\ LeaderSafetyMaintenance
    /\ DisasterRecoverySafety

==== 
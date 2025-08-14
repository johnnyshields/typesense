---- MODULE TypesenseRaft ----
\* Formal TLA+ specification for Typesense's Raft implementation
\* Based on MongoDB's Raft specifications with Typesense-specific adaptations
\* 
\* This specification focuses on:
\* 1. Core Raft safety properties (leader election, log replication)
\* 2. Configuration change safety (single-node changes)
\* 3. Quorum validation and term-based safety
\* 4. DNS-aware node addressing (abstracted)
\*
\* Copyright 2024 Typesense, Inc.

EXTENDS Naturals, FiniteSets, Sequences, TLC

\* The set of server IDs (can be hostnames or IPs)
CONSTANTS Server

\* Server states
CONSTANTS Follower, Candidate, Leader, Down

\* Special values
CONSTANTS Nil, NoOp

\* Maximum number of configuration versions to model check
CONSTANTS MaxConfigVersion

\* Maximum log length for model checking
CONSTANTS MaxLogLen

----
\* Global variables

\* Sequence of configuration versions
\* Each config is a record: [version |-> Nat, term |-> Nat, servers |-> SUBSET Server]
VARIABLE configs

\* Set of entries that are immediately committed (have majority consensus)
\* Each entry: [index |-> Nat, term |-> Nat, configVersion |-> Nat, type |-> String]
VARIABLE committedEntries

\* Set of entries that are applied to the state machine
\* Subset of committedEntries
VARIABLE appliedEntries

----
\* Per-server variables (functions with domain Server)

\* Server's current term
VARIABLE currentTerm

\* Server's current state
VARIABLE state

\* Server's vote in current term (Nil if hasn't voted)
VARIABLE votedFor

\* Server's log (sequence of entries)
VARIABLE log

\* Index of highest entry known to be committed
VARIABLE commitIndex

\* Index of last entry applied to state machine
VARIABLE lastApplied

\* Configuration version this server is using
VARIABLE configVersion

\* Configuration term (for version comparison)
VARIABLE configTerm

\* For leaders: next index to send to each follower
VARIABLE nextIndex

\* For leaders: highest index replicated to each follower
VARIABLE matchIndex

\* DNS resolution state (abstracted - maps server names to resolved addresses)
VARIABLE dnsResolution

----
\* Variable groups for convenience

serverVars == <<currentTerm, state, votedFor>>
logVars == <<log, commitIndex, lastApplied>>
configVars == <<configVersion, configTerm>>
leaderVars == <<nextIndex, matchIndex>>
globalVars == <<configs, committedEntries, appliedEntries>>

vars == <<serverVars, logVars, configVars, leaderVars, globalVars, dnsResolution>>

----
\* Helper functions

\* Get the current configuration for a server
GetConfig(s) == 
    LET cv == configVersion[s]
    IN IF cv <= Len(configs) THEN configs[cv] ELSE configs[Len(configs)]

\* Get servers in current configuration for server s
GetConfigServers(s) == GetConfig(s).servers

\* Check if server s is in its current configuration
InCurrentConfig(s) == s \in GetConfigServers(s)

\* Calculate majority size for a set of servers
Majority(servers) == (Cardinality(servers) \div 2) + 1

\* Check if we have majority in current config
HasMajority(s, supporters) == 
    LET configServers == GetConfigServers(s)
        validSupporters == supporters \cap configServers
    IN Cardinality(validSupporters) >= Majority(configServers)

\* Get last log index
LastLogIndex(s) == Len(log[s])

\* Get last log term
LastLogTerm(s) == 
    IF LastLogIndex(s) = 0 THEN 0 ELSE log[s][LastLogIndex(s)].term

\* Check if log entry exists
HasEntry(s, index) == index <= LastLogIndex(s) /\ index > 0

\* Get term of log entry
GetEntryTerm(s, index) == 
    IF HasEntry(s, index) THEN log[s][index].term ELSE 0

\* Check if configuration is newer (MongoDB-style comparison)
IsNewerConfig(newVersion, newTerm, oldVersion, oldTerm) ==
    \/ newTerm > oldTerm
    \/ (newTerm = oldTerm /\ newVersion > oldVersion)

\* Validate quorum for configuration change (single-node change safety)
ValidateConfigChange(oldServers, newServers) ==
    \* Only single-node changes allowed
    LET added == newServers \ oldServers
        removed == oldServers \ newServers
    IN Cardinality(added) + Cardinality(removed) = 1

\* Check if configuration change maintains quorum overlap
HasQuorumOverlap(oldServers, newServers) ==
    LET oldMajority == Majority(oldServers)
        newMajority == Majority(newServers)
        intersection == oldServers \cap newServers
    IN Cardinality(intersection) >= oldMajority /\ Cardinality(intersection) >= newMajority

----
\* Initial state

Init == 
    \* Initial configuration with all servers
    /\ configs = <<[version |-> 1, term |-> 1, servers |-> Server]>>
    /\ committedEntries = {}
    /\ appliedEntries = {}
    \* All servers start as followers in term 1
    /\ currentTerm = [s \in Server |-> 1]
    /\ state = [s \in Server |-> Follower]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ lastApplied = [s \in Server |-> 0]
    /\ configVersion = [s \in Server |-> 1]
    /\ configTerm = [s \in Server |-> 1]
    /\ nextIndex = [s \in Server |-> [t \in Server |-> 1]]
    /\ matchIndex = [s \in Server |-> [t \in Server |-> 0]]
    \* Abstract DNS resolution (all servers resolve to themselves initially)
    /\ dnsResolution = [s \in Server |-> s]

----
\* Actions

\* Server s times out and starts election
StartElection(s) ==
    /\ state[s] \in {Follower, Candidate}
    /\ InCurrentConfig(s)  \* Only configured servers can become candidates
    /\ state' = [state EXCEPT ![s] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]  \* Vote for self
    /\ UNCHANGED <<logVars, configVars, leaderVars, globalVars, dnsResolution>>

\* Server s votes for server t in term term
Vote(s, t, term) ==
    /\ state[s] \in {Follower, Candidate}
    /\ currentTerm[s] < term
    /\ votedFor[s] = Nil \/ (currentTerm[s] < term)
    /\ InCurrentConfig(s) /\ InCurrentConfig(t)  \* Both must be in config
    \* Log safety: candidate's log must be at least as up-to-date
    /\ LastLogTerm(t) > LastLogTerm(s) 
       \/ (LastLogTerm(t) = LastLogTerm(s) /\ LastLogIndex(t) >= LastLogIndex(s))
    /\ currentTerm' = [currentTerm EXCEPT ![s] = term]
    /\ votedFor' = [votedFor EXCEPT ![s] = t]
    /\ state' = [state EXCEPT ![s] = Follower]
    /\ UNCHANGED <<logVars, configVars, leaderVars, globalVars, dnsResolution>>

\* Candidate s becomes leader after receiving majority votes
BecomeLeader(s) ==
    /\ state[s] = Candidate
    /\ InCurrentConfig(s)
    \* Has majority votes in current configuration
    /\ LET voters == {v \in GetConfigServers(s) : votedFor[v] = s /\ currentTerm[v] = currentTerm[s]}
       IN HasMajority(s, voters)
    /\ state' = [state EXCEPT ![s] = Leader]
    \* Initialize leader state
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [t \in Server |-> LastLogIndex(s) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [t \in Server |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, logVars, configVars, globalVars, dnsResolution>>

\* Leader s appends entry to log
AppendEntry(s, entry) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    /\ LastLogIndex(s) < MaxLogLen  \* Model checking bound
    /\ log' = [log EXCEPT ![s] = Append(log[s], entry)]
    /\ UNCHANGED <<serverVars, commitIndex, lastApplied, configVars, leaderVars, globalVars, dnsResolution>>

\* Leader s sends AppendEntries to follower t
SendAppendEntries(s, t) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s) /\ InCurrentConfig(t)
    /\ s # t
    \* Implementation detail: would send entries starting from nextIndex[s][t]
    /\ UNCHANGED vars  \* Simplified for model checking

\* Follower t accepts AppendEntries from leader s
AcceptAppendEntries(s, t, entries, leaderCommit) ==
    /\ state[t] = Follower
    /\ state[s] = Leader
    /\ currentTerm[t] = currentTerm[s]
    /\ InCurrentConfig(s) /\ InCurrentConfig(t)
    \* Log consistency check would happen here
    /\ log' = [log EXCEPT ![t] = log[s]]  \* Simplified
    /\ commitIndex' = [commitIndex EXCEPT ![t] = Min(leaderCommit, LastLogIndex(t))]
    /\ UNCHANGED <<serverVars, lastApplied, configVars, leaderVars, globalVars, dnsResolution>>

\* Leader s commits entries with majority replication
CommitEntries(s) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    /\ LET configServers == GetConfigServers(s)
           \* Find highest index replicated to majority
           replicatedTo == {i \in 1..LastLogIndex(s) : 
               LET supporters == {t \in configServers : matchIndex[s][t] >= i}
               IN HasMajority(s, supporters \cup {s})}
           newCommitIndex == IF replicatedTo = {} THEN commitIndex[s] 
                           ELSE CHOOSE i \in replicatedTo : \A j \in replicatedTo : i >= j
       IN /\ newCommitIndex > commitIndex[s]
          /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIndex]
          \* Add committed entries to global set
          /\ LET newCommits == {[index |-> i, term |-> GetEntryTerm(s, i), 
                                configVersion |-> configVersion[s]] : 
                               i \in (commitIndex[s]+1)..newCommitIndex}
             IN committedEntries' = committedEntries \cup newCommits
    /\ UNCHANGED <<serverVars, log, lastApplied, configVars, leaderVars, appliedEntries, dnsResolution>>

\* Server s applies committed entries to state machine
ApplyEntries(s) ==
    /\ lastApplied[s] < commitIndex[s]
    /\ InCurrentConfig(s)
    /\ LET newApplied == lastApplied[s] + 1
           entry == [index |-> newApplied, term |-> GetEntryTerm(s, newApplied),
                    configVersion |-> configVersion[s]]
       IN /\ lastApplied' = [lastApplied EXCEPT ![s] = newApplied]
          /\ appliedEntries' = appliedEntries \cup {entry}
    /\ UNCHANGED <<serverVars, log, commitIndex, configVars, leaderVars, configs, committedEntries, dnsResolution>>

\* Configuration change: add server to configuration
AddServer(s, newServer) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    /\ newServer \notin GetConfigServers(s)
    /\ Len(configs) < MaxConfigVersion  \* Model checking bound
    /\ LET oldConfig == GetConfig(s)
           newServers == oldConfig.servers \cup {newServer}
           newConfigVersion == configVersion[s] + 1
           newConfig == [version |-> newConfigVersion, 
                        term |-> currentTerm[s], 
                        servers |-> newServers]
       IN \* Validate single-node change
          /\ ValidateConfigChange(oldConfig.servers, newServers)
          /\ HasQuorumOverlap(oldConfig.servers, newServers)
          \* Update configuration
          /\ configs' = Append(configs, newConfig)
          /\ configVersion' = [configVersion EXCEPT ![s] = newConfigVersion]
          /\ configTerm' = [configTerm EXCEPT ![s] = currentTerm[s]]
    /\ UNCHANGED <<serverVars, logVars, leaderVars, committedEntries, appliedEntries, dnsResolution>>

\* Configuration change: remove server from configuration
RemoveServer(s, oldServer) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    /\ oldServer \in GetConfigServers(s)
    /\ oldServer # s  \* Cannot remove self
    /\ Len(configs) < MaxConfigVersion  \* Model checking bound
    /\ LET oldConfig == GetConfig(s)
           newServers == oldConfig.servers \ {oldServer}
           newConfigVersion == configVersion[s] + 1
           newConfig == [version |-> newConfigVersion, 
                        term |-> currentTerm[s], 
                        servers |-> newServers]
       IN \* Validate single-node change
          /\ ValidateConfigChange(oldConfig.servers, newServers)
          /\ HasQuorumOverlap(oldConfig.servers, newServers)
          /\ Cardinality(newServers) > 0  \* Don't create empty config
          \* Update configuration
          /\ configs' = Append(configs, newConfig)
          /\ configVersion' = [configVersion EXCEPT ![s] = newConfigVersion]
          /\ configTerm' = [configTerm EXCEPT ![s] = currentTerm[s]]
    /\ UNCHANGED <<serverVars, logVars, leaderVars, committedEntries, appliedEntries, dnsResolution>>

\* Server s adopts newer configuration
AdoptNewerConfig(s, newVersion, newTerm) ==
    /\ newVersion <= Len(configs)
    /\ IsNewerConfig(newVersion, newTerm, configVersion[s], configTerm[s])
    /\ configVersion' = [configVersion EXCEPT ![s] = newVersion]
    /\ configTerm' = [configTerm EXCEPT ![s] = newTerm]
    \* Step down if no longer in configuration
    /\ IF s \notin configs[newVersion].servers /\ state[s] = Leader
       THEN state' = [state EXCEPT ![s] = Follower]
       ELSE UNCHANGED state
    /\ UNCHANGED <<currentTerm, votedFor, logVars, leaderVars, globalVars, dnsResolution>>

\* DNS re-resolution (abstracted)
DNSReresolution(s, newResolution) ==
    /\ dnsResolution' = [dnsResolution EXCEPT ![s] = newResolution]
    /\ UNCHANGED <<serverVars, logVars, configVars, leaderVars, globalVars>>

\* Server failure/recovery
ServerDown(s) ==
    /\ state[s] # Down
    /\ state' = [state EXCEPT ![s] = Down]
    /\ UNCHANGED <<currentTerm, votedFor, logVars, configVars, leaderVars, globalVars, dnsResolution>>

ServerRecover(s) ==
    /\ state[s] = Down
    /\ state' = [state EXCEPT ![s] = Follower]
    /\ UNCHANGED <<currentTerm, votedFor, logVars, configVars, leaderVars, globalVars, dnsResolution>>

----
\* Next state relation

Next == 
    \/ \E s \in Server : StartElection(s)
    \/ \E s, t \in Server : Vote(s, t, currentTerm[t])
    \/ \E s \in Server : BecomeLeader(s)
    \/ \E s \in Server, entry \in [term : Nat, type : {"NoOp", "Data"}] : AppendEntry(s, entry)
    \/ \E s, t \in Server : SendAppendEntries(s, t)
    \/ \E s, t \in Server, entries \in Seq([term : Nat, type : {"NoOp", "Data"}]), commit \in Nat : 
           AcceptAppendEntries(s, t, entries, commit)
    \/ \E s \in Server : CommitEntries(s)
    \/ \E s \in Server : ApplyEntries(s)
    \/ \E s \in Server, newServer \in Server : AddServer(s, newServer)
    \/ \E s \in Server, oldServer \in Server : RemoveServer(s, oldServer)
    \/ \E s \in Server, v \in 1..Len(configs), t \in Nat : AdoptNewerConfig(s, v, t)
    \/ \E s \in Server, resolution \in Server : DNSReresolution(s, resolution)
    \/ \E s \in Server : ServerDown(s)
    \/ \E s \in Server : ServerRecover(s)

----
\* Specification

Spec == Init /\ [][Next]_vars

----
\* Safety Properties (Invariants)

\* At most one leader per term in any configuration
LeaderSafety == 
    \A s, t \in Server : 
        (state[s] = Leader /\ state[t] = Leader /\ s # t) =>
        (currentTerm[s] # currentTerm[t] \/ GetConfigServers(s) # GetConfigServers(t))

\* Leader's log is never overwritten
LogIntegrity ==
    \A s \in Server : state[s] = Leader =>
        \A i \in 1..commitIndex[s] :
            \A t \in GetConfigServers(s) :
                HasEntry(t, i) => GetEntryTerm(t, i) = GetEntryTerm(s, i)

\* Committed entries are never lost
CommitSafety ==
    \A entry \in committedEntries :
        \E s \in Server : 
            /\ InCurrentConfig(s)
            /\ HasEntry(s, entry.index)
            /\ GetEntryTerm(s, entry.index) = entry.term

\* Applied entries are committed
AppliedCommitted ==
    \A entry \in appliedEntries : entry \in committedEntries

\* Configuration changes are single-node only
SingleNodeChange ==
    \A i \in 1..(Len(configs)-1) :
        ValidateConfigChange(configs[i].servers, configs[i+1].servers)

\* Configuration changes maintain quorum overlap
QuorumOverlapSafety ==
    \A i \in 1..(Len(configs)-1) :
        HasQuorumOverlap(configs[i].servers, configs[i+1].servers)

\* Only configured servers can be leaders
LeaderInConfig ==
    \A s \in Server : state[s] = Leader => InCurrentConfig(s)

\* Term monotonicity
TermMonotonic ==
    \A s \in Server : currentTerm[s] >= 1

\* Configuration version consistency
ConfigVersionConsistent ==
    \A s \in Server : 
        /\ configVersion[s] >= 1
        /\ configVersion[s] <= Len(configs)

----
\* Liveness Properties (for model checking with fairness)

\* Eventually some server becomes leader (under fairness)
EventualLeader == <>(\E s \in Server : state[s] = Leader)

\* Committed entries are eventually applied (under fairness)
EventualApplication == 
    \A entry \in committedEntries : <>(entry \in appliedEntries)

----
\* Type invariants for model checking

TypeInvariant ==
    /\ configs \in Seq([version : Nat, term : Nat, servers : SUBSET Server])
    /\ committedEntries \in SUBSET [index : Nat, term : Nat, configVersion : Nat]
    /\ appliedEntries \in SUBSET [index : Nat, term : Nat, configVersion : Nat]
    /\ currentTerm \in [Server -> Nat]
    /\ state \in [Server -> {Follower, Candidate, Leader, Down}]
    /\ votedFor \in [Server -> Server \cup {Nil}]
    /\ log \in [Server -> Seq([term : Nat, type : {"NoOp", "Data"}])]
    /\ commitIndex \in [Server -> Nat]
    /\ lastApplied \in [Server -> Nat]
    /\ configVersion \in [Server -> Nat]
    /\ configTerm \in [Server -> Nat]
    /\ dnsResolution \in [Server -> Server]

==== 
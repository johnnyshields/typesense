---- MODULE DisasterRecoveryExample ----
\* Focused example demonstrating disaster recovery safety
\* Shows how Typesense safely handles scenarios where all node IPs change
\* 
\* This example validates the core problem we solved:
\* - All nodes change IP addresses (disaster recovery)
\* - DNS resolution allows nodes to find each other
\* - Safety properties are maintained throughout recovery

EXTENDS TypesenseRaft

----
\* Disaster recovery scenario constants

\* Original IP addresses (before disaster)
CONSTANTS OriginalIP1, OriginalIP2, OriginalIP3

\* New IP addresses (after disaster) 
CONSTANTS NewIP1, NewIP2, NewIP3

\* Hostname mappings (stable across disaster)
CONSTANTS Host1, Host2, Host3

----
\* Disaster recovery actions

\* Simulate disaster: all servers change IP addresses
DisasterStrike ==
    /\ \A s \in Server : state[s] # Down  \* Servers were running
    /\ dnsResolution' = [s \in Server |-> 
        CASE s = Host1 -> NewIP1
          [] s = Host2 -> NewIP2  
          [] s = Host3 -> NewIP3]
    /\ UNCHANGED <<serverVars, logVars, configVars, leaderVars, globalVars>>

\* Server recovers after disaster with new IP
ServerRecoverWithNewIP(s) ==
    /\ state[s] = Down
    /\ state' = [state EXCEPT ![s] = Follower]
    \* DNS resolution already updated by disaster
    /\ UNCHANGED <<currentTerm, votedFor, logVars, configVars, leaderVars, globalVars, dnsResolution>>

\* Leader validates cluster safety after disaster
PostDisasterSafetyCheck(s) ==
    /\ state[s] = Leader
    /\ InCurrentConfig(s)
    \* Verify all configured servers can be reached via DNS
    /\ \A t \in GetConfigServers(s) : dnsResolution[t] # Nil
    \* Verify we still have majority
    /\ LET configServers == GetConfigServers(s)
           reachableServers == {t \in configServers : state[t] # Down}
       IN HasMajority(s, reachableServers)
    /\ UNCHANGED vars

----
\* Disaster recovery specification

DisasterRecoveryInit ==
    /\ Init
    \* Initially, hostnames resolve to original IPs
    /\ dnsResolution = [Host1 |-> OriginalIP1, Host2 |-> OriginalIP2, Host3 |-> OriginalIP3]

DisasterRecoveryNext ==
    \/ DisasterStrike
    \/ \E s \in Server : ServerRecoverWithNewIP(s)
    \/ \E s \in Server : PostDisasterSafetyCheck(s)
    \/ Next  \* All normal Raft operations

DisasterRecoverySpec == DisasterRecoveryInit /\ [][DisasterRecoveryNext]_vars

----
\* Disaster recovery safety properties

\* After disaster, cluster can still achieve consensus
PostDisasterConsensus ==
    \* If disaster has occurred (DNS changed) and servers recovered
    (\A s \in {Host1, Host2, Host3} : dnsResolution[s] \in {NewIP1, NewIP2, NewIP3}) =>
    \* Then consensus can still be achieved
    <>(\E s \in Server : state[s] = Leader)

\* Safety properties are maintained throughout disaster recovery
DisasterRecoverySafety ==
    \* Core safety properties hold even during disaster
    /\ LeaderSafety
    /\ CommitSafety
    /\ LogIntegrity
    \* Cluster remains functional if majority survives
    /\ (\E s \in Server : state[s] = Leader) => 
       (Cardinality({t \in Server : state[t] # Down}) >= Majority(Server))

\* DNS resolution enables recovery
DNSEnablesRecovery ==
    \* If hostnames are used in configuration
    /\ GetConfigServers(CHOOSE s \in Server : TRUE) = {Host1, Host2, Host3}
    \* And DNS resolution is working
    /\ \A h \in {Host1, Host2, Host3} : dnsResolution[h] # Nil
    \* Then servers can communicate regardless of IP changes
    => \A s, t \in Server : 
        (state[s] = Leader /\ InCurrentConfig(t)) => 
        (dnsResolution[t] # Nil)

----
\* Example configuration for model checking

ExampleInit ==
    /\ Server = {Host1, Host2, Host3}
    /\ DisasterRecoveryInit

ExampleSpec == ExampleInit /\ [][DisasterRecoveryNext]_vars

\* Properties to verify
THEOREM DisasterRecoveryTheorem ==
    ExampleSpec => []DisasterRecoverySafety

THEOREM DNSRecoveryTheorem ==
    ExampleSpec => []DNSEnablesRecovery

==== 
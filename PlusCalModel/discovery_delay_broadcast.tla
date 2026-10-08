---- MODULE discovery_delay_broadcast ----
EXTENDS TLC, Sequences, Integers, FiniteSets

\* Main model parameters, presented in 4.1, first controls participant count, second concurrent inflight messages,
\* third the maximum age messages may have in SPDP cycles
CONSTANTS Numparticipants, MaxInflight, MaxAge

\* Definitions for model structure
Participants == 1..Numparticipants

\* Process definitions, each participant has one SPDP and one SEDP/SNAP process
SpdpProcs == {[Type |-> "SPDP", pID |-> p] : p \in Participants}
SedpProcs  == {[Type |-> "SEDP",  pID |-> p] : p \in Participants}

SPDPMessageStates == {"unconf","newUnconf","conf","newConf"}

participantsKnowledge == SPDPMessageStates \cup {"unknown"}
ParticipantStateType == [root: Participants \cup {0},
                         participants: [Participants -> participantsKnowledge],
                         endpoints: SUBSET Participants]

\* Pluscal definition of the actual model
(* --algorithm discover_delay_broadcast


\* Model state
variables
    \* Tracking which participants have joined currently, joiner process admits them one at a time
    Joined = {};
    \* Presented in 4.1, tracks each participants internal state, so only ps processes may modify this struct
    ParticipantState = [p \in Participants |->
        [root |-> 0,
         participants     |-> [q \in Participants |-> "unknown"],
         endpoints        |-> {}]];
    \* Not explicitly presented in the paper, tracks the participants p wants to announce to and freezes them when the message
    \* is suppose to be emitted. The network process does not model multicast and may, depending on config, only allow one concurrent
    \* announcement, so this buffers the excess messages
    toAnnounce = [p \in Participants |-> {}];
    \* Presented in 4.1, p_pi^{pj} in the paper, participant ps view on qs root,
    PeerRoots = [p \in Participants |-> [q \in Participants |-> 0]];
    \* Presented in 4.1, message queue to be delivered, may hold spdp, snap announcements, snap requests and snap responses
    Messages = {};
    \* Presented in 4.1, T_{pi} in the paper, resync targets for detected missing endpoints, these must survive configured
    \* -> unconfigured transitions as we may get livelocks otherwise, discussed in 4.4 livelock case
    pendingResync = [p \in Participants |-> {}];

\* Helper operators
define

    \* Is participant discovered operator, checks SNAP/SEDP FSM state
    isDiscovered(p) == pc[[Type |-> "SEDP", pID |-> p]] = "fsm_discovered"

    \* Is participant configured operator
    isConfigured(p) == isDiscovered(p) \/ pendingResync[p] # {}

    \* Operators for easier access to the SPDP view on other participants held within "state"
    newConfiguredParticipants(state) == {p \in Participants : state.participants[p] = "newConf"}
    newUnconfiguredParticipants(state) == {p \in Participants : state.participants[p] = "newUnconf"}

    known(state) == {p \in Participants : state.participants[p] # "unknown"}
    \* Election operator, $\preceq$ in the paper, computed based on the local state of some participant
    amLowest(state, self) == \A y \in known(state) \ {self} :
        state.participants[y] \in {"unconf","newUnconf"} => self <= y

    contributionFrom(p) == {p} \cup ParticipantState[p].endpoints

    \* Operator modeling the hash check, for simplicity reduced to simple set inclusion check, simplification discussed at the end of 4.1
    hashOK(self, p) == p \in ParticipantState[self].endpoints

    \* Operator Checks for missing endpoints of known participants
    holes(self) == {q \in Participants :
        /\ \/ /\ ParticipantState[self].participants[q] \in {"conf","newConf"}
              /\ PeerRoots[self][q] = ParticipantState[self].root
           \/ q \in pendingResync[self]
        /\ q \notin ParticipantState[self].endpoints}

    \* If we are missing endpoints, obviously the participant is not complete
    Complete(p) == holes(p) = {}

    \* gossip servers from self's local SPDP view only, no discovery oracle
    gossipServers(self) == {q \in Participants \ {self} :
        ParticipantState[self].participants[q] \in {"conf","newConf"}}

    \* Operator to determine valid snap request (gossip) partners
    validServers(self) == gossipServers(self)

    \* Convenience operator to update the SPDP view
    ApplySpdp(cur, disc, rootChanged) ==
        IF disc THEN IF cur = "newConf" \/ (cur = "conf" /\ ~rootChanged) THEN cur ELSE "newConf"
                ELSE IF cur \in {"unconf","newUnconf"} THEN cur ELSE "newUnconf"

    \* Operator to check if a new SPDP message should be emitted, we save those if all states are in alignment as a
    \* model efficiency measure. If all views are up to date, the message would change nothing
    changeNeeded(d, s, disc) ==
        IF disc THEN \/ ParticipantState[d].participants[s] \notin {"conf","newConf"}
                     \/ PeerRoots[d][s] # ParticipantState[s].root
                ELSE ParticipantState[d].participants[s] \notin {"unconf","newUnconf"}

    \* Definitions / convenienve operators for accessing messages and counts
    SpdpMsgs == {m \in Messages : m.type = "spdp"}
    SedpMsgs == {m \in Messages : m.type \in {"ann","sreq","sresp"}}
    SpdpCount == Cardinality(SpdpMsgs)
    SedpCount == Cardinality(SedpMsgs)

    \* Operator for messages that must be delivered now, as their age is maxAge or even overdue
    Overdue == \E m \in Messages : m.age >= MaxAge
    \* Operator to increase message age by one
    AgeAll(M) == { [m EXCEPT !.age = m.age + 1] : m \in M }

end define;

\* Presented in 4.1, single joiner process, admits participants one by one in arbitrary orders
\* This should complete, so we do not allow endless stuttering and make it a fair process
fair process joiner = [Type |-> "joiner", pID |-> 0]
begin
    join:
        \* Block on overdue messages to force delivery, check if any participants left to join
        await ~Overdue /\ Joined # Participants;
        \* Choose arbitrary participants and add him to joined set, initialized with p.root = p
        with p \in Participants \ Joined do
            ParticipantState[p] := [ParticipantState[p] EXCEPT !.root = p];
            Joined := Joined \cup {p};
        end with;
        \* Iterate until blocked
        goto join;
end process;

\* Presented in 4.1, per participant SPDP process, transmits on-demand/on-view-mismatch SPDP announcements
\* This should always make progress, so strong fairness
fair+ process spdpSend \in SpdpProcs
begin
    spdp_send:
        \* Of course check that no message is overdue
        \* Check that we are joined, we do not exceed MaxInflight, there is a view mismtach and there is not currently another in flight
        await ~Overdue
           /\ self.pID \in Joined
           /\ SpdpCount < MaxInflight
           /\ \E dst \in (Joined \ {self.pID}) :
                /\ changeNeeded(dst, self.pID, isConfigured(self.pID))
                /\ ~(\E m \in SpdpMsgs : m.src = self.pID /\ m.dst = dst);
        \* Pick a destination then ageAll messages
        with dst \in { d \in (Joined \ {self.pID}) :
                /\ changeNeeded(d, self.pID, isConfigured(self.pID))
                /\ ~(\E m \in SpdpMsgs : m.src = self.pID /\ m.dst = d) } do
            \* Then append another SPDP announcement to the message queue, most fields are empty as
            Messages := AgeAll(Messages) \cup
                { [type |-> "spdp", src |-> self.pID, dst |-> dst, age |-> 0,
                   disc |-> isConfigured(self.pID), rt |-> ParticipantState[self.pID].root, eps |-> {}, view |-> {}] };
        end with;
        \* Iterate once again
        goto spdp_send;
end process;

\* Presented in 4.1, single network process, applies/transmits messages to their receivers
fair process net = [Type |-> "net", pID |-> 0]
begin
    net_step:
        \* Await a message that this process should handle, responses are awaited by the receiver Snap FSM
        await \E m \in Messages : m.type \in {"spdp","ann","sreq"};
        with m \in { x \in Messages : x.type \in {"spdp","ann","sreq"} } do
            \* Picked one, now handle it
            if m.type = "spdp" then
                \* Apply SPDP view update, store other participants root, remove message
                ParticipantState[m.dst].participants[m.src] :=
                    ApplySpdp(ParticipantState[m.dst].participants[m.src], m.disc, PeerRoots[m.dst][m.src] # m.rt);
                PeerRoots[m.dst][m.src] := m.rt;
                Messages := Messages \ {m};
            elsif m.type = "ann" then
                \* Update endpoint set and remove message
                ParticipantState[m.dst].endpoints := ParticipantState[m.dst].endpoints \cup {m.src};
                Messages := Messages \ {m};
            else
                \* Message was a gossip request, in the code these are answered without leaving discovered, so without the FSM,
                \* consequently, we just do it inplace by appending a response message with the current responders state and remove the request
                Messages := (Messages \ {m}) \cup
                    { [type |-> "sresp", src |-> m.dst, dst |-> m.src, age |-> 0, disc |-> FALSE,
                       rt |-> ParticipantState[m.dst].root,
                       eps |-> IF m.rs THEN {} ELSE ParticipantState[m.dst].endpoints,
                       view |-> IF m.rs THEN {} ELSE known(ParticipantState[m.dst]), rs |-> m.rs] };
            end if;
        end with;
        \* Iterate again, these processes are all endless loops
        goto net_step;
end process;

\* Presented in 4.1, per participant EDP/Snap process, implements the Snap core FSM
\* This should always make progress, so strong fairness
fair+ process sedpFsm \in SedpProcs
begin
    \* Initial state in the paper FSM
    fsm_initial:
        \* Await either overdue messages or a new participant showing up
        await ~Overdue
           /\ (\/ newConfiguredParticipants(ParticipantState[self.pID]) # {}
               \/ newUnconfiguredParticipants(ParticipantState[self.pID]) # {});

        \* Either choose warm start or cold start path, TC1 or TC2 in the paper
        if newConfiguredParticipants(ParticipantState[self.pID]) # {} then
            \* TC2, do not ack yet, so we can recall who to ask for gossip
            goto fsm_reqgossip;
        else
            \* TC1, set newUnconfigured to just unconfigured to ack the new participant
            ParticipantState[self.pID].participants :=
                [q \in Participants |->
                    IF ParticipantState[self.pID].participants[q] = "newUnconf"
                    THEN "unconf"
                    ELSE ParticipantState[self.pID].participants[q]];
            goto fsm_election;
        end if;

    \* Snapshot state in the paper
    fsm_reqgossip:
        if validServers(self.pID) = {} then
            \* no server left, fall back to Initial
            goto fsm_initial;
        else
            \* We have a valid target, check message properties
            await ~Overdue /\ SedpCount < MaxInflight;
            \* uniform random pick among all configured peers
            with partner \in validServers(self.pID) do
                \* With a valid partner, send a snapshot request for their endpoints
                Messages := Messages \cup
                    { [type |-> "sreq", src |-> self.pID, dst |-> partner,
                       age |-> 0, disc |-> FALSE, rt |-> 0, eps |-> {}, view |-> {}, rs |-> FALSE] };
            end with;
            \* Now await response, in the paper FSM this is TC6
            goto fsm_await_resp;
        end if;

    \* Not directly present in the paper or code, represented by TC6
    fsm_await_resp:
        \* Await a response, should always arrive due to fairness and no loss in the model
        await \E m \in Messages : m.type = "sresp" /\ m.dst = self.pID;
        \* Apply the update: Root, endpoints and finally ack the participant SPDP state
        with m \in { x \in Messages : x.type = "sresp" /\ x.dst = self.pID } do
            ParticipantState[self.pID] := [ParticipantState[self.pID] EXCEPT
                !.root        = IF m.rt < @ THEN m.rt ELSE @,
                !.endpoints   = @ \cup (({m.src} \cup m.eps) \ {self.pID}),
                !.participants = [q \in Participants |->
                    IF q = m.src THEN "newConf"
                    ELSE IF q \in m.view /\ ParticipantState[self.pID].participants[q] = "unknown"
                         THEN "unconf"
                         ELSE ParticipantState[self.pID].participants[q]]];
            \* Update root view and delete message
            PeerRoots[self.pID][m.src] := m.rt;
            Messages := Messages \ {m};
            \* Was this a resync? This state is reused between warm-joins and resyncs
            \* If yes then no need to announce own endpoints again, if not then not
            if m.rs then
                \* resync response, no re-announce, straight back to the gate
                goto fsm_reconcile;
            else
                toAnnounce[self.pID] := Joined \ {self.pID};
                goto fsm_announce;
            end if;
        end with;

    \* Reconciliation state, reflects paper directly
    fsm_reconcile:
        await ~Overdue;
        \* If we are complete, go to discovered
        if Complete(self.pID) then
            pendingResync[self.pID] := {};
            goto fsm_discovered;
        else
            \* Endpoint set is incomplete -> we must perform a resync and retrieve the missing endpoints
            await SedpCount < MaxInflight;
            with target \in holes(self.pID) do
                \* Emit a snapshot request with resync set to true, to indicate retrieval
                Messages := Messages \cup
                    { [type |-> "sreq", src |-> self.pID, dst |-> target,
                       age |-> 0, disc |-> FALSE, rt |-> 0, eps |-> {}, view |-> {}, rs |-> TRUE] };
            end with;
            \* Await response, reuses the warm-join state. On more missing endpoints this may repeat
            goto fsm_await_resp;
        end if;

    \* Announcement state, again reflects paper directly
    fsm_announce:
        \* If nothing to announce in a later iteration, proceed to reconcile state
        if toAnnounce[self.pID] = {} then
            goto fsm_reconcile;
        else
            \* Choose on destination participant to broadcast to, then emit an annoucement message
            await ~Overdue /\ SedpCount < MaxInflight;
            with dst \in toAnnounce[self.pID] do
                Messages := Messages \cup
                    { [type |-> "ann", src |-> self.pID, dst |-> dst, age |-> 0,
                       disc |-> FALSE, rt |-> 0, eps |-> {}, view |-> {}] };
                \* Remove processed destination from toAnnounce target set
                toAnnounce[self.pID] := toAnnounce[self.pID] \ {dst};
            end with;
            \* Iterate until all are completed
            goto fsm_announce;
        end if;

    \* Election state
    fsm_election:
        \* Again await overdue messages
        await ~Overdue;
        \* Prevent race with configured participant, corresponds to TC4
        if newConfiguredParticipants(ParticipantState[self.pID]) # {} then
            \* a configured peer showed up meanwhile, warm start a join instead
            goto fsm_reqgossip;
        \* Election over all visible participants
        elsif amLowest(ParticipantState[self.pID], self.pID) then
            \* Winner, proceed to announcement, corresponds to TC5
            ParticipantState[self.pID] := [ParticipantState[self.pID] EXCEPT !.root = self.pID];
            toAnnounce[self.pID] := Joined \ {self.pID};
            goto fsm_announce;
        else
            \* Lost, return to initial and try again or warm join
            goto fsm_initial;
        end if;

    \* Discovered state
    fsm_discovered:
        \* Await overdue messages or new configured peer case, newConf might have lower GUID requiring merging
        await ~Overdue /\ newConfiguredParticipants(ParticipantState[self.pID]) # {};
        \* Select the lowest GUID newest unconfigured participant
        with p = CHOOSE x \in newConfiguredParticipants(ParticipantState[self.pID]) :
                 \A y \in newConfiguredParticipants(ParticipantState[self.pID]) : x <= y do
            if PeerRoots[self.pID][p] # 0 /\ PeerRoots[self.pID][p] < ParticipantState[self.pID].root then
                \* New participant has lower root -> we adopt their root, join their domain and reannounce
                \* This is a small model code difference, code does the announcement inline, while the model uses the state as an
                \* intermediate
                ParticipantState[self.pID] := [ParticipantState[self.pID] EXCEPT
                    !.root = PeerRoots[self.pID][p]];
                toAnnounce[self.pID] := Joined \ {self.pID};
                goto fsm_announce;
            elsif PeerRoots[self.pID][p] = ParticipantState[self.pID].root /\ ~hashOK(self.pID, p) then
                \* New participant has our root -> joined our domain but Hashes are not correct -> needs reconciliation step,
                \* corresponds to TC 9
                pendingResync[self.pID] := pendingResync[self.pID] \cup {p};
                goto fsm_reconcile;
            else
                \* New regularly joined our domain, no special handling required, just ack new participant
                ParticipantState[self.pID].participants[p] := "conf";
                goto fsm_discovered;
            end if;
        end with;
end process;

end algorithm; *)

\* Model constraint, discussed in 4.2, bounds the state space if all participants are configured,
\* all known, nothing of interest happens anymore and TLC should not explore the trace further
ModelConstraint ==
    \/ Messages # {}
    \/ \E p \in Participants : ParticipantState[p].endpoints # Participants \ {p}
    \/ \E p \in Participants :
         \/ ~isDiscovered(p)
         \/ \E q \in Participants \ {p} : ParticipantState[p].participants[q] = "newConf"
         \/ toAnnounce[p] # {}

\* Discussed in 4.2, eventually always all participants will be discovered and will have all endpoints
EndpointConvergence == <>[](/\ \A p \in Participants : isDiscovered(p)
                    /\ \A p \in Participants : ParticipantState[p].endpoints = Participants \ {p})

\* Not presented in the paper, checks that when two participants are discovered and have mutally seen each
\* other, they hold each others endpoints
EndpointsAfterDiscovery ==
    \A p, q \in Participants :
        (/\ p # q
         /\ isDiscovered(p)
         /\ isDiscovered(q)
         /\ ParticipantState[p].participants[q] = "conf"
         /\ ParticipantState[q].participants[p] = "conf"
         /\ PeerRoots[p][q] = ParticipantState[p].root
         /\ PeerRoots[q][p] = ParticipantState[q].root)
        => (/\ q \in ParticipantState[p].endpoints
            /\ p \in ParticipantState[q].endpoints)

\* Discussed in 4.2, eventually all participants will always have the same root
RootConvergence == <>[](\A p, q \in Participants :
    (/\ p \in Joined /\ q \in Joined
     /\ isDiscovered(p)
     /\ isDiscovered(q))
    => ParticipantState[p].root = ParticipantState[q].root)

\* Also not presented in the paper, property just checks type and range correctness
TypeOK ==
    /\ Joined \subseteq Participants
    /\ \A p \in Participants :
         /\ ParticipantState[p].root \in (Participants \cup {0})
         /\ ParticipantState[p].endpoints \subseteq Participants
         /\ \A q \in Participants :
              ParticipantState[p].participants[q] \in participantsKnowledge
    /\ \A p \in Participants : toAnnounce[p] \subseteq Participants
    /\ \A p \in Participants : pendingResync[p] \subseteq Participants
    /\ \A p \in Participants : \A q \in Participants : PeerRoots[p][q] \in (Participants \cup {0})
    /\ \A m \in Messages :
         /\ m.type \in {"spdp","ann","sreq","sresp"}
         /\ m.src \in Participants
         /\ m.dst \in Participants
         /\ m.age \in 0..MaxAge
         /\ m.view \subseteq Participants
    /\ SpdpCount <= MaxInflight
    /\ SedpCount <= MaxInflight

\* Also not presented in the paper, used for development, if the next transition is disabled due to the model constraint
\* all participants must hold all endpoints or be basically converged
QuiescentImpliesConverged ==
    (~ENABLED Next) => \A p \in Participants : ParticipantState[p].endpoints = Participants \ {p}

\* Presented in 4.2, as we have no leavings in the model, we may only gain endpoint. This property enforces that for all participants
\* the next states ParticipantState'[p].endpoints must be a superset or equal.
EndpointMonotonicity ==
    [][\A p \in Participants :
         ParticipantState[p].endpoints \subseteq ParticipantState'[p].endpoints]_vars

\* Presented in 4.2, as we have no leavings, the root may on descent. The property enforces this but allows self assignment in case of
\* initial self election
RootMonotonicity ==
    [][\A p \in Participants :
         \/ ParticipantState'[p].root <= ParticipantState[p].root
         \/ ParticipantState'[p].root = p]_vars

====

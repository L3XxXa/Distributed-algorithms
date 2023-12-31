import datetime
from collections import defaultdict
from dataclasses import dataclass,field
from messages import *
from typing import List,Dict,Any
from timesource import TimeSource
from threading import Lock

@dataclass(frozen=True,init=True)
class State:
    currentTerm: int = 1
    votedFor: int = 0
    log: List[LogEntry] = field(default_factory=list)

    def log_term(self, index: int = -1):
        if index < 0:
            index = len(self.log)
        if index < 1 or index > len(self.log):
            return 0
        else:
            return self.log[index-1].term

@dataclass(frozen=True,init=True)
class VolatileState:
    commitIndex: int = 0
    lastApplied: int = 0
    nextIndex: Dict[int,int] = field(default_factory=lambda: defaultdict(int))
    matchIndex: Dict[int,int] = field(default_factory=lambda: defaultdict(int))
    votes: Dict[int,bool] = field(default_factory=lambda: defaultdict(bool))

    def with_set_votes(self, votes):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=votes)

    def with_last_applied(self, index):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=index, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)
    
    def with_commit_index(self, index):
        return VolatileState(commitIndex=index, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)

    def with_next_index(self, d):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex|d, matchIndex=self.matchIndex, votes=self.votes)

    def with_match_index(self, d):
        return VolatileState(commitIndex=self.commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex|d, votes=self.votes)

    def with_commit_advance(self, nservers: int, lastIndex: int, state: State):
        indices = list(self.matchIndex.values())
        indices.append(lastIndex)
        while len(indices)<nservers:
            indices.append(0)
        indices.sort()
        commitIndex=max(self.commitIndex, indices[nservers//2])
        if state.log_term(commitIndex)==state.currentTerm:
            return VolatileState(commitIndex=commitIndex, lastApplied=self.lastApplied, nextIndex=self.nextIndex, matchIndex=self.matchIndex, votes=self.votes)
        else:
            return self

@dataclass(frozen=True,init=True)
class Result:
    next_state: Any = None
    next_volatile_state: Any = None
    next_state_func: Any = None
    update_last_time: bool = False
    message: Any = None
    messages: Any = None

class Raft:
    def __init__(self, id: int, nodes, ts: TimeSource = TimeSource()):
        self.id = id
        self.nodes = nodes
        self.dht = {}
        self.ts = ts
        self.nservers = len(nodes)+1
        self.min_votes = (self.nservers + 1)//2
        self.npeers = len(nodes)
        self.state = State()
        self.lock = Lock()
        self.volatile_state = VolatileState()
        self.state_func = self.follower
        self.last_time = self.ts.now()

    def on_append_entries(self, message: AppendEntriesRequest, state: State, volatile_state: VolatileState):
        if message.term < state.currentTerm:
            return Result(
                message=AppendEntriesResponse(src=self.id, dst=message.src, term=state.currentTerm, success=False, matchIndex=0),
                update_last_time=True
            )

        assert(message.term == state.currentTerm)

        matchIndex=0
        commitIndex=volatile_state.commitIndex
        success=False
        if (message.prevLogIndex==0 or (message.prevLogIndex <= len(state.log) and state.log_term(message.prevLogIndex)==message.prevLogTerm)):
            success=True
            index=message.prevLogIndex
            log=state.log
            for entry in message.entries:
                index=index+1
                if state.log_term(index) != entry.term:
                    while len(log) > index-1:
                        log.pop()
                    log.append(entry)
            matchIndex=index
            commitIndex=max(commitIndex, message.leaderCommit)
        return Result(
            message=AppendEntriesResponse(src=self.id, dst=message.src, term=message.term, success=success, matchIndex=matchIndex),
            next_volatile_state=volatile_state.with_commit_index(commitIndex),
            next_state_func=self.follower,
            update_last_time=True
        )

    def on_request_vote(self, message: RequestVoteRequest, state: State, volatile_state: VolatileState):
        print(f"Node {self.id} requested vote")
        if message.term < state.currentTerm:
            return Result(
                message=RequestVoteResponse(src=self.id, dst=message.src, term=state.currentTerm, voteGranted=False)
            )
        elif message.term == state.currentTerm:
            accept=False
            if state.votedFor == 0:
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm > state.log_term():
                accept=True
            elif state.votedFor == message.candidateId and message.lastLogTerm == state.log_term() and message.lastLogIndex >= len(state.log):
                accept=True

            return Result(
                next_state=State(currentTerm=message.term, votedFor=message.candidateId, log=state.log),
                message=RequestVoteResponse(src=self.id, dst=message.src, term=message.term, voteGranted=accept)
            )
        
    def _reset_dht(self):
        for i in range (0, len(self.state.log)):
            if(self.state.log[i].operation_type == 'set'):
                self.dht[self.state.log[i].key] = self.state.log[i].value
    def _create_vote(self, state):
        return RequestVoteRequest(
            src=self.id,
            dst=0,
            term=state.currentTerm+1,
            candidateId=self.id,
            lastLogIndex=len(state.log),
            lastLogTerm=0 if len(state.log)==0 else state.log[-1].term
        )
    
    def _create_requst_for_two_phase_commit(self, state, volatile_state, nodeId):
        prevIndex = volatile_state.nextIndex[nodeId] - 1
        lastIndex = min(prevIndex+1,len(state.log))
        if volatile_state.matchIndex[nodeId]+1 < volatile_state.nextIndex[nodeId]:
            lastIndex = prevIndex
        return TwoPhaseCommitRequest(
            src=self.id,
            dst=nodeId,
            term=state.currentTerm,
            leaderId=self.id,
            prevLogIndex=prevIndex,
            prevLogTerm=state.log_term(prevIndex),
            entries=state.log[prevIndex:lastIndex],
            leaderCommit=min(volatile_state.commitIndex,lastIndex)
        )


    def _create_append_entries(self, state, volatile_state, nodeId):
        prevIndex = volatile_state.nextIndex[nodeId] - 1
        lastIndex = min(prevIndex+1,len(state.log))
        print(f'\n{nodeId} match index {volatile_state.matchIndex[nodeId]} {lastIndex} {prevIndex} {volatile_state.commitIndex}\n')
        if volatile_state.matchIndex[nodeId]+1 < volatile_state.nextIndex[nodeId]:
            lastIndex = prevIndex
        return AppendEntriesRequest(
            src=self.id,
            dst=nodeId,
            term=state.currentTerm,
            leaderId=self.id,
            prevLogIndex=prevIndex,
            prevLogTerm=state.log_term(prevIndex),
            entries=state.log[prevIndex:lastIndex],
            leaderCommit=min(volatile_state.commitIndex,lastIndex)
        )

    def follower(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last > Timeout.Election):
                print('=============\nElection timeout\n=============')
                return Result(
                    next_state_func=self.candidate,
                    update_last_time=True
                )
        elif isinstance(message, TwoPhaseCommitRequest):
            return Result(
            message=TwoPhaseCommitResponse(src=self.id, dst=message.src, can_append=True),
            next_volatile_state=volatile_state.with_commit_index(volatile_state.commitIndex),
            next_state_func=self.follower,
            update_last_time=True
        )        
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, AppendEntriesRequest):
            return self.on_append_entries(message, state, volatile_state)
        # elif isinstance(message, GetValueByKeyRequest):


        return None        

    def _on_set_value(self, key, value):
        with self.lock:
            self.dht[key]=value
            print(self.dht)
            return SetKeyToValueResponse()
    
    def _on_get_value(self, key):
        with self.lock:
            value = ''
            if(key in self.dht):
                value = self.dht[key]
                return GetValueByKeyResponse(value)
            else:
                value = f'Not found item for {key}'
                return ErrorMessage(f'Not found value for key {key}')

    def candidate(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        if isinstance(message, Timeout):
            if (now - last > Timeout.Election):
                return Result(
                    next_state=State(currentTerm=state.currentTerm+1,votedFor=self.id, log=state.log),
                    next_volatile_state=VolatileState(),
                    update_last_time=True,
                    message=self._create_vote(state)
                )
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, RequestVoteResponse):
            votes = volatile_state.votes
            if message.term > state.currentTerm:
                return Result(
                    next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor, log=state.log),
                    next_state_func=self.follower,
                    update_last_time=True
                )
            # print(message.voteGranted)
            if message.voteGranted and message.term == state.currentTerm:
                votes = votes|{message.src: True}
            # print(votes)
            nvotes = len(list(filter(lambda x:x, votes.values()))) + 1
            print(f"Need/total {self.min_votes}/{nvotes}")
            if nvotes >= self.min_votes:
                print('===========')
                print(f'{self.id} is now leader')
                print('===========')
                value = len(state.log)+1
                next_indices = {key: value for key in self.nodes.keys()}
                self._reset_dht()
                return Result(
                    next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor, log=state.log),
                    next_volatile_state=VolatileState(
                        commitIndex=volatile_state.commitIndex,
                        lastApplied=volatile_state.lastApplied,
                        nextIndex=next_indices,
                    ),
                    next_state_func=self.leader,
                    update_last_time=True
                )
            return Result(
                next_state=State(currentTerm=state.currentTerm,votedFor=state.votedFor, log=state.log),
                next_volatile_state=volatile_state.with_set_votes(votes),
            )
        elif isinstance(message, AppendEntriesRequest):
            return self.on_append_entries(message, state, volatile_state)

        return None

    def leader(self, now: datetime, last: datetime, message, state: State, volatile_state: VolatileState) -> Result:
        # print(f'node {self.id} is leader')
        if isinstance(message, Timeout):
            if (now - last) > Timeout.Heartbeat:
                return Result(
                    update_last_time=True,
                    messages=[self._create_requst_for_two_phase_commit(state, volatile_state, nodeId) for nodeId in self.nodes.keys()]
                )
        elif(isinstance(message, TwoPhaseCommitResponse)):
            print(f'response {message.can_append}')
            return Result(
                    update_last_time=True,
                    messages=[self.on_append_entries(state, volatile_state, nodeId) for nodeId in self.nodes.keys()]
                )
        elif isinstance(message, AppendEntriesResponse):
            print('append entries')
            if message.term == state.currentTerm:
                nodeId=message.src
                if message.success:
                    matchIndex = max(volatile_state.matchIndex[nodeId], message.matchIndex)
                    return Result(
                        next_volatile_state=volatile_state.with_match_index({nodeId: matchIndex}).with_next_index({nodeId: message.matchIndex+1}).with_commit_advance(self.nservers,len(state.log),state)
                    )
                else:
                    return Result(
                        next_volatile_state=volatile_state.with_next_index({nodeId: max(1, volatile_state.nextIndex[nodeId]-1)})
                    )
        elif isinstance(message, CommandRequest):
            log=state.log
            log.append(LogEntry(term=state.currentTerm, operation_type=message.operation_type, key=message.key, value=message.value))
            return Result(
                next_state=State(currentTerm=state.currentTerm, votedFor=state.votedFor, log=log),
                next_volatile_state=volatile_state.with_commit_advance(self.nservers,len(log),state),
                message=CommandResponse()
            )
        elif isinstance(message, SetKeyToValueRequest):
            log = state.log
            log.append(LogEntry(term=state.currentTerm, operation_type='set', key=message.key, value=message.value))
            message_result = self._on_set_value(key=message.key, value = message.value)
            return Result(
                next_state=State(currentTerm=state.currentTerm, votedFor=state.votedFor, log=log),
                next_volatile_state=volatile_state.with_commit_advance(self.nservers,len(log),state),
                message=message_result
            ) 
        elif isinstance(message, GetValueByKeyRequest):
            log = state.log
            log.append(LogEntry(term=state.currentTerm, operation_type='get', key=message.key, value=''))
            message_result = self._on_get_value(message.key)
            return Result(
                next_state=State(currentTerm=state.currentTerm, votedFor=state.votedFor, log=log),
                next_volatile_state=volatile_state.with_commit_advance(self.nservers,len(log),state),
                message=message_result
            )
        
        elif isinstance(message, RequestVoteRequest):
            return self.on_request_vote(message, state, volatile_state)
        elif isinstance(message, RequestVoteResponse):
            pass

        return None

    def become(self, state_func):
        if self.state_func != state_func:
            print(f"Node {self.id} changed state: {self.state_func.__name__}->{state_func.__name__}")
            self.state_func = state_func
            self.process(Timeout(), None)

    def process(self, message, replyto=None):
        now = self.ts.now()
        if not isinstance(message,Timeout) and not isinstance(message,CommandRequest) and not isinstance(message,SetKeyToValueRequest) and not isinstance(message, GetValueByKeyRequest) and not isinstance(message, TwoPhaseCommitResponse) and message.term > self.state.currentTerm:
            self.state=State(currentTerm=message.term, votedFor=0, log=self.state.log)
            self.state_func=self.follower
        self.apply_result(now, self.state_func(now, self.last_time, message, self.state, self.volatile_state), replyto)

    def apply_result(self, now, result, replyto=None):
        if result:
            if result.update_last_time:
                self.last_time = now
            if result.next_state:
                self.state = result.next_state
            if result.next_volatile_state:
                self.volatile_state = result.next_volatile_state
            if result.message:
                if isinstance(result.message,CommandResponse):
                    if replyto:
                        replyto.send(result.message)
                elif isinstance(result.message, SetKeyToValueResponse):
                    if replyto:
                        replyto.send(result.message)
                elif isinstance(result.message, GetValueByKeyResponse):
                    if replyto:
                        replyto.send(result.message)
                elif isinstance(result.message, ErrorMessage):
                    if replyto:
                        replyto.send(result.message)
                else:
                    if result.message.dst == 0:
                        for k,v in self.nodes.items():
                            v.send(result.message)
                    else:
                        self.nodes[result.message.dst].send(result.message)
            if result.messages:
                for m in result.messages:
                    self.nodes[m.dst].send(m)

            if result.next_state_func:
                self.become(result.next_state_func)

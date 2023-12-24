import pickle
import struct
import random
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List

@dataclass(frozen=True)
class LogEntry:
    term: int = 1
    operation_type: bytes = None
    key: bytes = None
    value: bytes = None


@dataclass
class RequestVoteRequest:
    src: int
    dst: int
    term: int
    candidateId: int
    lastLogIndex: int
    lastLogTerm: int

@dataclass
class RequestVoteResponse:
    src: int
    dst: int
    term: int
    voteGranted: bool

@dataclass
class SetKeyToValueRequest:
    key: bytes
    value: bytes

@dataclass
class SetKeyToValueResponse:
    pass

@dataclass 
class GetValueByKeyRequest:
    key: bytes

@dataclass
class GetValueByKeyResponse:
    value: bytes


@dataclass
class AppendEntriesRequest:
    src: int
    dst: int
    term: int
    leaderId: int
    prevLogIndex: int
    prevLogTerm: int
    leaderCommit: int
    entries: List[LogEntry] = field(default_factory=list)

@dataclass
class AppendEntriesResponse:
    src: int
    dst: int
    term: int
    success: bool
    matchIndex: int

@dataclass
class ErrorMessage:
    e: bytes

@dataclass
class CommandRequest:
    operation_type: bytes
    key: bytes
    value: bytes

@dataclass
class CommandResponse:
    pass

class Timeout:
    Election = timedelta(seconds = random.randint(3, 6))
    Heartbeat = timedelta(seconds = 2)
    LockTimeout = timedelta(seconds=1)

def serialize(data):
    payload = pickle.dumps(data)
    header = struct.pack('i', len(payload))
    return header,payload

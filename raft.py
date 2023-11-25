import os
from hashtable import HashTable
from commit_log import CommitLog
from pathlib import Path
from random import random, randint

class Raft:
    def __init__(self, ip, port, partitions) -> None:
        self.ip = ip
        self.port = port
        self.hash_table = HashTable()
        self.commit_log = CommitLog(file = f'commit-log-{self.ip}:{self.port}.txt')
        self.partitions = eval(partitions)
        self.cluster_index = -1
        self.server_index = -1
        self.connections = [[None] * (len((self.partitions[i]) for i in range(len(self.partitions))))]
        commit_log_file = Path(self.commit_log.file)
        commit_log_file.touch(exist_ok = True)
        for i in range(len(self.partitions)):
            cluster = self.partitions[i]
            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)
                
                if (ip, port) == (self.ip, self.port):
                    self.cluster_index = i
                    self.server_index = j
                    
                else: 
                    self.conns[i][j] = (ip, port)
        self.current_term = 1
        self.voted_for = -1
        self.votes = set()
        self.state = 'follower' if len(self.partitions[self.cluster_index]) > 1 else 'leader'
        self.leader_id = -1
        self.commit_index = 0
        self.next_indices = [0] * len(self.partitions[self.cluster_index])
        self.match_indices = [-1] * len(self.partitions[self.cluster_index])
        self.election_period = randint(1000, 5000)
        self.rpc_period = 3000 
        self.election_timeouit = -1
        self.rpc_timeoute = [-1] * len(self.partitions[self.cluster_index]) #Таймаут для передачи сообщения из сервера через сокет
        print('starting')
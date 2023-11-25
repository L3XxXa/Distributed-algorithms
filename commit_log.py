from threading import Lock
import datetime

class CommitLog:
    def __init__(self, file='logs.txt') -> None:
        self.file = file
        self.lock = Lock()
        self.last_term = 0
        self.last_index = -1

        def get_last_index_term(self):
            with self.lock:
                return self.last_index, self.last_term
            
        def commit_log(self, term, command):
            with self.lock:
                with open(self.file, 'a') as opened_file:
                    message = f'${datetime.now().strftime("%H:%M:%S")} : {term} : {command}'
                    opened_file.write(f'{message}\n')
                    self.last_term = term
                    self.last_index += 1
                return self.last_index, self.last_term
            
        def commit_multiple_logs_and_truncate(self, term, commands, start):
            idx = 0
            with self.lock:
                with open(self.file, 'a') as opened_file:
                    if(len (commands) > 0):
                        for i in range(0, len(commands)):
                            if(idx >= start):
                                message = f'${datetime.now().strftime("%H:%M:%S")} : {term} : {commands[i]}'
                                opened_file.write(f'{message}\n')
                                if idx > self.last_index:
                                    self.last_term = term
                                    self.last_index = idx
                            idx+=1
                        opened_file.truncate()
                return self.last_index, self.last_term

        def read_all_log(self):
            with self.lock:
                logs = []
                with open(self.file, 'r') as opened_file:
                    for line in opened_file:
                        date, term, command = line.strip.split(' : ')
                        logs += [(date, term, command)]

                return logs
            
        def read_logs_with_indices(self, start, end=None):
            with self.lock:
                logs = []
                idx = 0
                with open(self.file, 'r') as opened_file:
                    for line in opened_file:
                        if(idx >= start):
                            date, term, command = line.strip.split(' : ')
                            logs += [(date, term, command)]
                            idx += 1
                        if(end and idx > end):
                            break
                return logs
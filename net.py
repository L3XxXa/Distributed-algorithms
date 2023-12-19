from datetime import datetime,timedelta
import asyncio
from messages import Timeout
from node import Sender,Receiver

class Net:
    def __init__(self, nodes, raft):
        self.nodes = nodes
        self.raft = raft
        for k,v in self.nodes.items():
            v.start(self.handle_request)

    async def handle_request(self, reader, writer):
        sender = Sender(writer)
        receiver = Receiver(reader)
        while True:
            obj = await receiver.rcv()
            print(f"Received: {obj}")
            self.raft.process(obj, sender)
            await writer.drain()

    async def connector(self):
        while True:
            for k,v in self.nodes.items():
                await v.drain()
            await asyncio.sleep(0.1)

    async def idle(self):
        t0=datetime.now()
        dt=timedelta(seconds=2)
        while True:
            self.raft.process(Timeout())
            t1=datetime.now()
            if t1>t0+dt:
                print(f"State: {self.raft.state}")
                t0=t1
            await asyncio.sleep(0.01)

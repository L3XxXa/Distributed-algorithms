import asyncio
import sys
from node import Node
from raft import Raft
from net import Net

async def main():
    nodes = {}
    myid = int(sys.argv[1])
    servers = [(1,"::1",8888),(2,"::1",8889),(3,"::1",8890)]
    for (id,host,port) in servers:
        if id == myid:
            myhost,myport = host, port
        else:
            nodes[id] = Node(id, host, port)

    raft = Raft(myid, nodes)
    net = Net(nodes, raft)
    asyncio.create_task(net.connector())
    asyncio.create_task(net.idle())
    print(f'starting server on {myhost}:{myport}')
    server = await asyncio.start_server(net.handle_request, myhost, myport)
    print(f'Server started successfully on {myhost}:{myport}')
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import sys
from messages import CommandRequest
from node import Node

async def main():
    servers = [(1,"::1",8888),(2,"::1",8889),(3,"::1",8890)]    
    i = int(sys.argv[1]) - 1

    node = Node(servers[i][0], servers[i][1], servers[i][2])
    await node.connect()
    print("Connected")

    try:
        for line in sys.stdin:
            data=line.strip()
            print(f"Send {data}")
            node.send(CommandRequest(data))
            await node.drain()
            await node.rcv()
            print("Done")
    except Exception as ex:
        print(f"Exception: {ex}")


if __name__ == "__main__":
    asyncio.run(main())

# -*- coding: utf-8 -*-

import asyncio
import json

from asyncio import PriorityQueue
from datetime import datetime
from socket import *


class JsonServer:

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.queue = PriorityQueue(loop=self.loop)
        self.loop.create_task(self.json_server(('', 25000)))
        self.loop.create_task(self.queue_dumper())
        self.loop.run_forever()

    async def json_server(self, address):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock.bind(address)
        sock.listen(5)
        sock.setblocking(False)
        while True:
            client, addr = await self.loop.sock_accept(sock)
            print('Connection from', addr)
            self.loop.create_task(self.json_handler(client))

    async def json_handler(self, client):
        with client:
            while True:
                raw_data = await self.loop.sock_recv(client, 10000)
                if not raw_data:
                    break
                print("Just received data ", raw_data)
                try:
                    data = json.loads(raw_data.strip().decode("utf-8"))
                except:
                    # In case a valid json dose not come in ignore incoming message and ignore it.
                    # TODO: Log it.
                    print("Something went wrong while parsing json.")
                    await self.loop.sock_sendall(client, b'Rejected: ' + raw_data)
                    break
                if data and data['action'] == 'apply':
                    # TODO Change first condition above to actually check if json is valid as expected.
                    ts = datetime.strptime(data['when'], '%Y-%m-%d %H:%M:%S')
                    print('Updating queue')
                    await self.queue.put((ts, data['template']))
                    await self.loop.sock_sendall(client, b'Accepted: ' + raw_data)
                else:
                    await self.loop.sock_sendall(client, b'Rejected: ' + raw_data)
        print('Connection closed')

    async def queue_dumper(self):
        while True:
            if not self.queue.qsize():
                print("Nothing in queue to print. Will try later.")
                await asyncio.sleep(5)
            else:
                print("Got something in queue. Here is what I see:")
                _copy = PriorityQueue()
                while not self.queue.empty():
                    await _copy.put(await self.queue.get())
                while not _copy.empty():
                    element = await _copy.get()
                    print(element)
                    await self.queue.put(element)
            await asyncio.sleep(5)

if __name__ == '__main__':
    server = JsonServer()

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
            self.loop.create_task(self.json_handler(client))

    async def json_handler(self, client):
        with client:
            while True:
                raw_data = await self.loop.sock_recv(client, 10000)
                if not raw_data:
                    break
                try:
                    data = json.loads(raw_data.strip().decode("utf-8"))
                except:
                    # In case a valid json dose not come in ignore incoming message and ignore it.
                    # TODO: Log it.
                    await self.loop.sock_sendall(client, b'Rejected: ' + raw_data)
                    break
                if self.is_valid_data_input(data) and data['action'] == 'apply':
                    # TODO Change first condition above to actually check if json is valid as expected.
                    ts = datetime.strptime(data['when'], '%Y-%m-%d %H:%M:%S')
                    await self.queue.put((ts, data['template']))
                    await self.loop.sock_sendall(client, b'Accepted: ' + raw_data)
                else:
                    await self.loop.sock_sendall(client, b'Rejected: ' + raw_data)

    def is_valid_data_input(self, data):
        if not data.get('action'):
            return False
        if not data.get('when'):
            return False
        if not data.get('template'):
            return False
        return True

    async def queue_dumper(self):
        """Dumps status of queue to terminal (Eventually a file) every second."""
        # TODO : Ensure this also prints to a file.
        while True:
            if not self.queue.qsize():
                await asyncio.sleep(1)
            else:
                _copy = PriorityQueue()
                while not self.queue.empty():
                    await _copy.put(await self.queue.get())
                print(chr(27) + "[2J") # Bit of Ctr + L magic trick
                while not _copy.empty():
                    element = await _copy.get()
                    print(element)
                    await self.queue.put(element)
            await asyncio.sleep(1)

if __name__ == '__main__':
    server = JsonServer()
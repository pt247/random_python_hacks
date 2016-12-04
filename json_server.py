# -*- coding: utf-8 -*-

import asyncio
import json

from asyncio import PriorityQueue
from datetime import datetime
from socket import *

loop = asyncio.get_event_loop()

async def json_server(address, queue):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen(5)
    sock.setblocking(False)
    while True:
        client, addr = await loop.sock_accept(sock)
        print('Connection from', addr)
        loop.create_task(json_handler(client, queue))

async def json_handler(client, queue):
    with client:
        while True:
            raw_data = await loop.sock_recv(client, 10000)
            if not raw_data:
                break
            print("Just received data ", raw_data)
            try:
                data = json.loads(raw_data.strip().decode("utf-8"))
            except:
                # In case a valid json dose not come in ignore incoming message and ignore it.
                # TODO: Log it.
                print("Something went wrong while parsing json.")
                await loop.sock_sendall(client, b'Rejected: ' + raw_data)
                break
            if data and data['action'] == 'apply':
                # TODO Change first condition above to actually check if json is valid as expected.
                ts = datetime.strptime(data['when'], '%Y-%m-%d %H:%M:%S')
                print('Populating queue')
                print('Size of queue', queue.qsize())
                await queue.put((ts, data['template']))
                print('Size of updated queue', queue.qsize())
                await loop.sock_sendall(client, b'Accepted: ' + raw_data)
            else:
                await loop.sock_sendall(client, b'Rejected: ' + raw_data)

    print('Connection closed')

async def queue_dumper(queue):
    while True:
        if not queue.qsize():
            print("Nothing in queue to print. Will try later.")
            await asyncio.sleep(5)
        else:
            print("Got something in queue. Here is what I see:")
            _copy = PriorityQueue()
            while not queue.empty():
                await _copy.put(await queue.get())
            while not _copy.empty():
                element = await _copy.get()
                print(element)
                await queue.put(element)
        await asyncio.sleep(5)

queue = PriorityQueue(loop=loop)
loop.create_task(json_server(('', 25000), queue))
loop.create_task(queue_dumper(queue))
loop.run_forever()
loop.close()

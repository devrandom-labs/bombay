"""Observe native TLS transport milestones; no actor or crypto admission credit."""
import asyncio
import enum
import hashlib
import json
from pathlib import Path
import socket
import sys

PROTECTED_TEXT = '{ "amount": 9007199254740993, "marker": "static-remote-fixture" }\n'
CERTIFICATES = Path(sys.argv[2])
EVIDENCE = Path(sys.argv[3])

class ObservationCase(enum.Enum):
    RECEIPT = 'receipt'
    CANCELLED = 'cancelled'
    REJECTED = 'rejected'
    LATE_EXIT = 'late_exit'

class TransportWorker:
    def __init__(self, role, executable, layout, operation, address, directory):
        self.role = role
        self.arguments = [executable, role, layout, operation, address, str(CERTIFICATES)]
        self.directory = directory
        self.events = []
        self.child = None
        self.stderr = None

    async def start(self):
        self.stderr = (self.directory / (self.role + '.stderr')).open('wb')
        self.child = await asyncio.create_subprocess_exec(
            *self.arguments, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=self.stderr)
        event = await self.observe('ready')
        assert event['role'] == self.role, event

    async def command(self, command):
        self.child.stdin.write((json.dumps(command) + '\n').encode())
        await self.child.stdin.drain()

    async def observe(self, expected):
        encoded = await asyncio.wait_for(self.child.stdout.readline(), 12)
        assert encoded, (self.role, 'unexpected EOF', self.child.returncode)
        event = json.loads(encoded)
        self.events.append(event)
        assert event['event'] == expected, (self.role, expected, event)
        return event

    async def close(self):
        if self.child is None or self.child.returncode is not None:
            raise AssertionError((self.role, 'unexpected exit before close observation', self.child.returncode if self.child else None))
        await self.command('EXIT')
        event = await self.observe('session_close_returned')
        assert event['role'] == self.role, event
        self.child.stdin.close()
        exit_code = await asyncio.wait_for(self.child.wait(), 12)
        assert exit_code == 0, (self.role, exit_code)
        if self.stderr:
            self.stderr.close()

    async def abandon(self):
        if self.child and self.child.returncode is None:
            self.child.kill()
            await self.child.wait()
        if self.stderr:
            self.stderr.close()

async def campaign(executable, layout, operation, case):
    directory = EVIDENCE / (layout + '-' + operation + '-' + case.value)
    directory.mkdir(parents=True, exist_ok=True)
    with socket.socket() as port_claim:
        port_claim.bind(('127.0.0.1', 0))
        address = 'tls/localhost:' + str(port_claim.getsockname()[1])
    roles = ['router', 'recipient', 'caller'] if layout == 'router_client' else ['recipient', 'caller']
    workers = [TransportWorker(role, executable, layout, operation, address, directory) for role in roles]
    record = {'layout': layout, 'operation': operation, 'case': case.value,
              'address': address, 'binary_sha256': hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
              'failures': [], 'workers': []}
    try:
        for worker in workers:
            await worker.start()
        recipient, caller = workers[-2:]
        if case is ObservationCase.REJECTED:
            await caller.command('RELEASE')
            event = await caller.observe('worker_failed')
            assert event['cause'] == 'caller cannot release recipient work', event
            event = await caller.observe('session_close_returned')
            assert event['role'] == 'caller', event
            exit_code = await asyncio.wait_for(caller.child.wait(), 12)
            assert exit_code == 1, exit_code
        else:
            await caller.command('RUN')
            for milestone in ['potentially_transmitted', 'invocation_returned']:
                event = await caller.observe(milestone)
                assert event['operation'] == operation, event
            if case is ObservationCase.CANCELLED:
                await caller.command('CANCEL')
                await caller.observe('wait_cancelled')
            await recipient.command('RELEASE')
            received = await recipient.observe('received')
            assert received['protected_text'].encode() == PROTECTED_TEXT.encode(), received
            for milestone in ['potentially_transmitted', 'invocation_returned']:
                event = await recipient.observe(milestone)
                assert event['operation'] == operation, event
            if case in (ObservationCase.RECEIPT, ObservationCase.LATE_EXIT):
                await caller.command('WAIT')
                event = await caller.observe('receipt')
                assert event['receipt'] == {'protected_text': PROTECTED_TEXT, 'observations': 1}, event
        if case is ObservationCase.LATE_EXIT:
            caller.child.kill()
            await caller.child.wait()
        remaining = workers[:-1] if case is ObservationCase.REJECTED else workers
        for worker in reversed(remaining):
            await worker.close()
    except Exception as cause:
        record['failures'].append(repr(cause))
    finally:
        for worker in workers:
            await worker.abandon()
            record['workers'].append({'role': worker.role, 'argv': worker.arguments,
                                      'exit': worker.child.returncode if worker.child else None,
                                      'events': worker.events})
        (directory / 'observations.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps({key: record[key] for key in ['layout', 'operation', 'case', 'failures']}), flush=True)
    return record

async def observe_campaigns(executable):
    records = []
    for layout in ['peer', 'router_client']:
        for operation in ['query', 'publication']:
            for case in ObservationCase:
                records.append(await campaign(executable, layout, operation, case))
    EVIDENCE.mkdir(exist_ok=True)
    (EVIDENCE / 'campaigns.json').write_text(json.dumps(records, indent=2) + '\n')
    for record in records:
        if record['case'] == ObservationCase.LATE_EXIT.value:
            expected = AssertionError(('caller', 'unexpected exit before close observation', -9))
            assert record['failures'] == [repr(expected)], record
        elif record['failures']:
            raise SystemExit(1)

if __name__ == '__main__':
    asyncio.run(observe_campaigns(sys.argv[1]))

from __future__ import annotations

import pytest
from membot.agent.persistence.redis_transport import QueueFullError, RedisStreamTransport


class FakeRedis:
    def __init__(self):
        self.entries = []
        self.pending = {}
        self.counter = 0

    async def xgroup_create(self, **kwargs):
        return True

    async def xlen(self, key):
        return len(self.entries)

    async def xadd(self, key, fields, **kwargs):
        self.counter += 1
        entry = (f"{self.counter}-0", fields)
        self.entries.append(entry)
        return entry[0]

    async def xreadgroup(self, **kwargs):
        entries = self.entries[:]
        self.entries.clear()
        for entry in entries:
            self.pending[entry[0]] = entry
        return [("stream", entries)] if entries else []

    async def xack(self, key, group, entry_id):
        return int(self.pending.pop(entry_id, None) is not None)

    async def xdel(self, key, entry_id):
        return 1

    async def xautoclaim(self, *args, **kwargs):
        entries = list(self.pending.values())
        return "0-0", entries, []


@pytest.mark.asyncio
async def test_stream_ack_and_pending_recovery_are_at_least_once():
    redis = FakeRedis()
    transport = RedisStreamTransport(redis, max_depth=1, prefetch=1)
    first_id = await transport.publish({"invocationId": "one"})
    assert first_id == "1-0"
    with pytest.raises(QueueFullError):
        await transport.publish({"invocationId": "two"})
    delivered = await transport.read()
    assert delivered[0][1] == {"invocationId": "one"}
    recovered = await transport.recover_pending()
    assert recovered[0][0] == first_id
    assert await transport.ack(first_id) == 1

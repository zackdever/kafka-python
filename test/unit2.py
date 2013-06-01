import os
import random
import struct
import unittest

from kafka.consumer import SimpleConsumer 
from kafka.client import KafkaClient
from kafka.common import (
    ErrorMapping, OffsetAndMessage, OffsetCommitResponse, Message, FetchResponse
)

class MockClient(object):
    def __init__(self):
        self.topic_partitions = {"topic": [0]}
        self.mock_committed_offsets = {}

    def send_fetch_request(self, reqs):
        resps = []
        for req in reqs:
            msgs = [OffsetAndMessage(0, Message(0, 0, "key", "value1")),
                    OffsetAndMessage(1, Message(0, 0, "key", "value2")),
                    OffsetAndMessage(2, Message(0, 0, "key", "value3")),
                    OffsetAndMessage(3, Message(0, 0, "key", "value4")),
                    OffsetAndMessage(4, Message(0, 0, "key", "value5"))]
            resp = FetchResponse(req.topic, req.partition, ErrorMapping.NO_ERROR, 0, msgs)
            resps.append(resp)
        return resps

    def send_offset_commit_request(self, group, reqs):
        resps = []
        for req in reqs:
            self.mock_committed_offsets[(req.topic, req.partition)] = req.offset
            resp = OffsetCommitResponse(req.topic, req.partition, ErrorMapping.NO_ERROR)
            resps.append(resp)
        return resps

    def _load_metadata_for_topics(self, topic):
        pass

class TestConsumer(unittest.TestCase):
    def test_offsets(self):
        client = MockClient()
        consumer = SimpleConsumer(client, "group", "topic", auto_commit=False)
        it = iter(consumer)
        m = it.next()
        self.assertEquals(m.offset, 0)
        self.assertEquals(consumer.offsets[0], 0)
        m = it.next()
        self.assertEquals(m.offset, 1)
        self.assertEquals(consumer.offsets[0], 1)
        consumer.commit()

        print(client.mock_committed_offsets)

if __name__ == '__main__':
    unittest.main()

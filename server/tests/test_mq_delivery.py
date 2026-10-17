"""Real broker regressions. Use only an isolated localhost RabbitMQ instance."""
import argparse
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import unittest
import uuid

import pika

parser = argparse.ArgumentParser()
parser.add_argument("--probe", required=True)
parser.add_argument("--proto-dir", required=True)
args, unittest_args = parser.parse_known_args()
sys.path.insert(0, args.proto_dir)
import base_pb2  # noqa: E402


class BrokerDeliveryTest(unittest.TestCase):
    def setUp(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            "127.0.0.1", credentials=pika.PlainCredentials("guest", "guest"),
            socket_timeout=5, blocked_connection_timeout=5))
        self.channel = self.connection.channel()
        self.name = "chatsystem-ack-test-" + uuid.uuid4().hex
        self.dead = self.name + ".dead"
        self.channel.exchange_declare(self.dead, exchange_type="fanout")
        self.channel.queue_declare(self.dead)
        self.channel.queue_bind(self.dead, self.dead)
        self.channel.queue_declare(self.name, arguments={"x-dead-letter-exchange": self.dead})
        self.process = None
        self.output = []
        self.events = queue.Queue()

    def tearDown(self):
        self.stop_consumer()
        self.channel.queue_delete(self.name)
        self.channel.queue_delete(self.dead)
        self.channel.exchange_delete(self.dead)
        self.connection.close()

    def start_consumer(self, recover=False):
        command = [str(Path(args.probe).resolve()), self.name]
        if recover:
            command.append("recover")
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True)
        process = self.process

        def read_output():
            for line in process.stdout:
                self.output.append(line.rstrip())
                if line.startswith("EVENT "):
                    _, key, attempt, result = line.split()
                    self.events.put((key, int(attempt), result, time.monotonic()))

        self.reader = threading.Thread(target=read_output, daemon=True)
        self.reader.start()

    def stop_consumer(self):
        if self.process is None:
            return
        if self.process.poll() is None:
            # EVENT is printed before the wrapper sends the frame. Give the I/O
            # loop a chance to flush it; remain well inside the one-second retry.
            time.sleep(0.1)
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            self.fail("Consumer failed to shut down with pending delivery")
        self.reader.join(timeout=2)
        self.process.stdout.close()
        self.process = None

    def publish(self, key, mode="success", media=False):
        message = base_pb2.MessageInfo(message_id="server-" + key,
                                       client_message_id=key, chat_session_id="session")
        message.sender.user_id = "sender"
        if media:
            message.message.message_type = base_pb2.IMAGE
            message.message.image_message.image_content = b"file-retry"
        else:
            message.message.message_type = base_pb2.STRING
            message.message.string_message.content = mode
        self.channel.basic_publish("", self.name, message.SerializeToString())

    def event(self, key, attempt, result, timeout=4):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                item = self.events.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                break
            if item[:3] == (key, attempt, result):
                return item
        self.fail(f"Missing event {(key, attempt, result)}; output={self.output}")

    def assert_queue_empty(self):
        method, _, _ = self.channel.basic_get(self.name, auto_ack=True)
        self.assertIsNone(method, "A successfully persisted message remained ready for delivery")

    def test_success_and_duplicate_are_acknowledged_without_second_write(self):
        self.start_consumer()
        self.publish("duplicate")
        self.event("duplicate", 1, "ack")
        self.publish("duplicate")
        self.event("duplicate", 2, "ack")
        self.stop_consumer()
        self.assertEqual(self.output.count("STORED duplicate"), 1)
        self.assert_queue_empty()

    def check_recovery(self, mode, media=False):
        self.start_consumer()
        self.publish("recovery", mode, media)
        first = self.event("recovery", 1, "retry")
        second = self.event("recovery", 2, "ack")
        self.assertGreaterEqual(second[3] - first[3], 0.8, "Retry spun without backoff")
        self.stop_consumer()
        self.assertEqual(self.output.count("STORED recovery"), 1)
        self.assert_queue_empty()

    def test_elasticsearch_failure_is_retried(self):
        self.check_recovery("es-retry")

    def test_mysql_failure_is_retried(self):
        self.check_recovery("mysql-retry")

    def test_file_service_failure_is_retried(self):
        self.check_recovery("file-retry", media=True)

    def test_standard_exception_does_not_kill_consumer_or_ack(self):
        self.start_consumer()
        self.publish("exception", "throw-once")
        self.event("exception", 1, "exception")
        self.event("exception", 2, "ack")
        self.publish("next")
        self.event("next", 1, "ack")
        self.stop_consumer()
        self.assert_queue_empty()

    def test_nonstandard_exception_does_not_kill_consumer_or_ack(self):
        self.start_consumer()
        self.publish("exception", "throw-nonstd-once")
        self.event("exception", 1, "exception")
        self.event("exception", 2, "ack")
        self.stop_consumer()
        self.assert_queue_empty()

    def test_poison_message_is_dead_lettered_and_next_message_is_processed(self):
        self.start_consumer()
        self.channel.basic_publish("", self.name, b"\xff")
        self.event("invalid", 1, "reject")
        self.publish("after-poison")
        self.event("after-poison", 1, "ack")
        self.stop_consumer()
        method, properties, body = self.channel.basic_get(self.dead, auto_ack=True)
        self.assertIsNotNone(method, "Poison message was ACKed instead of rejected")
        self.assertEqual(body, b"\xff")
        self.assertEqual(properties.headers["x-death"][0]["reason"], "rejected")
        self.assert_queue_empty()

    def test_shutdown_during_retry_preserves_delivery_for_next_consumer(self):
        self.start_consumer()
        self.publish("restart", "always-retry")
        self.event("restart", 1, "retry")
        self.stop_consumer()
        self.start_consumer(recover=True)
        self.event("restart", 1, "ack")
        self.stop_consumer()
        self.assert_queue_empty()

    def test_persistent_failure_applies_backpressure_without_dropping(self):
        self.start_consumer()
        self.publish("outage", "always-retry")
        first = self.event("outage", 1, "retry")
        third = self.event("outage", 3, "retry")
        self.assertGreaterEqual(third[3] - first[3], 1.8)
        self.stop_consumer()
        method, _, body = self.channel.basic_get(self.name, auto_ack=True)
        self.assertIsNotNone(method, "Retrying message was lost")
        self.assertEqual(base_pb2.MessageInfo.FromString(body).client_message_id, "outage")

    def test_outage_does_not_deliver_following_messages_past_prefetch(self):
        self.start_consumer()
        self.publish("outage", "always-retry")
        self.publish("waiting")
        self.event("outage", 2, "retry")
        self.assertNotIn("STORED waiting", self.output)
        self.stop_consumer()
        pending = set()
        while True:
            method, _, body = self.channel.basic_get(self.name, auto_ack=True)
            if method is None:
                break
            pending.add(base_pb2.MessageInfo.FromString(body).client_message_id)
        self.assertEqual(pending, {"outage", "waiting"})


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]] + unittest_args, verbosity=2)

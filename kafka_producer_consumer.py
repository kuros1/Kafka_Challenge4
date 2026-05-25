import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer, KafkaException

# Configuration
TOPIC = "endpoint-logs"
BOOTSTRAP_SERVER = "localhost:9092"
TOTAL_MESSAGES = 10

# Fake Data Pools
NODE_IDS = [
    "node-1", "node-2", "node-3", "node-4", "node-5"
]

EVENT_TYPES = [
    "INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"
]

DETAILS_POOL = [
    "CPU usage spiked to 95%",
    "Request timeout on /api/payments",
    "Database connection pool exhausted",
    "Cache miss for product_id: 4821",
    "User authentication failed: token expired",
    "Disk I/O latency: 320ms",
    "Memory usage crossed 87% threshold",
    "Health check passed on port 8080",
    "Retry attempt 3 of 5 for order service",
    "Successful deployment: version 2.4.1",
]

# Delivery Callback
# Called for every message — confirms delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"  ✗ Delivery failed: {err}")
    else:
        print(f"  ✓ Delivered to partition [{msg.partition()}] offset {msg.offset()}")


# Producer
def produce_messages():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVER
    })

    print("\n" + "=" * 55)
    print("  PRODUCER — Sending 10 log events to endpoint-logs")
    print("=" * 55)

    for i in range(TOTAL_MESSAGES):
        # Build the log event
        event = {
            "node_id":    random.choice(NODE_IDS),
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "event_type": random.choice(EVENT_TYPES),
            "details":    DETAILS_POOL[i],
        }

        # Serialize to JSON bytes
        event_bytes = json.dumps(event).encode("utf-8")

        print(f"\n[Event {i + 1}]")
        print(f"  node_id    : {event['node_id']}")
        print(f"  timestamp  : {event['timestamp']}")
        print(f"  event_type : {event['event_type']}")
        print(f"  details    : {event['details']}")

        # Send to Kafka
        producer.produce(
            TOPIC,
            value=event_bytes,
            callback=delivery_report
        )

        # Process delivery callbacks
        producer.poll(0)
        time.sleep(0.3)

    # Wait until all messages are confirmed delivered
    print("\nFlushing producer — waiting for confirmations...")
    producer.flush()
    print("\n✓ All 10 messages produced successfully.\n")


# Consumer
def consume_messages():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVER,
        "group.id":          "log-consumer-group",
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([TOPIC])

    print("=" * 55)
    print("  CONSUMER — Reading messages from endpoint-logs")
    print("=" * 55)

    messages_read = 0
    empty_polls   = 0
    max_empty     = 5   # stop after 5 consecutive empty polls

    try:
        while messages_read < TOTAL_MESSAGES:
            msg = consumer.poll(timeout=3.0)

            if msg is None:
                empty_polls += 1
                print(f"  Waiting for messages... ({empty_polls}/{max_empty})")
                if empty_polls >= max_empty:
                    print("  No more messages found. Stopping.")
                    break
                continue

            if msg.error():
                raise KafkaException(msg.error())

            # Reset empty poll counter on success
            empty_polls = 0

            # Deserialize JSON bytes back to dict
            event = json.loads(msg.value().decode("utf-8"))
            messages_read += 1

            print(f"\n[Message {messages_read}]")
            print(f"  node_id    : {event['node_id']}")
            print(f"  timestamp  : {event['timestamp']}")
            print(f"  event_type : {event['event_type']}")
            print(f"  details    : {event['details']}")

    finally:
        consumer.close()

    print(f"\n✓ Consumer closed. Total messages read: {messages_read}\n")


# Main
if __name__ == "__main__":
    print("\n╔══════════════════════════════════════════════════════╗")
    print("║         Kafka Challenge 4 — KRaft Mode               ║")
    print("╚══════════════════════════════════════════════════════╝")

    # Step 1: Produce
    produce_messages()

    # Brief pause before consuming
    print("Waiting 2 seconds before consuming...\n")
    time.sleep(2)

    # Step 2: Consume
    consume_messages()

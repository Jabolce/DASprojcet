from confluent_kafka import Consumer
import json
import traceback

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock-data"

consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'detailed-diagnostic-group',
    'auto.offset.reset': 'earliest'
}

# Create consumer
consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

print(f"Listening for messages on topic: {TOPIC_NAME}")
print("Press Ctrl+C to exit")


def print_message_details(message):
    try:
        decoded_message = message.value().decode('utf-8')

        try:
            parsed_message = json.loads(decoded_message)
        except json.JSONDecodeError:
            parsed_message = decoded_message

        print("\n--- Message Details ---")
        print(f"Topic: {message.topic()}")
        print(f"Partition: {message.partition()}")
        print(f"Offset: {message.offset()}")
        print(f"Timestamp: {message.timestamp()}")

        print("\n--- Message Content ---")
        print("Raw Message:")
        print(decoded_message)

        print("\nParsed Message:")
        if isinstance(parsed_message, dict):
            for key, value in parsed_message.items():
                print(f"{key}: {value} (Type: {type(value).__name__})")
        else:
            print(parsed_message)

        print("\n--- Message Metadata ---")
        print(f"Message Headers: {list(message.headers() or [])}")

    except Exception as e:
        print(f"Error processing message: {e}")
        traceback.print_exc()


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print_message_details(msg)

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions",
)


def create_base_transaction(event_id: str):

    transaction = {
        "event_id": event_id,
        "source_row_id": 999999,
        "Time": 999999.0,
        "Amount": 100.0,
        "Class": 0,
        "produced_at": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    for i in range(1, 29):
        transaction[f"V{i}"] = 0.0

    return transaction


def main():

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    test_records = []

    # ----------------------------------------------
    # 1. Valid control record
    # ----------------------------------------------

    valid_record = create_base_transaction(
        f"quarantine-test-valid-{uuid.uuid4()}"
    )

    test_records.append(valid_record)

    # ----------------------------------------------
    # 2. Negative amount
    # ----------------------------------------------

    negative_amount = create_base_transaction(
        f"quarantine-test-negative-amount-{uuid.uuid4()}"
    )

    negative_amount["Amount"] = -500.0

    test_records.append(negative_amount)

    # ----------------------------------------------
    # 3. Invalid class
    # ----------------------------------------------

    invalid_class = create_base_transaction(
        f"quarantine-test-invalid-class-{uuid.uuid4()}"
    )

    invalid_class["Class"] = 2

    test_records.append(invalid_class)

    # ----------------------------------------------
    # 4. Null feature
    # ----------------------------------------------

    null_feature = create_base_transaction(
        f"quarantine-test-null-v1-{uuid.uuid4()}"
    )

    null_feature["V1"] = None

    test_records.append(null_feature)

    # ----------------------------------------------
    # 5. Multiple validation failures
    # ----------------------------------------------

    multiple_errors = create_base_transaction(
        f"quarantine-test-multiple-{uuid.uuid4()}"
    )

    multiple_errors["Amount"] = -100.0
    multiple_errors["Class"] = 99
    multiple_errors["V5"] = None

    test_records.append(multiple_errors)

    # ----------------------------------------------
    # Produce records
    # ----------------------------------------------

    print("=" * 70)
    print("QUARANTINE TEST PRODUCER")
    print("=" * 70)

    for record in test_records:

        producer.produce(
            KAFKA_TOPIC,
            value=json.dumps(record).encode("utf-8"),
        )

        print(
            f"Produced: "
            f"{record['event_id']}"
        )

    producer.flush()

    print("\nProduced 5 quarantine test records.")


if __name__ == "__main__":
    main()
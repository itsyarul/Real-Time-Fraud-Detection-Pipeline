import json
import os
import time

import pandas as pd

from confluent_kafka import Producer


DATA_PATH = os.getenv(
    "DATA_PATH",
    "/app/data/creditcard.csv",
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "PRODUCER_KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "PRODUCER_KAFKA_TOPIC",
    "transactions",
)

PRODUCER_MODE = os.getenv(
    "PRODUCER_MODE",
    "bounded",
).lower()

BATCH_SIZE = int(
    os.getenv(
        "PRODUCER_BATCH_SIZE",
        "1000",
    )
)

PRODUCER_OFFSET = int(
    os.getenv(
        "PRODUCER_OFFSET",
        "0",
    )
)

PRODUCER_LOOP = (
    os.getenv(
        "PRODUCER_LOOP",
        "true",
    ).lower()
    == "true"
)

DELAY_MS = int(
    os.getenv(
        "PRODUCER_DELAY_MS",
        "100",
    )
)

SHUFFLE = (
    os.getenv(
        "PRODUCER_SHUFFLE",
        "true",
    ).lower()
    == "true"
)

SEED = int(
    os.getenv(
        "PRODUCER_SEED",
        "42",
    )
)


def delivery_report(err, msg):
    if err is not None:
        print(
            f"Message delivery failed: {err}"
        )


def load_transactions():

    print(
        f"Loading dataset: {DATA_PATH}"
    )

    df = pd.read_csv(DATA_PATH)

    df = df.reset_index(
        names="source_row_id",
    )

    if SHUFFLE:

        print(
            f"Shuffling dataset with seed={SEED}"
        )

        df = (
            df
            .sample(
                frac=1,
                random_state=SEED,
            )
            .reset_index(
                drop=True,
            )
        )

    print(
        f"Total dataset records: {len(df)}"
    )

    return df


def create_producer():

    return Producer(
        {
            "bootstrap.servers":
                KAFKA_BOOTSTRAP_SERVERS,

            "client.id":
                "creditcard-replay-producer",

            "acks":
                "all",

            "enable.idempotence":
                True,
        }
    )


def create_event(row, cycle=1):

    source_row_id = int(
        row["source_row_id"]
    )

    event = {
        "event_id":
            f"creditcard-cycle-{cycle}-row-{source_row_id}",

        "source_row_id":
            source_row_id,
        
        "cycle_id":
            cycle,
    }

    for column, value in row.items():

        if column == "source_row_id":
            continue

        if pd.isna(value):

            event[column] = None

        elif column == "Class":

            event[column] = int(value)

        else:

            event[column] = float(value)

    return event


STATE_FILE = "/app/data/producer_state.json"


def send_records(
    producer,
    df,
    start,
    end,
    cycle=1,
):

    total = end - start

    print(
        f"Sending records "
        f"{start} to {end - 1} for cycle {cycle}"
    )

    for position in range(
        start,
        end,
    ):

        row = df.iloc[position]

        event = create_event(
            row,
            cycle=cycle,
        )

        producer.produce(
            topic=KAFKA_TOPIC,
            key=str(
                event["source_row_id"]
            ),
            value=json.dumps(
                event
            ),
            on_delivery=delivery_report,
        )

        producer.poll(0)

        progress = (
            position - start + 1
        )

        if (
            progress % 100 == 0
            or progress == total
        ):

            print(
                f"Produced "
                f"{progress}/{total}"
            )
            
            try:
                with open(STATE_FILE, "w") as f:
                    json.dump({"offset": position + 1, "cycle": cycle}, f)
            except Exception as e:
                print(f"Error saving state: {e}")

        time.sleep(
            DELAY_MS / 1000
        )

    producer.flush()


def run_bounded(
    producer,
    df,
):

    total_records = len(df)

    start = PRODUCER_OFFSET
    
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                start = max(start, state.get("offset", 0))
        except Exception:
            pass

    end = min(
        start + BATCH_SIZE,
        total_records,
    )

    if start >= total_records:
        print("Dataset completely exhausted in bounded mode.")
        return

    print(
        "\n=== BOUNDED MODE ==="
    )

    print(
        f"Offset: {start}"
    )

    print(
        f"Batch size: {end - start}"
    )

    send_records(
        producer,
        df,
        start,
        end,
    )

    print(
        "Bounded replay completed."
    )


def run_continuous(
    producer,
    df,
):

    total_records = len(df)

    cycle = 1
    start_offset = 0
    
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
                start_offset = state.get("offset", 0)
                cycle = state.get("cycle", 1)
        except Exception:
            pass

    while True:

        print(
            f"\n=== CONTINUOUS MODE "
            f"| CYCLE {cycle} ==="
        )

        send_records(
            producer,
            df,
            start_offset,
            total_records,
            cycle=cycle,
        )

        start_offset = 0

        print(
            f"Cycle {cycle} completed."
        )

        if not PRODUCER_LOOP:

            print(
                "Continuous replay finished."
            )

            break

        cycle += 1


def main():

    if PRODUCER_MODE not in (
        "bounded",
        "continuous",
    ):

        raise ValueError(
            "PRODUCER_MODE must be either "
            "'bounded' or 'continuous'."
        )

    df = load_transactions()

    producer = create_producer()

    print(
        f"\nKafka topic: {KAFKA_TOPIC}"
    )

    print(
        f"Producer mode: "
        f"{PRODUCER_MODE}"
    )

    if PRODUCER_MODE == "bounded":

        run_bounded(
            producer,
            df,
        )

    elif PRODUCER_MODE == "continuous":

        run_continuous(
            producer,
            df,
        )


if __name__ == "__main__":
    main()
import pandas as pd
import json
import time
import paho.mqtt.client as mqtt


# =========================
# MQTT Configuration
# =========================

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC = "comp5339/A2group26/electricity/facility_stream"


# =========================
# Data Loading and Preparation
# =========================

def load_and_prepare_stream_data(csv_path="integrated_power_emissions_data.csv"):
    """
    Load the cached integrated dataset and prepare it for MQTT streaming.
    Records are sorted by event timestamp, facility code, and unit code.
    """
    df = pd.read_csv(csv_path)

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.dropna(
        subset=[
            "facility_name",
            "facility_code",
            "unit_code",
            "timestamp",
            "power_mw",
            "emissions_t",
            "lat",
            "lon"
        ]
    )

    df = df.sort_values(
        by=["timestamp", "facility_code", "unit_code"]
    ).reset_index(drop=True)

    return df


# =========================
# MQTT Message Format
# =========================

def create_mqtt_message(row):
    """
    Convert one row of the integrated dataset into a JSON-compatible MQTT message.
    """
    message = {
        "facility_name": row["facility_name"],
        "facility_code": row["facility_code"],
        "unit_code": row["unit_code"],
        "timestamp": row["timestamp"].isoformat(),
        "power_mw": float(row["power_mw"]),
        "emissions_t": float(row["emissions_t"]),
        "lat": float(row["lat"]),
        "lon": float(row["lon"])
    }

    return message


# =========================
# MQTT Connection
# =========================

def connect_mqtt():
    """
    Create and connect an MQTT client.
    """
    print("Creating MQTT client...")

    client = mqtt.Client(client_id="comp5339_mqtt_publisher")

    print("Connecting to MQTT broker...")
    client.connect(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=60
    )

    print("Connected to MQTT broker.")

    return client


# =========================
# MQTT Publishing
# =========================

def publish_single_message(client, row):
    """
    Publish one row as a JSON MQTT message.
    """
    message = create_mqtt_message(row)
    payload = json.dumps(message)

    result = client.publish(
        MQTT_TOPIC,
        payload=payload,
        qos=0
    )

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"Failed to publish message. MQTT result code: {result.rc}")

    return result


def publish_dataframe(client, df, delay_seconds=0.1, max_messages=None):
    """
    Publish the integrated dataset row by row in event-time order.

    Parameters:
    client: MQTT client
    df: sorted DataFrame
    delay_seconds: delay between each MQTT message
    max_messages: optional limit for testing/demo
    """
    publish_count = 0

    if max_messages is not None:
        df_to_publish = df.head(max_messages)
    else:
        df_to_publish = df

    for _, row in df_to_publish.iterrows():
        publish_single_message(client, row)
        publish_count += 1

        print(
            f"Published {publish_count}: "
            f"{row['timestamp']} | "
            f"{row['facility_name']} | "
            f"Power={row['power_mw']} MW | "
            f"Emissions={row['emissions_t']} t"
        )

        time.sleep(delay_seconds)

    print(f"Finished publishing {publish_count} messages.")


# =========================
# One-Cycle Demo
# =========================

def run_one_streaming_cycle(
    csv_path="integrated_power_emissions_data.csv",
    delay_between_messages=0.1,
    max_messages_per_cycle=30
):
    """
    Run one MQTT publishing cycle for testing and demonstration.
    """
    client = connect_mqtt()
    client.loop_start()

    try:
        print("=" * 80)
        print("Starting one streaming cycle")
        print("Loading integrated dataset...")

        df = load_and_prepare_stream_data(csv_path)

        print(f"Prepared {len(df)} records for MQTT publishing.")
        print("Publishing messages...")

        publish_dataframe(
            client=client,
            df=df,
            delay_seconds=delay_between_messages,
            max_messages=max_messages_per_cycle
        )

        print("One streaming cycle completed.")

    finally:
        client.loop_stop()
        client.disconnect()
        print("MQTT client disconnected.")


# =========================
# Continuous Execution
# =========================

def run_continuous_streaming(
    csv_path="integrated_power_emissions_data.csv",
    delay_between_messages=0.1,
    delay_between_cycles=60,
    max_messages_per_cycle=30
):
    """
    Run the MQTT publisher continuously to simulate an unbounded data stream.

    Each cycle:
    1. Loads the cached integrated dataset
    2. Sorts records by event time
    3. Publishes messages one by one through MQTT
    4. Waits 60 seconds before the next cycle
    """
    client = connect_mqtt()
    client.loop_start()

    cycle_count = 0

    try:
        while True:
            cycle_count += 1

            print("=" * 80)
            print(f"Starting streaming cycle {cycle_count}")
            print("Loading integrated dataset...")

            df = load_and_prepare_stream_data(csv_path)

            print(f"Prepared {len(df)} records for MQTT publishing.")
            print("Publishing messages...")

            publish_dataframe(
                client=client,
                df=df,
                delay_seconds=delay_between_messages,
                max_messages=max_messages_per_cycle
            )

            print(f"Cycle {cycle_count} completed.")
            print(f"Waiting {delay_between_cycles} seconds before the next cycle...")

            time.sleep(delay_between_cycles)

    except KeyboardInterrupt:
        print("Continuous streaming stopped by user.")

    finally:
        client.loop_stop()
        client.disconnect()
        print("MQTT client disconnected.")


# =========================
# Main Program
# =========================

if __name__ == "__main__":
    # Demo mode: publish only 30 messages for testing.
    run_one_streaming_cycle(
        csv_path="integrated_power_emissions_data.csv",
        delay_between_messages=0.1,
        max_messages_per_cycle=30
    )

    # To run continuous streaming, comment out the demo above and uncomment below:
    # run_continuous_streaming(
    #     csv_path="integrated_power_emissions_data.csv",
    #     delay_between_messages=0.1,
    #     delay_between_cycles=60,
    #     max_messages_per_cycle=30
    # )
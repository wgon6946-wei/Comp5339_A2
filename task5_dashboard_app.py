import json
import threading
from datetime import datetime

import duckdb
import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import folium
from streamlit_folium import st_folium


DB_PATH = "assignment2_energy_stream.duckdb"
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "comp5339/A2group26/electricity/facility_stream"

DB_LOCK = threading.Lock()


def init_db():
    con = duckdb.connect(DB_PATH)
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("""
    CREATE TABLE IF NOT EXISTS mqtt_received_measurements (
        received_at TIMESTAMP DEFAULT current_timestamp,
        event_time TIMESTAMP,
        facility_name VARCHAR,
        facility_code VARCHAR,
        unit_code VARCHAR,
        power_mw DOUBLE,
        emissions_t DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        geometry GEOMETRY
    );
    """)
    con.close()


def insert_mqtt_message(message):
    with DB_LOCK:
        con = duckdb.connect(DB_PATH)
        con.execute("LOAD spatial;")

        con.execute("""
        INSERT INTO mqtt_received_measurements (
            event_time,
            facility_name,
            facility_code,
            unit_code,
            power_mw,
            emissions_t,
            latitude,
            longitude,
            geometry
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            ST_GeomFromText(?)
        );
        """, [
            message["timestamp"],
            message["facility_name"],
            message["facility_code"],
            message["unit_code"],
            float(message["power_mw"]),
            float(message["emissions_t"]),
            float(message["lat"]),
            float(message["lon"]),
            f"POINT({float(message['lon'])} {float(message['lat'])})"
        ])

        con.close()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(MQTT_TOPIC)
        print("Subscribed to:", MQTT_TOPIC)
    else:
        print("MQTT connection failed:", rc)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        message = json.loads(payload)
        insert_mqtt_message(message)
        print("Received:", message["facility_name"], message["power_mw"])
    except Exception as e:
        print("Failed to process MQTT message:", e)


def start_mqtt_client():
    client = mqtt.Client(client_id="comp5339_dashboard_subscriber")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def load_latest_data():
    con = duckdb.connect(DB_PATH)
    con.execute("LOAD spatial;")

    df = con.execute("""
    WITH latest AS (
        SELECT
            facility_code,
            MAX(event_time) AS latest_time
        FROM mqtt_received_measurements
        GROUP BY facility_code
    )
    SELECT
        m.facility_name,
        m.facility_code,
        m.unit_code,
        m.event_time,
        m.power_mw,
        m.emissions_t,
        m.latitude,
        m.longitude
    FROM mqtt_received_measurements m
    JOIN latest l
      ON m.facility_code = l.facility_code
     AND m.event_time = l.latest_time
    ORDER BY m.facility_name;
    """).fetchdf()

    con.close()
    return df


def load_fallback_facilities():
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
    SELECT
        facility_name,
        facility_code,
        NULL AS unit_code,
        NULL AS event_time,
        NULL AS power_mw,
        NULL AS emissions_t,
        latitude,
        longitude
    FROM facilities
    LIMIT 300;
    """).fetchdf()
    con.close()
    return df


def build_map(df, display_metric):
    if df.empty:
        return folium.Map(location=[-25.0, 134.0], zoom_start=4)

    centre_lat = df["latitude"].mean()
    centre_lon = df["longitude"].mean()

    m = folium.Map(location=[centre_lat, centre_lon], zoom_start=5)

    for _, row in df.iterrows():
        if pd.isna(row["latitude"]) or pd.isna(row["longitude"]):
            continue

        if display_metric == "Power output (MW)":
            value = row["power_mw"]
            metric_text = "Power"
            unit = "MW"
        else:
            value = row["emissions_t"]
            metric_text = "Emissions"
            unit = "t"

        value_text = "Waiting for stream" if pd.isna(value) else f"{value:.3f} {unit}"

        popup_html = f"""
        <b>{row['facility_name']}</b><br>
        Facility code: {row['facility_code']}<br>
        Unit code: {row['unit_code']}<br>
        Latest time: {row['event_time']}<br>
        {metric_text}: {value_text}
        """

        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{row['facility_name']} | {value_text}"
        ).add_to(m)

    return m


def main():
    st.set_page_config(page_title="Electricity Facility Stream Dashboard", layout="wide")
    st.title("Electricity Generation and Emissions Streaming Dashboard")

    init_db()

    if "mqtt_started" not in st.session_state:
        st.session_state.mqtt_client = start_mqtt_client()
        st.session_state.mqtt_started = True

    st_autorefresh(interval=3000, key="dashboard_refresh")

    st.sidebar.header("Dashboard Controls")
    display_metric = st.sidebar.radio(
        "Display metric",
        ["Power output (MW)", "CO2 emissions (t)"]
    )

    latest_df = load_latest_data()

    if latest_df.empty:
        st.warning("No MQTT messages received yet. Showing static facility locations from the database.")
        map_df = load_fallback_facilities()
    else:
        map_df = latest_df

    st.subheader("Latest Received Facility Data")
    st.write(f"Facilities shown: {len(map_df)}")
    st.dataframe(map_df.head(30), use_container_width=True)

    st.subheader("Map-based Facility Dashboard")
    fmap = build_map(map_df, display_metric)
    st_folium(fmap, width=1200, height=650)


if __name__ == "__main__":
    main()

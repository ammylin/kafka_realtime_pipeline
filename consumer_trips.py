import json
import psycopg2
from kafka import KafkaConsumer

print("DEBUG: script started")

def run_consumer():
    """Consumes 'trips' messages from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] Connecting to Kafka...")
        consumer = KafkaConsumer(
            "trips",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="trips-consumer-group",
        )
        print("[Consumer] ✓ Kafka connected.")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5433",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ PostgreSQL connected.")

        # Create trips table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trips (
                trip_id VARCHAR(50) PRIMARY KEY,
                driver_id VARCHAR(20),
                city VARCHAR(100),
                distance_km NUMERIC(6,2),
                duration_min INTEGER,
                fare_usd NUMERIC(8,2),
                vehicle_type VARCHAR(50),
                timestamp TIMESTAMP
            );
        """)
        print("[Consumer] ✓ Table 'trips' ready.")

        print("[Consumer] Listening for messages...\n")

        message_count = 0
        for msg in consumer:
            try:
                trip = msg.value

                cur.execute("""
                    INSERT INTO trips (
                        trip_id, driver_id, city, distance_km,
                        duration_min, fare_usd, vehicle_type, timestamp
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (trip_id) DO NOTHING;
                """, (
                    trip["trip_id"],
                    trip["driver_id"],
                    trip["city"],
                    trip["distance_km"],
                    trip["duration_min"],
                    trip["fare_usd"],
                    trip["vehicle_type"],
                    trip["timestamp"]
                ))

                message_count += 1
                print(f"[Consumer] ✓ Inserted trip {trip['trip_id']} from {trip['city']} (${trip['fare_usd']})")

            except Exception as e:
                print(f"[Consumer ERROR] Could not insert message: {e}")

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        raise

if __name__ == "__main__":
    run_consumer()

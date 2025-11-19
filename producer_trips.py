import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_synthetic_trip():
    """Generates synthetic ride-sharing trip data."""
    cities = ["New York", "Chicago", "Los Angeles", "Miami", "Houston"]
    drivers = [f"D{100+i}" for i in range(10)]
    vehicles = ["Sedan", "SUV", "Hatchback", "Electric", "Hybrid"]

    city = random.choice(cities)
    driver_id = random.choice(drivers)
    vehicle_type = random.choice(vehicles)

    distance_km = round(random.uniform(1.0, 25.0), 2)
    duration_min = random.randint(5, 60)
    fare = round(distance_km * random.uniform(0.8, 2.0), 2)

    return {
        "trip_id": str(uuid.uuid4())[:8],
        "driver_id": driver_id,
        "city": city,
        "distance_km": distance_km,
        "duration_min": duration_min,
        "fare_usd": fare,
        "vehicle_type": vehicle_type,
        "timestamp": datetime.now().isoformat()
    }

def run_producer():
    """Kafka producer that sends synthetic trip events to 'trips' topic."""
    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trip = generate_synthetic_trip()
            print(f"[Producer] Sending trip #{count}: {trip}")

            future = producer.send("trips", value=trip)
            meta = future.get(timeout=10)
            print(f"[Producer] ✓ Sent to partition {meta.partition} offset {meta.offset}")

            producer.flush()
            count += 1

            time.sleep(random.uniform(0.5, 2.0))

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        raise

if __name__ == "__main__":
    run_producer()

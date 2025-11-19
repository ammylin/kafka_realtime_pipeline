import time
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Trips Dashboard", layout="wide")
st.title("🚗 Real-Time Trips Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5433/kafka_db"

@st.cache_resource
def get_engine(url):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_trips(city_filter=None, limit=200):
    query = "SELECT * FROM trips"
    params = {}

    if city_filter and city_filter != "All":
        query += " WHERE city = :city"
        params["city"] = city_filter

    query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = limit

    try:
        return pd.read_sql_query(text(query), con=engine.connect(), params=params)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()

# Sidebar filters
city_options = ["All", "New York", "Chicago", "Los Angeles", "Miami", "Houston"]
selected_city = st.sidebar.selectbox("City Filter", city_options)
update_interval = st.sidebar.slider("Refresh interval (s)", 2, 20, 5)
limit_records = st.sidebar.number_input("Records to load", 50, 2000, 200)

placeholder = st.empty()

while True:
    df = load_trips(selected_city, int(limit_records))

    with placeholder.container():
        if df.empty:
            st.warning("No trips available yet...")
            time.sleep(update_interval)
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        total_trips = len(df)
        avg_fare = df["fare_usd"].mean()
        avg_distance = df["distance_km"].mean()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Trips", total_trips)
        col2.metric("Average Fare", f"${avg_fare:,.2f}")
        col3.metric("Avg Distance (km)", f"{avg_distance:.2f}")

        st.markdown("### Recent Trips")
        st.dataframe(df.head(10), use_container_width=True)

        # Charts
        city_chart = px.bar(
            df.groupby("city")["fare_usd"].sum().reset_index(),
            x="city",
            y="fare_usd",
            title="Revenue by City"
        )

        vehicle_chart = px.pie(
            df.groupby("vehicle_type")["trip_id"].count().reset_index(),
            names="vehicle_type",
            values="trip_id",
            title="Trips by Vehicle Type"
        )

        c1, c2 = st.columns(2)
        c1.plotly_chart(city_chart, use_container_width=True)
        c2.plotly_chart(vehicle_chart, use_container_width=True)

        st.caption(f"Updated at {datetime.now()} • Auto-refresh every {update_interval}s")

    time.sleep(update_interval)

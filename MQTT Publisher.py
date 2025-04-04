# publisher.py
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import json
import random
import time
import joblib

# Load Scaler
scaler = joblib.load("C:/Users/user3/Desktop/The Main Notes/Programming/S_scaler.pkl")

# MQTT Broker Details
BROKER = "mqtt-dashboard.com"  # Public MQTT Broker
PORT = 1883
TOPIC = "hvac/sensor_data"

# Define Feature Names and Ranges
features = ["COMP.B", "FLW", "EVAT", "MAIN", "OT"]
feature_ranges = {
    "COMP.B": (0, 4855), "FLW": (33, 3448), "EVAT": (10, 26), "MAIN": (0, 14914),
    "OT": (12, 36)
}

# Define Optimized Fault Conditions
fault_conditions = {
    "Inefficiency Fault": {"COMP.B": (3606, 4855), "FLW": (2803, 3448), "MAIN": (7101, 14914)},
    "Low Airflow / Blockage": {"FLW": (33, 291)},
    "Evaporator Coil Freezing": {"EVAT": (10, 11)},
    "Overheating / Inefficiency": {"MAIN": (7101, 14914), "OT": (29, 36)},
}

# Function to Generate Sensor Data with Fault Injection
def generate_sensor_data():
    fault_label, fault_values = random.choice(list(fault_conditions.items()))
    raw_data = {feature: np.random.randint(*feature_ranges[feature]) for feature in features}

    # Apply Fault Condition
    for feature, (min_val, max_val) in fault_values.items():
        raw_data[feature] = np.random.randint(min_val, max_val)

    print("\n🔍 Raw Sensor Data Before Scaling:", raw_data)  # Debugging

    # Convert to DataFrame for Scaling
    df = pd.DataFrame([raw_data])
    df_scaled = pd.DataFrame(scaler.transform(df), columns=df.columns)

    # Convert to Dict for MQTT Transmission
    scaled_data = df_scaled.iloc[0].to_dict()
    
    # Create final payload including both raw and scaled data
    final_payload = {**{f"Raw Data_{key}": value for key, value in raw_data.items()},
                     **{f"Scaled Data_{key}": value for key, value in scaled_data.items()},
                     "Fault_Label": fault_label}
    
    return final_payload


# MQTT Publisher Function
def publish_data():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(BROKER, PORT, 60)

    while True:
        sensor_data = generate_sensor_data()
        payload = json.dumps(sensor_data)
        client.publish(TOPIC, payload)
        print(f"📤 Sent: {payload}")
        time.sleep(2)  # Simulate real-time data transmission

if __name__ == "__main__":
    publish_data()

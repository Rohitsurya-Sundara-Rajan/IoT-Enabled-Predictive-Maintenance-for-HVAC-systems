# subscriber.py
import paho.mqtt.client as mqtt
import pandas as pd
import joblib
import json

# Load Pretrained Models and Scaler
rf_model = joblib.load("C:/Users/user3/Desktop/The Main Notes/Programming/FINAL MODEL/S_rf.pkl")
knn_model = joblib.load("C:/Users/user3/Desktop/The Main Notes/Programming/FINAL MODEL/S_KNN.pkl")
label_encoder = joblib.load("C:/Users/user3/Desktop/The Main Notes/Programming/FINAL MODEL/S_label.pkl") 
scaler = joblib.load("C:/Users/user3/Desktop/The Main Notes/Programming/FINAL MODEL/S_scaler.pkl")

# MQTT Broker Details
PUBLISHER_BROKER = "mqtt-dashboard.com"  # Data source broker
SUBSCRIBER_BROKER = "demo.thingsboard.io"  # ThingsBoard broker
PORT = 1883
ACCESS_TOKEN = "bfwbtrfjiwpwrr5s1DJI"
TOPIC_SUBSCRIBE = "hvac/sensor_data"
TOPIC_PUBLISH = "v1/devices/me/telemetry"

# Define Feature Set
selected_features = ["COMP.B", "FLW", "EVAT", "MAIN", "OT"]

# MQTT Client Setup for ThingsBoard
thingsboard_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
thingsboard_client.username_pw_set(ACCESS_TOKEN)
thingsboard_client.connect(SUBSCRIBER_BROKER, PORT, 60)

# Cumulative Accuracy Tracking
rf_total_predictions = 0
rf_correct_predictions = 0
knn_total_predictions = 0
knn_correct_predictions = 0

def on_message(client, userdata, msg):
    global rf_total_predictions, rf_correct_predictions, knn_total_predictions, knn_correct_predictions
    
    data = json.loads(msg.payload.decode())
    fault_label = data.pop("Fault_Label")  # Extract the actual injected fault

    # Extract Raw Values before Scaling
    raw_data = {feature: data[f"Raw Data_{feature}"] for feature in selected_features}
    scaled_data = {feature: data[f"Scaled Data_{feature}"] for feature in selected_features}

    # Convert received data to DataFrame
    df = pd.DataFrame([scaled_data])
    df = df[selected_features]  # Ensure correct feature order

    # Predict Fault Type using RF and KNN
    rf_prediction = rf_model.predict(df)
    knn_prediction = knn_model.predict(df)

    # Decode Predictions
    rf_fault_label = label_encoder.inverse_transform([rf_prediction[0]])[0]
    knn_fault_label = label_encoder.inverse_transform([knn_prediction[0]])[0]

    # Update Cumulative Accuracy
    rf_total_predictions += 1
    knn_total_predictions += 1
    if fault_label == rf_fault_label:
        rf_correct_predictions += 1
    if fault_label == knn_fault_label:
        knn_correct_predictions += 1

    rf_accuracy = (rf_correct_predictions / rf_total_predictions) * 100
    knn_accuracy = (knn_correct_predictions / knn_total_predictions) * 100

    rf_errors = 100 - rf_accuracy
    knn_errors = 100 - knn_accuracy

    # Prepare Data for ThingsBoard
    telemetry_data = {
        "RF Cumulative Accuracy (%)": rf_accuracy,
        "KNN Cumulative Accuracy (%)": knn_accuracy,
        "RF Errors (%)": rf_errors,
        "KNN Errors (%)": knn_errors,
        "Simulated Fault": fault_label,
        "RF Predicted Fault": rf_fault_label,
        "KNN Predicted Fault": knn_fault_label
    }
    for feature in selected_features:
        telemetry_data[f"Raw Data_{feature}"] = raw_data[feature]
        telemetry_data[f"Scaled Data_{feature}"] = scaled_data[feature]
    
    # Publish Data to ThingsBoard in one payload
    thingsboard_client.publish(TOPIC_PUBLISH, json.dumps(telemetry_data))
    print(f"📤 Sent to ThingsBoard: {json.dumps(telemetry_data, indent=2)}")

# MQTT Subscriber Setup
def subscribe_data():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(PUBLISHER_BROKER, PORT, 60)
    client.subscribe(TOPIC_SUBSCRIBE)
    client.loop_forever()

if __name__ == "__main__":
    subscribe_data()

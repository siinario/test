import json, ssl, os, threading
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
from flask import Flask

# ==========================================
# 1. LẤY "CHÌA KHÓA" TỪ BIẾN MÔI TRƯỜNG
# ==========================================
MONGO_URI = os.environ.get("MONGO_URI")
MQTT_BROKER = os.environ.get("MQTT_BROKER")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TOPIC = "floodguard/station1/data"

# ==========================================
# 2. KẾT NỐI MONGODB
# ==========================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["flood_monitoring"]
collection = db["sensor_data"]

# ==========================================
# 3. CÁC HÀM XỬ LÝ MQTT
# ==========================================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Đã kết nối HiveMQ! Đang lắng nghe: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Lỗi kết nối MQTT: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        data["server_timestamp"] = datetime.utcnow() # Thêm mốc thời gian
        result = collection.insert_one(data)
        print(f"💾 Đã lưu DB: {data} (ID: {result.inserted_id})")
    except Exception as e:
        print(f"❌ Lỗi xử lý dữ liệu: {e}")

# Thiết lập MQTT Client
mqtt_client = mqtt.Client(client_id="Python_Backend_Render")
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# ==========================================
# 4. WEB SERVER ẢO (GIỮ RENDER KHÔNG NGỦ ĐÔNG)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 Hệ thống FloodGuard Backend đang hoạt động tốt!"

def run_mqtt():
    print("⏳ Đang kết nối HiveMQ...")
    mqtt_client.connect(MQTT_BROKER, 8883, keepalive=60)
    mqtt_client.loop_forever()

if __name__ == "__main__":
    # 1. Chạy MQTT ở luồng phụ (chạy ngầm)
    mqtt_thread = threading.Thread(target=run_mqtt)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    
    # 2. Chạy Web Server ở luồng chính
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
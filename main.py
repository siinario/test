import json, ssl, os, threading, time
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
from flask import Flask, jsonify, request

# ==========================================
# 1. BIẾN MÔI TRƯỜNG & TOÀN CỤC
# ==========================================
MONGO_URI = os.environ.get("MONGO_URI")
MQTT_BROKER = os.environ.get("MQTT_BROKER")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TOPIC = "floodguard/station1/data"

last_alert_time = 0  # Đã thêm biến toàn cục

# ==========================================
# 2. KẾT NỐI MONGODB
# ==========================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["flood_monitoring"]
collection = db["sensor_data"]

# ==========================================
# 3. CÁC HÀM XỬ LÝ & MQTT
# ==========================================
def send_telegram_alert(h, risk):
    # Hàm giả lập (Bạn có thể thêm code gọi API Telegram thực tế vào đây sau)
    print(f"🚨 [TELEGRAM ALERT] Ngập lụt! Mực nước: {h}m - Mức độ rủi ro: {risk}%")

# Sửa lại hàm on_connect một chút để ép nhả log
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Đã kết nối HiveMQ! Đang lắng nghe: {MQTT_TOPIC}", flush=True)
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Lỗi kết nối MQTT. Mã từ chối từ Server: {rc}", flush=True)

# Gắn các hàm callback vào client
mqtt_client = mqtt.Client(client_id="Python_Backend_Render")
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect  # Thêm dòng này
mqtt_client.on_log = on_log                # Thêm dòng này

def on_disconnect(client, userdata, rc):
    print(f"⚠️ [CẢNH BÁO] Đã ngắt kết nối MQTT (Mã trạng thái: {rc})", flush=True)

def on_log(client, userdata, level, buf):
    print(f"📝 [MQTT SYSTEM LOG]: {buf}", flush=True)

def calculate_flood_metrics(distance, r_t, d_t):
    MAX_DEPTH = 4.25       
    SURFACE_AREA = 10.0
    
    H = MAX_DEPTH - distance
    if H < 0: H = 0
        
    V = SURFACE_AREA * H
    base_risk = (H / MAX_DEPTH) * 100
    risk_modifier = (r_t * 10) - (d_t * 5)
    
    risk_score = base_risk + risk_modifier
    
    if risk_score > 100: risk_score = 100
    if risk_score < 0: risk_score = 0
        
    return round(H, 2), round(V, 2), round(risk_score, 2)

def on_message(client, userdata, msg):
    global last_alert_time
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        
        raw_distance = data.get("distance", 0)
        r_t = data.get("R_t", data.get("R(t)", 0))
        d_t = data.get("D_t", data.get("D(t)", 0))
        
        calc_H, calc_V, calc_risk = calculate_flood_metrics(raw_distance, r_t, d_t)
        
        data["H"] = calc_H
        data["V"] = calc_V
        data["risk_score"] = calc_risk
        data["R_t"] = round(r_t, 3)
        data["D_t"] = round(d_t, 3)
        data["server_timestamp"] = datetime.utcnow()
        
        collection.insert_one(data)
        
        current_time = time.time()
        if calc_risk > 80 and (current_time - last_alert_time > 300):
            send_telegram_alert(calc_H, calc_risk)
            last_alert_time = current_time
            
        print(f"✅ Đã lưu DB | H: {calc_H}m, R(t): {r_t}, D(t): {d_t}, Risk: {calc_risk}")
            
    except Exception as e:
        print(f"❌ Lỗi xử lý dữ liệu: {e}")

mqtt_client = mqtt.Client(client_id="Python_Backend_Render")
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# ==========================================
# 4. WEB SERVER & API
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 Hệ thống FloodGuard Backend đang hoạt động tốt!"

# Đã bổ sung API truy xuất theo trạm
@app.route('/api/station/<station_name>', methods=['GET'])
def get_station_data(station_name):
    try:
        limit_records = int(request.args.get('limit', 50))
        query = {"station": station_name}
        cursor = collection.find(query, {"_id": 0}).sort("server_timestamp", -1).limit(limit_records)
        data_list = list(cursor)
        
        if not data_list:
            return jsonify({"status": "error", "message": f"Không có dữ liệu cho trạm {station_name}"}), 404
            
        return jsonify({
            "status": "success",
            "station": station_name,
            "total_records": len(data_list),
            "data": data_list
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def run_mqtt():
    print("⏳ Đang chuẩn bị kết nối HiveMQ...")
    try:
        # Làm sạch biến môi trường để phòng lỗi khoảng trắng hoặc None
        broker_url = str(MQTT_BROKER).replace("tls://", "").replace("mqtts://", "").strip()
        print(f"🔍 URL Broker đang dùng: {broker_url}")
        
        mqtt_client.connect(broker_url, 8883, keepalive=60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"🔥 LỖI CHÍ MẠNG KHI KẾT NỐI MQTT: {e}")

if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=run_mqtt)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

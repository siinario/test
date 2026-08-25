import json, ssl, os, threading, time, random
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

last_alert_time = 0

# ==========================================
# 2. KẾT NỐI MONGODB
# ==========================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["flood_monitoring"]
collection = db["sensor_data"]

# ==========================================
# 3. ĐỊNH NGHĨA CÁC HÀM XỬ LÝ (PHẢI ĐẶT TRÊN CÙNG)
# ==========================================

"""
stations_cache = {}
system_latest_state = []

def calculate_h_and_v(station_name, R_current, D_current, ts_current):
    global stations_cache
    state = stations_cache.get(
        station_name,
        {"H_prev": 0.0, "R_prev": None, "D_prev": None, "ts_prev": None},
    )
    H_prev = state["H_prev"]
    R_prev = state["R_prev"]
    D_prev = state["D_prev"]
    ts_prev = state["ts_prev"]

    # Bản ghi đầu tiên của trạm
    if ts_prev is None or R_prev is None or D_prev is None:
        H_current = 0.0
        V_current = 0.0
    else:
        delta_t = (ts_current - ts_prev).total_seconds() / 60.0  # Chuyển sang phút

        if delta_t > 0:
            r_avg = (R_prev + R_current) / 20.0  # mm/phút
            D_avg = (D_prev + D_current) / 2.0   # mm/phút
            delta_H_step = ((r_avg - D_avg) * delta_t) / 10.0  # cm
            H_current = max(0.0, H_prev + delta_H_step)
            V_current = (H_current - H_prev) / delta_t
        else:
            H_current = H_prev
            V_current = 0.0
    # Cập nhật đệm
    stations_cache[station_name] = {
        "H_prev": H_current,
        "R_prev": R_current,
        "D_prev": D_current,
        "ts_prev": ts_current,
    }
    return H_current, V_current


def calculate_and_classify_risk(H,V,R,H_tide,
    H_crit=50.0,
    H_warning=30.0,
    T_response=10.0,
    w_H=0.75,
    w_V=0.25,
    R_high=15.0,
    H_tide_high=1.50,
):
    delta_H_crit = H_crit - H_warning
    V_crit = delta_H_crit / T_response

    S_H = min(1.0, max(0.0, H / H_crit))
    S_V = min(1.0, max(0.0, V / V_crit))

    T_crit = float("inf")
    if V > 0 and H < H_crit:
        T_crit = (H_crit - H) / V

    raw_S_risk = 100.0 * (w_H * S_H + w_V * S_V)

    if T_crit <= T_response: S_risk = max(raw_S_risk, 75.0)
    elif H >= H_crit: S_risk = max(raw_S_risk, 100.0 * S_H)
    else: S_risk = raw_S_risk

    if S_risk < 20: code, label, description = 0, "Safe", "An toàn"
    elif 20 <= S_risk < 45: code, label, description = 1, "Advisory", "Cảnh báo nhẹ"
    elif 45 <= S_risk < 75: code, label, description = 2, "Warning", "Nguy hiểm"
    else: code, label, description = 3, "Emergency", "Khẩn cấp"

    if code == 0:
        return {
            "S_risk": round(S_risk, 2),
            "T_crit_min": round(T_crit, 2) if T_crit != float("inf") else "N/A",
            "code": code,
            "label": label,
            "description": description,
            "status": "An toàn",
        }
        
    heavy_rain = R >= R_high
    high_tide = H_tide >= H_tide_high

    if heavy_rain and high_tide: Status = "Ngập kết hợp Mưa lớn + Triều cường"
    elif not heavy_rain and high_tide: Status = "Ngập do Triều cường"
    elif heavy_rain and not high_tide: Status = "Ngập do Lượng mưa tăng nhanh"
    else: Status = "Ngập cục bộ do tích tụ tại bề mặt"

    return {
        "S_risk": round(S_risk, 2),
        "T_crit_min": round(T_crit, 2) if T_crit != float("inf") else "N/A",
        "code": code,
        "label": label,
        "description": description,
        "status": Status,
    }

def update_or_append_station_result(new_record):
    global system_latest_state
    updated = False
    for idx, record in enumerate(system_latest_state):
        if record["station_name"] == new_record["station_name"]:
            system_latest_state[idx] = new_record
            updated = True
            break
    if not updated:
        system_latest_state.append(new_record)



def process_station_data(station_name, R, D, H_tide, timestamp_str):
    ts_current = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    # 1. Tính H, V
    H_current, V_current = calculate_h_and_v(
        station_name, float(R), float(D), ts_current
    )
    # 2. calculate_and_classify_risk
    risk_result = calculate_and_classify_risk(
        H=H_current, V=V_current, R=float(R), H_tide=float(H_tide)
    )
    # 3. Đóng gói final_record
    final_record = {
        "station_name": station_name,
        "timestamp": timestamp_str,
        "R": float(R),
        "D": float(D),
        "H_tide": float(H_tide),
        "H": round(H_current, 2),
        "V": round(V_current, 2),
        "S_risk": risk_result["S_risk"],
        "T_crit_min": risk_result["T_crit_min"],
        "code":risk_result["code"],
        "label": risk_result["label"],
        "description": risk_result["description"],
        "status": risk_result["status"],
        "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    # 4. Update mảng trạng thái
    update_or_append_station_result(final_record)
    return final_record

"""













def send_telegram_alert(h, risk):
    print(f"🚨 [TELEGRAM ALERT] Ngập lụt! Mực nước: {h}m - Mức độ rủi ro: {risk}%", flush=True)

def on_disconnect(client, userdata, rc):
    print(f"⚠️ [CẢNH BÁO] Đã ngắt kết nối MQTT (Mã trạng thái: {rc})", flush=True)

def on_log(client, userdata, level, buf):
    print(f"📝 [MQTT SYSTEM LOG]: {buf}", flush=True)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Đã kết nối HiveMQ! Đang lắng nghe: {MQTT_TOPIC}", flush=True)
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Lỗi kết nối MQTT. Mã từ chối từ Server: {rc}", flush=True)

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
            
        print(f"✅ Đã lưu DB | H: {calc_H}m, R(t): {r_t}, D(t): {d_t}, Risk: {calc_risk}", flush=True)
            
    except Exception as e:
        print(f"❌ Lỗi xử lý dữ liệu: {e}", flush=True)

# ==========================================
# 4. GẮN HÀM VÀO MQTT CLIENT
# ==========================================
# Gắn thêm số ngẫu nhiên từ 1000 đến 9999 để không bao giờ bị trùng lặp
random_client_id = f"Python_Backend_Render_{random.randint(1000, 9999)}"

mqtt_client = mqtt.Client(client_id=random_client_id)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)

# Phải gắn sau khi các hàm đã được định nghĩa ở trên
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_log = on_log

# ==========================================
# 5. WEB SERVER & LUỒNG CHẠY MQTT
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 Hệ thống FloodGuard Backend đang hoạt động tốt!"

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
    print("⏳ Đang chuẩn bị kết nối HiveMQ...", flush=True)
    try:
        # Làm sạch chuỗi URL phòng trường hợp có khoảng trắng hoặc ký tự lạ
        broker_url = str(MQTT_BROKER).replace("tls://", "").replace("mqtts://", "").replace("https://", "").strip()
        print(f"🔍 URL Broker đang dùng: {broker_url}", flush=True)
        
        mqtt_client.connect(broker_url, 8883, keepalive=60)
        mqtt_client.loop_forever()
    except Exception as e:
        print(f"🔥 LỖI CHÍ MẠNG KHI KẾT NỐI MQTT: {e}", flush=True)

if __name__ == "__main__":
    mqtt_thread = threading.Thread(target=run_mqtt)
    mqtt_thread.daemon = True
    mqtt_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

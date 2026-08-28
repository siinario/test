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
MQTT_TOPIC_RAW = "floodguard/raw_data"
MQTT_TOPIC_PROCESSED = "floodguard/processed_data"

last_alert_time = 0

# Khởi tạo bộ đệm cho thuật toán mới
stations_cache = {}
system_latest_state = []

# ==========================================
# 2. KẾT NỐI MONGODB
# ==========================================
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["flood_monitoring"]
collection = db["sensor_data"]

# ==========================================
# 3. CÁC HÀM MQTT CƠ BẢN
# ==========================================
def send_telegram_alert(h, risk, status):
    print(f"🚨 [TELEGRAM ALERT] {status}! Mực nước: {h}m - Mức độ rủi ro: {risk}%", flush=True)

def on_disconnect(client, userdata, rc):
    print(f"⚠️ [CẢNH BÁO] Đã ngắt kết nối MQTT (Mã trạng thái: {rc})", flush=True)

def on_log(client, userdata, level, buf):
    # Bạn có thể comment dòng dưới lại nếu thấy log in ra quá dài
    # print(f"📝 [MQTT LOG]: {buf}", flush=True)
    pass

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Đã kết nối HiveMQ! Đang lắng nghe: {MQTT_TOPIC_RAW}", flush=True)
        client.subscribe(MQTT_TOPIC_RAW) # Đổi thành MQTT_TOPIC_RAW
    else:
        print(f"❌ Lỗi kết nối MQTT. Mã từ chối từ Server: {rc}", flush=True)

# ==========================================
# 4. THUẬT TOÁN XỬ LÝ DỮ LIỆU THỦY VĂN (MỚI)
# ==========================================
def calculate_h_and_v(station_name, R_current, D_current, H_tide_current, ts_current):
    global stations_cache
    
    # KHẮC PHỤC 1: Phục hồi bộ nhớ từ MongoDB (Bóc tách chuẩn từ mảng stations_data)
    if station_name not in stations_cache:
        last_record = collection.find_one(
            {"stations_data.station_name": station_name}, 
            sort=[("processed_at", -1)]
        )
        
        if last_record:
            # Lọc đúng dict của station_name trong mảng stations_data
            st_data = next(
                (s for s in last_record.get("stations_data", []) if s.get("station_name") == station_name), 
                {}
            )
            try:
                last_ts = datetime.strptime(last_record.get("timestamp"), "%Y-%m-%d %H:%M:%S")
            except:
                last_ts = None
                
            stations_cache[station_name] = {
                "H_prev": st_data.get("H", 0.0),
                "R_prev": st_data.get("R", 0.0),
                "D_prev": st_data.get("D", 0.0),
                "H_tide_prev": st_data.get("H_tide", H_tide_current), 
                "ts_prev": last_ts
            }
        else:
            # Khởi tạo mặc định nếu trạm hoàn toàn mới
            stations_cache[station_name] = {
                "H_prev": 0.0, 
                "R_prev": None, 
                "D_prev": None, 
                "H_tide_prev": None, 
                "ts_prev": None
            }
            
    # Bắt đầu tính toán
    state = stations_cache[station_name]
    H_prev = state["H_prev"]
    R_prev = state["R_prev"]
    D_prev = state["D_prev"]
    H_tide_prev = state.get("H_tide_prev")
    ts_prev = state["ts_prev"]

    if ts_prev is None or R_prev is None or D_prev is None or H_tide_prev is None:
        # Vẫn trả về H_prev ở giây đo đầu tiên, vì toán học cần thời gian delta_t để nước tích tụ
        H_current = H_prev 
        V_current = 0.0
    else:
        delta_t = (ts_current - ts_prev).total_seconds() / 60.0  # phút

        if delta_t > 0:
            # 1. Mức nước dâng do Mưa - Xả (cm)
            r_avg = (R_prev + R_current) / 20.0  
            D_avg = (D_prev + D_current) / 2.0   

            # Chỉ xả nước khi bề mặt đang có ngập (H_prev > 0)
            drainage = D_avg if H_prev > 0 else 0.0
            delta_H_rain = ((r_avg - drainage) * delta_t) / 10.0  # cm
            #delta_H_rain = ((r_avg - D_avg) * delta_t) / 10.0  
            
            # KHẮC PHỤC 2: Mức nước dâng/rút do Triều cường (Đổi mét sang cm)
            delta_H_tide = (H_tide_current - H_tide_prev) * 100.0
            
            # Tổng hợp nước
            delta_H_step = delta_H_rain + delta_H_tide
            H_current = max(0.0, H_prev + delta_H_step)
            
            V_current = (H_current - H_prev) / delta_t
        else:
            H_current = H_prev
            V_current = 0.0
            
    # Cập nhật lại bộ nhớ
    stations_cache[station_name] = {
        "H_prev": H_current,
        "R_prev": R_current,
        "D_prev": D_current,
        "H_tide_prev": H_tide_current, # Cập nhật H_tide cũ
        "ts_prev": ts_current,
    }
    return H_current, V_current

def calculate_and_classify_risk(H, V, R, H_tide, H_crit=250.0, H_warning=230.0, T_response=10.0, w_H=0.75, w_V=0.25, R_high=15.0, H_tide_high=1.5):
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
    
    # Đã truyền thêm tham số float(H_tide) vào hàm tính toán
    H_current, V_current = calculate_h_and_v(
        station_name, float(R), float(D), float(H_tide), ts_current
    )
    
    risk_result = calculate_and_classify_risk(
        H=H_current, V=V_current, R=float(R), H_tide=float(H_tide)
    )
    
    final_record = {
        "station_name": station_name,
        "timestamp": timestamp_str,
        "R": float(R),
        "D": float(D),
        "H_tide": float(H_tide),
        "H": round(H_current, 2),
        "V": round(V_current, 5),
        "S_risk": risk_result["S_risk"],
        "T_crit_min": risk_result["T_crit_min"],
        "code": risk_result["code"],
        "label": risk_result["label"],
        "description": risk_result["description"],
        "status": risk_result["status"],
        "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }
    
    update_or_append_station_result(final_record)
    return final_record


# ==========================================
# 5. KHỚP NỐI MQTT VỚI THUẬT TOÁN (CẬP NHẬT CHO 9 TRẠM)
# ==========================================
def on_message(client, userdata, msg):
    try:
        raw_payload = json.loads(msg.payload.decode('utf-8'))
        timestamp_str = raw_payload.get("timestamp")
        
        # 1. Chạy toán học cho 9 trạm
        processed_records = []
        for station_info in raw_payload.get("stations_data", []):
            station_name = station_info.get("station_name", "Unknown")
            R, D, H_tide = station_info.get("R", 0.0), station_info.get("D", 0.0), station_info.get("H_tide", 0.0)
            
            result = process_station_data(station_name, R, D, H_tide, timestamp_str)
            processed_records.append(result)
            
        if processed_records:
            # 2. ĐÓNG GÓI 9 TRẠM VÀO 1 DOCUMENT TỔNG HỢP
            master_document = {
                "timestamp": timestamp_str,
                "processed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "stations_data": processed_records
            }
            
            # 3. Bắn MQTT (dùng .copy() hoặc bóc tách trực tiếp để tránh _id)
            client.publish(MQTT_TOPIC_PROCESSED, json.dumps({
                "timestamp": timestamp_str, 
                "data": processed_records
            }))
            
            # Cập nhật RAM để phục vụ API Lấy dữ liệu mới nhất
            #update_system_state(processed_records)
            
            # 4. LƯU VÀO DB (Dùng insert_one thay vì insert_many)
            collection.insert_one(master_document)
            
        print(f"✅ Đã lưu 1 file JSON tổng (chứa 9 trạm) | Lúc {timestamp_str}", flush=True)
            
    except Exception as e:
        print(f"❌ Lỗi xử lý dữ liệu: {e}", flush=True)

# Gắn hàm vào Client
random_client_id = f"Python_Backend_Render_{random.randint(1000, 9999)}"
mqtt_client = mqtt.Client(client_id=random_client_id)
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_log = on_log

# ==========================================
# 6. WEB SERVER & API 
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "🚀 FloodGuard Data Pipeline đang hoạt động!"

@app.route('/api/stations/latest', methods=['GET'])
def get_all_stations_latest():
    # Lấy từ RAM, không bị ảnh hưởng bởi _id
    return jsonify({"status": "success", "total_stations": len(system_latest_state), "data": system_latest_state}), 200

@app.route('/api/station/<station_name>', methods=['GET'])
def get_station_data(station_name):
    limit_records = int(request.args.get('limit', 50))
    
    # Tìm các document tổng hợp có chứa tên trạm trong mảng stations_data
    cursor = collection.find(
        {"stations_data.station_name": station_name}, 
        {"_id": 0}
    ).sort("timestamp", -1).limit(limit_records)
    
    data_list = []
    
    # Lọc ra chính xác trạm mà Frontend yêu cầu từ mảng tổng hợp
    for doc in cursor:
        for station in doc.get("stations_data", []):
            if station.get("station_name") == station_name:
                data_list.append(station)
                break  # Tìm thấy trạm trong phút này thì chuyển sang phút tiếp theo
                
    return jsonify({"status": "success", "data": data_list}), 200

def run_mqtt():
    broker_url = str(MQTT_BROKER).replace("tls://", "").replace("mqtts://", "").replace("https://", "").strip()
    mqtt_client.connect(broker_url, 8883, keepalive=60)
    mqtt_client.loop_forever()

if __name__ == "__main__":
    threading.Thread(target=run_mqtt, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

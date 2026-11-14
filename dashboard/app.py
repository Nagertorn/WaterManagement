import os
import sqlite3
import threading
from flask import Flask, render_template_string
from confluent_kafka import Consumer, KafkaError

app = Flask(__name__)

DB_PATH = os.getenv("DB_PATH")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

telemetry_live = {}
station_operators = {}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>WM Central - Panel de Monitorización</title>
    <!-- Recarga la página cada 2 segundos para ver cambios en tiempo real -->
    <meta http-equiv="refresh" content="1">
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #121212; color: #e0e0e0; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #333; padding-bottom: 10px; margin-bottom: 20px; }
        h1 { color: #4CAF50; margin: 0; }
        table { width: 100%; border-collapse: collapse; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        th, td { padding: 14px; text-align: left; border-bottom: 1px solid #2a2a2a; }
        th { background-color: #1e1e1e; color: #aaa; text-transform: uppercase; font-size: 0.85em; letter-spacing: 1px; }
        
        .status-badge { padding: 6px 12px; border-radius: 4px; font-weight: bold; display: inline-block; }
        .DISPONIBLE { background-color: #2e7d32; color: #ffffff; }
        .REGANDO { background-color: #1b5e20; color: #ffffff; animation: blink 1s infinite; }
        .FUGA { background-color: #c62828; color: #ffffff; }
        .FUERA_DE_SERVICIO { background-color: #ef6c00; color: #ffffff; }
        .DESCONECTADA { background-color: #424242; color: #bdbdbd; }
        
        @keyframes blink { 50% { opacity: 0.4; } }
    </style>
</head>
<body>
    <div class="header">
        <h1>WaterManagement — Central Panel</h1>
        <span>Estado del Sistema: <strong>ONLINE</strong></span>
    </div>

    <table>
        <thead>
            <tr>
                <th>ID Estación</th>
                <th>Ubicación</th>
                <th>Estado Actual</th>
                <th>Caudal (L/min)</th>
                <th>Vol. Acumulado (L)</th>
                <th>Operador</th>
            </tr>
        </thead>
        <tbody>
            {% for station in stations %}
            <tr>
                <td><strong>{{ station['id'] }}</strong></td>
                <td>{{ station['location'] }}</td>
                <td>
                    <span class="status-badge {{ station['status'] }}">
                        {% if station['status'] == 'FUERA_DE_SERVICIO' %}
                            FUERA DE SERVICIO
                        {% else %}
                            {{ station['status'] }}
                        {% endif %}
                    </span>
                </td>
                <td><strong>{{ station['flow'] }}</strong></td>
                <td><strong>{{ station['vol'] }}</strong></td>
                <td><strong>{{ station['operator'] }}</strong></td>
            </tr>
            {% else %}
            <tr>
                <td colspan="5" style="text-align: center; color: #888;">No hay estaciones registradas en el sistema.</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

def kafka_listener():
    """Hilo secundario para consumir telemetría de central_events"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'flask_dashboard_group',
        'auto.offset.reset': 'latest'
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe(['central_events', 'central_requests'])
        print("[FLASK KAFKA] Suscrito exitosamente a 'central_events' y 'central_requests'")

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[FLASK KAFKA ERROR] {msg.error()}")
                continue

            topic = msg.topic()
            payload = msg.value().decode('utf-8').strip()
            parts = payload.split('#')
            
            # Registrar qué operario solicita el riego en cada estación
            if topic == 'central_requests':
                # Formato: OPERATOR_WATER_REQUEST#<ws_id>#<op_id>#<duration>
                if len(parts) >= 3 and parts[0] == "OPERATOR_WATER_REQUEST":
                    ws_id = parts[1]
                    op_id = parts[2]
                    station_operators[ws_id] = op_id

            elif topic == 'central_events':
                # Formato: TELEMETRY#<ws_id>#<caudal>#<acumulado>
                if len(parts) == 4 and parts[0] == "TELEMETRY":
                    ws_id = parts[1]
                    flow = parts[2]
                    vol = parts[3]
                    op_id = station_operators.get(ws_id, '-')
                    telemetry_live[ws_id] = {'flow': flow, 'vol': vol, 'operator': op_id}
                    
                # Formato: END_WATERING#<ws_id>#<vol>#<duracion>
                elif len(parts) >= 2 and parts[0] == "END_WATERING":
                    ws_id = parts[1]
                    vol = parts[2]
                    telemetry_live[ws_id] = {'flow': 0, 'vol': vol, 'operator': '-'}
                    station_operators.pop(ws_id, None)
                
    except Exception as e:
        print(f"[FLASK KAFKA THREAD ERROR] {e}")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        db_stations = conn.execute('SELECT id, location, status FROM watering_station').fetchall()
        conn.close()

        stations = []

        for st in db_stations:
            ws_id = st['id']
            
            # Obtener directamente los datos consumidos de Kafka
            live_data = telemetry_live.get(ws_id, {'flow': 0, 'vol': 0, 'operator': '-'})
            
            stations.append({
                'id': ws_id,
                'location': st['location'],
                'status': st['status'],
                'flow': live_data['flow'],
                'vol': live_data['vol'],
                'operator': live_data.get('operator', '-')
            })
    except Exception as e:
        print(f"[FLASK ERROR] {e}")
        stations = []

    return render_template_string(HTML_TEMPLATE, stations=stations)

if __name__ == '__main__':
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()

    app.run(host='0.0.0.0', port=5000)

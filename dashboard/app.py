import os
import sqlite3
from flask import Flask, render_template_string

app = Flask(__name__)

# Ruta a la base de datos SQLite (se ajusta según entorno local o Docker/Railway)
DB_PATH = os.getenv("DB_PATH", "../central/data/water_managment.db")

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>WM Central - Panel de Monitorización</title>
    <!-- Recarga la página cada 2 segundos para ver cambios en tiempo real -->
    <meta http-equiv="refresh" content="2">
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #121212; color: #e0e0e0; margin: 0; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid #333; padding-bottom: 10px; margin-bottom: 20px; }
        h1 { color: #4CAF50; margin: 0; }
        table { width: 100%; border-collapse: collapse; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
        th, td { padding: 14px; text-align: left; border-bottom: 1px solid #2a2a2a; }
        th { background-color: #1e1e1e; color: #aaa; text-transform: uppercase; font-size: 0.85em; letter-spacing: 1px; }
        
        /* Reglas CSS para los estados requeridos por la práctica */
        .status-badge { padding: 6px 12px; border-radius: 4px; font-weight: bold; display: inline-block; }
        
        .DISPONIBLE { background-color: #2e7d32; color: #ffffff; } /* VERDE */
        .REGANDO { background-color: #1b5e20; color: #ffffff; animation: blink 1s infinite; } /* VERDE PARPADEANDO */
        .LEAK { background-color: #c62828; color: #ffffff; } /* ROJO */
        .OUT_OF_SERVICE { background-color: #ef6c00; color: #ffffff; } /* NARANJA */
        .DESCONECTADA { background-color: #424242; color: #bdbdbd; } /* GRIS */
        
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
            </tr>
        </thead>
        <tbody>
            {% for station in stations %}
            <tr>
                <td><strong>{{ station['id'] }}</strong></td>
                <td>{{ station['location'] }}</td>
                <td>
                    <span class="status-badge {{ station['status'] }}">
                        {% if station['status'] == 'OUT_OF_SERVICE' %}
                            Fuera de Servicio
                        {% else %}
                            {{ station['status'] }}
                        {% endif %}
                    </span>
                </td>
            </tr>
            {% else %}
            <tr>
                <td colspan="3" style="text-align: center; color: #888;">No hay estaciones registradas en el sistema.</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</body>
</html>
"""

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        stations = conn.execute('SELECT id, location, status FROM watering_station').fetchall()
        conn.close()
    except Exception as e:
        stations = []
    return render_template_string(HTML_TEMPLATE, stations=stations)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

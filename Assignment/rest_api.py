#!/usr/bin/env python3
"""REST API for the Raspberry Pi IoT dashboard.

- Serves index.html and REST endpoints on port 5000.
- Subscribes to local Pi MQTT topics and exposes the latest values as JSON.
- Receives ChirpStack uplinks over secure MQTT WebSockets.
- Converts the two-byte potentiometer payload into a brightness command for
  the existing Raspberry Pi server.py process.
"""

import base64
import json
import logging
import os
import ssl
import time
import uuid
from datetime import datetime
from threading import Event
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# REST API
# ---------------------------------------------------------------------------
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

# ---------------------------------------------------------------------------
# Raspberry Pi MQTT broker (same broker used by server.py)
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "mqtt-dashboard.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_CLIENT_ID = os.getenv("MQTT_API_CLIENT_ID", f"ras67070102-rest-api-{os.getpid()}")

COMMAND_TOPIC = os.getenv("MQTT_COMMAND_TOPIC", "ras67070102/command")
TEMP_TOPIC = os.getenv("MQTT_TEMPERATURE_TOPIC", "ras67070102/temperature")
HUMIDITY_TOPIC = os.getenv("MQTT_HUMIDITY_TOPIC", "ras67070102/humidity")
DISTANCE_TOPIC = os.getenv("MQTT_DISTANCE_TOPIC", "ras67070102/distance")
LED_TOPIC = os.getenv("MQTT_LED_TOPIC", "ras67070102/led")
BRIGHTNESS_TOPIC = os.getenv("MQTT_BRIGHTNESS_TOPIC", "ras67070102/brightness")
TELEMETRY_TOPIC = os.getenv("MQTT_TELEMETRY_TOPIC", "ras67070102/telemetry")
COMMAND_RESULT_TOPIC = os.getenv("MQTT_COMMAND_RESULT_TOPIC", "ras67070102/command/result")
LORA_DATA_TOPIC = os.getenv("MQTT_LORA_DATA_TOPIC", "ras67070102/lora")

# ---------------------------------------------------------------------------
# ChirpStack MQTT over TLS WebSockets
# ---------------------------------------------------------------------------
CHIRPSTACK_HOST = os.getenv("CHIRPSTACK_MQTT_HOST", "lora.kmitl.ac.th")
CHIRPSTACK_PORT = int(os.getenv("CHIRPSTACK_MQTT_PORT", "8884"))
CHIRPSTACK_WS_PATH = os.getenv("CHIRPSTACK_WS_PATH", "/mqtt")
CHIRPSTACK_USERNAME = os.getenv("CHIRPSTACK_MQTT_USERNAME", "")
CHIRPSTACK_PASSWORD = os.getenv("CHIRPSTACK_MQTT_PASSWORD", "")
CHIRPSTACK_CLIENT_ID = os.getenv("CHIRPSTACK_CLIENT_ID", f"qcare-lora-bridge-{os.getpid()}")
CHIRPSTACK_TOPIC = os.getenv(
    "CHIRPSTACK_UPLINK_TOPIC",
    "application/47098c3e-bf24-4855-8014-25ad24d9e36f/device/93264b176ce481d9/event/up",
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("IoT-REST-API")

state_lock = Lock()
pending_lock = Lock()
pending_commands = {}
state = {
    "temperature_c": None,
    "humidity_pct": None,
    "distance_cm": None,
    "led": "off",
    "brightness_percent": 100,
    "brightness_source": "default",
    "potentiometer_raw": None,
    "lora_received_at": None,
    "temperature_timestamp": None,
    "humidity_timestamp": None,
    "distance_timestamp": None,
    "timestamp": None,
}
connection_state = {"local_mqtt_connected": False, "chirpstack_connected": False}


def _json_loads(payload):
    try:
        return json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return None


def _set_state(**values):
    updated_at = datetime.now().astimezone().isoformat(timespec="milliseconds")
    with state_lock:
        state.update(values)
        state["timestamp"] = updated_at
        if values.get("temperature_c") is not None and "temperature_timestamp" not in values:
            state["temperature_timestamp"] = updated_at
        if values.get("humidity_pct") is not None and "humidity_timestamp" not in values:
            state["humidity_timestamp"] = updated_at
        if values.get("distance_cm") is not None and "distance_timestamp" not in values:
            state["distance_timestamp"] = updated_at


def _local_on_connect(client, userdata, flags, reason_code, properties):
    connected = (reason_code == 0)
    with state_lock:
        connection_state["local_mqtt_connected"] = connected
    if connected:
        topics = [
            (TEMP_TOPIC, 1),
            (HUMIDITY_TOPIC, 1),
            (DISTANCE_TOPIC, 1),
            (LED_TOPIC, 1),
            (BRIGHTNESS_TOPIC, 1),
            (TELEMETRY_TOPIC, 1),
            (LORA_DATA_TOPIC, 1),
            (COMMAND_RESULT_TOPIC, 1),
        ]
        client.subscribe(topics)
        logger.info("Connected to Pi MQTT broker %s:%s", MQTT_BROKER, MQTT_PORT)
    else:
        logger.error("Pi MQTT connection failed: %s", reason_code)


def _local_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    with state_lock:
        connection_state["local_mqtt_connected"] = False
    logger.warning("Disconnected from Pi MQTT broker: %s", reason_code)


def _local_on_message(client, userdata, message):
    payload = message.payload.decode("utf-8", errors="replace").strip()

    if message.topic == COMMAND_RESULT_TOPIC:
        result = _json_loads(payload)
        if isinstance(result, dict) and result.get("request_id"):
            with pending_lock:
                pending = pending_commands.get(str(result["request_id"]))
                if pending is not None:
                    pending["result"] = result
                    pending["event"].set()
        return

    number = None
    try:
        number = float(payload)
    except ValueError:
        pass

    if message.topic == TEMP_TOPIC and number is not None:
        _set_state(temperature_c=number)
    elif message.topic == HUMIDITY_TOPIC and number is not None:
        _set_state(humidity_pct=number)
    elif message.topic == DISTANCE_TOPIC and number is not None:
        _set_state(distance_cm=number)
    elif message.topic == LED_TOPIC and payload.lower() in ("on", "off"):
        _set_state(led=payload.lower())
    elif message.topic == BRIGHTNESS_TOPIC and number is not None:
        _set_state(brightness_percent=max(0, min(100, int(round(number)))))
    elif message.topic == TELEMETRY_TOPIC:
        item = _json_loads(payload)
        if isinstance(item, dict):
            allowed = {
                "temperature_c": item.get("temperature_c"),
                "humidity_pct": item.get("humidity_pct"),
                "distance_cm": item.get("distance_cm"),
                "led": item.get("led"),
                "brightness_percent": item.get("brightness_percent"),
                "brightness_source": item.get("brightness_source"),
                "potentiometer_raw": item.get("potentiometer_raw"),
                "temperature_timestamp": item.get("temperature_timestamp"),
                "humidity_timestamp": item.get("humidity_timestamp"),
                "distance_timestamp": item.get("distance_timestamp"),
            }
            _set_state(**{key: value for key, value in allowed.items() if value is not None})
    elif message.topic == LORA_DATA_TOPIC:
        item = _json_loads(payload)
        if isinstance(item, dict):
            _set_state(
                potentiometer_raw=item.get("potentiometer_raw"),
                brightness_percent=item.get("brightness_percent"),
                brightness_source="lora",
                lora_received_at=item.get("received_at"),
            )


def _publish_local(topic, payload, retain=False):
    if not local_mqtt.is_connected():
        logger.warning("Pi MQTT is not connected; cannot publish to %s", topic)
        return False
    info = local_mqtt.publish(topic, payload, qos=1, retain=retain)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        logger.error("MQTT publish failed for %s (rc=%s)", topic, info.rc)
        return False
    return True


def _send_led_command_and_wait(state_value, timeout=4.0):
    """Publish LED control and wait until server.py confirms GPIO/PWM handling."""
    request_id = uuid.uuid4().hex[:12]
    event = Event()
    with pending_lock:
        pending_commands[request_id] = {"event": event, "result": None}

    command = json.dumps({
        "action": "led",
        "state": state_value,
        "request_id": request_id,
    })
    logger.info("Forwarding LED %s command to %s (request_id=%s)", state_value.upper(), COMMAND_TOPIC, request_id)
    if not _publish_local(COMMAND_TOPIC, command):
        with pending_lock:
            pending_commands.pop(request_id, None)
        return None, "Raspberry Pi MQTT broker is not connected"

    if not event.wait(timeout):
        with pending_lock:
            pending_commands.pop(request_id, None)
        return None, "No acknowledgement from server.py. Check that server.py is running and subscribed to the command topic."

    with pending_lock:
        pending = pending_commands.pop(request_id, None)
    result = pending.get("result") if pending else None
    if not isinstance(result, dict):
        return None, "Invalid command acknowledgement from server.py"
    return result, None


def _chirpstack_on_connect(client, userdata, flags, reason_code, properties):
    connected = (reason_code == 0)
    with state_lock:
        connection_state["chirpstack_connected"] = connected
    if connected:
        client.subscribe(CHIRPSTACK_TOPIC, qos=1)
        logger.info("Connected to ChirpStack; subscribed to %s", CHIRPSTACK_TOPIC)
    else:
        logger.error("ChirpStack MQTT connection failed: %s", reason_code)


def _chirpstack_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    with state_lock:
        connection_state["chirpstack_connected"] = False
    logger.warning("Disconnected from ChirpStack MQTT: %s", reason_code)


def _chirpstack_on_message(client, userdata, message):
    """Decode exactly two bytes: [ADC high byte, ADC low byte]."""
    try:
        uplink = json.loads(message.payload.decode("utf-8"))
        encoded = uplink.get("data")
        if not isinstance(encoded, str):
            raise ValueError("Uplink JSON has no Base64 'data' field")
        payload = base64.b64decode(encoded, validate=True)
        if len(payload) != 2:
            raise ValueError(f"Expected 2 potentiometer bytes, received {len(payload)}")

        raw_value = (payload[0] << 8) | payload[1]
        if not 0 <= raw_value <= 1023:
            raise ValueError(f"Potentiometer ADC value out of range: {raw_value}")
        brightness = int(round(raw_value * 100 / 1023))
        received_at = uplink.get("time") or time.strftime("%Y-%m-%dT%H:%M:%S%z")

        # server.py owns GPIO/PWM and remains the single source of hardware control.
        command = json.dumps({"action": "brightness", "raw": raw_value})
        if not _publish_local(COMMAND_TOPIC, command):
            logger.warning("Could not forward LoRa brightness command to Raspberry Pi")
            return

        lora_state = {
            "topic": message.topic,
            "received_at": received_at,
            "potentiometer_raw": raw_value,
            "brightness_percent": brightness,
            "payload_base64": encoded,
            "f_port": uplink.get("fPort"),
            "f_count": uplink.get("fCnt"),
        }
        _publish_local(LORA_DATA_TOPIC, json.dumps(lora_state, ensure_ascii=False), retain=True)
        _set_state(
            potentiometer_raw=raw_value,
            brightness_percent=brightness,
            lora_received_at=received_at,
        )
        logger.info("LoRa potentiometer=%s; brightness=%s%%", raw_value, brightness)
    except Exception as exc:
        logger.warning("Rejected ChirpStack uplink on %s: %s", message.topic, exc)


local_mqtt = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=MQTT_CLIENT_ID,
)
local_mqtt.on_connect = _local_on_connect
local_mqtt.on_disconnect = _local_on_disconnect
local_mqtt.on_message = _local_on_message
local_mqtt.reconnect_delay_set(min_delay=2, max_delay=30)
if MQTT_USERNAME:
    local_mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

chirpstack_mqtt = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=CHIRPSTACK_CLIENT_ID,
    transport="websockets",
)
chirpstack_mqtt.on_connect = _chirpstack_on_connect
chirpstack_mqtt.on_disconnect = _chirpstack_on_disconnect
chirpstack_mqtt.on_message = _chirpstack_on_message
chirpstack_mqtt.reconnect_delay_set(min_delay=2, max_delay=30)
chirpstack_mqtt.ws_set_options(path=CHIRPSTACK_WS_PATH)
chirpstack_mqtt.tls_set(cert_reqs=ssl.CERT_REQUIRED)
if CHIRPSTACK_USERNAME:
    chirpstack_mqtt.username_pw_set(CHIRPSTACK_USERNAME, CHIRPSTACK_PASSWORD or None)


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "IoTDashboardREST/1.0"

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.address_string(), fmt % args)

    def _send_bytes(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, body, "application/json; charset=utf-8")

    def do_OPTIONS(self):
        self._send_bytes(204, b"", "text/plain; charset=utf-8")

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                body = INDEX_FILE.read_bytes()
            except OSError:
                self._send_json(500, {"success": False, "message": "index.html not found"})
                return
            self._send_bytes(200, body, "text/html; charset=utf-8")
            return

        if self.path == "/api/health":
            with state_lock:
                connections = dict(connection_state)
            self._send_json(200, {"success": True, **connections})
            return

        if self.path == "/api/telemetry":
            with state_lock:
                result = dict(state)
                connections = dict(connection_state)
            self._send_json(200, {**result, **connections})
            return

        self._send_json(404, {"success": False, "message": "Not found"})

    def do_POST(self):
        if self.path != "/api/led":
            self._send_json(404, {"success": False, "message": "Not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 8192:
                raise ValueError("Invalid request body size")
            body = self.rfile.read(length)
            request = json.loads(body.decode("utf-8"))
            if not isinstance(request, dict):
                raise ValueError("Request body must be a JSON object")
            action = str(request.get("action", "led")).strip().lower()
            led_state = str(request.get("state", "")).strip().lower()
            if action != "led" or led_state not in ("on", "off"):
                raise ValueError("Expected JSON body: {\"action\":\"led\",\"state\":\"on|off\"}")
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self._send_json(400, {"success": False, "message": str(exc)})
            return

        result, error = _send_led_command_and_wait(led_state, timeout=8.0)
        if error:
            status = 503 if "MQTT" in error else 504
            self._send_json(status, {"success": False, "message": error})
            return
        if not result.get("success"):
            self._send_json(502, {
                "success": False,
                "message": result.get("message", "server.py rejected the LED command"),
            })
            return

        confirmed_state = result.get("state", led_state)
        _set_state(led=confirmed_state)
        self._send_json(200, {
            "success": True,
            "message": result.get("message", f"LED turned {confirmed_state}"),
            "state": confirmed_state,
            "request_id": result.get("request_id"),
        })


def start_mqtt_clients():
    try:
        local_mqtt.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        local_mqtt.loop_start()
        logger.info("Starting Pi MQTT connection to %s:%s", MQTT_BROKER, MQTT_PORT)
    except Exception:
        logger.exception("Could not start Pi MQTT client")

    try:
        chirpstack_mqtt.connect_async(CHIRPSTACK_HOST, CHIRPSTACK_PORT, keepalive=60)
        chirpstack_mqtt.loop_start()
        logger.info(
            "Starting ChirpStack secure WebSocket connection to %s:%s%s",
            CHIRPSTACK_HOST,
            CHIRPSTACK_PORT,
            CHIRPSTACK_WS_PATH,
        )
    except Exception:
        logger.exception("Could not start ChirpStack MQTT client")


def main():
    start_mqtt_clients()
    server = ThreadingHTTPServer((API_HOST, API_PORT), DashboardHandler)
    logger.info("REST API and dashboard listening on http://%s:%s", API_HOST, API_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping REST API")
    finally:
        server.server_close()
        for client in (local_mqtt, chirpstack_mqtt):
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                logger.exception("Error while stopping MQTT client")


if __name__ == "__main__":
    main()

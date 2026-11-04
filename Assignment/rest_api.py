#!/usr/bin/env python3
"""Dashboard REST API and ChirpStack-to-MQTT bridge."""

import base64
import json
import logging
import os
import ssl
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Lock

import paho.mqtt.client as mqtt

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

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
COMMAND_RESULT_TOPIC = os.getenv("MQTT_COMMAND_RESULT_TOPIC", "ras67070102/command/result")
LORA_DATA_TOPIC = os.getenv("MQTT_LORA_DATA_TOPIC", "ras67070102/lora")

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
LORA_STALE_AFTER = max(15, int(os.getenv("LORA_STALE_AFTER_SECONDS", "60")))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("IoT-REST-API")

state_lock = Lock()
pending_lock = Lock()
pending_commands = {}
state = {
    "temperature_c": None,
    "humidity_pct": None,
    "distance_cm": None,
    "led": "off",
    "led_output_percent": 0,
    "lora_received_at": None,
    "lora_f_cnt": None,
    "lora_f_port": None,
    "lora_payload_hex": None,
    "obstacle_detected": None,
    "obstacle_sensor_available": False,
    "obstacle_received_at": None,
    "obstacle_f_cnt": None,
    "obstacle_payload_hex": None,
    "temperature_timestamp": None,
    "humidity_timestamp": None,
    "distance_timestamp": None,
    "timestamp": None,
}
connections = {"local_mqtt_connected": False, "chirpstack_connected": False}
latest_lora_snapshot = None


def parse_time(value):
    if not value:
        return None
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone() if result.tzinfo is None else result
    except (TypeError, ValueError, OverflowError):
        return None


def age_seconds(value):
    parsed = parse_time(value)
    return max(0.0, (datetime.now(parsed.tzinfo) - parsed).total_seconds()) if parsed else None


def update_state(**values):
    timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
    with state_lock:
        state.update(values)
        state["timestamp"] = timestamp
        for key, time_key in (
            ("temperature_c", "temperature_timestamp"),
            ("humidity_pct", "humidity_timestamp"),
            ("distance_cm", "distance_timestamp"),
        ):
            if values.get(key) is not None and time_key not in values:
                state[time_key] = timestamp


def json_load(payload):
    try:
        return json.loads(payload)
    except (TypeError, json.JSONDecodeError):
        return None


def lora_is_newer(received_at):
    incoming = parse_time(received_at)
    with state_lock:
        current = parse_time(state.get("lora_received_at"))
    return incoming is not None and (current is None or incoming >= current)


def local_on_connect(client, userdata, flags, reason_code, properties):
    connected = reason_code == 0
    with state_lock:
        connections["local_mqtt_connected"] = connected
    if not connected:
        logger.error("Pi MQTT connection failed: %s", reason_code)
        return
    client.subscribe([
        (TEMP_TOPIC, 1), (HUMIDITY_TOPIC, 1), (DISTANCE_TOPIC, 1),
        (LED_TOPIC, 1), (LORA_DATA_TOPIC, 1),
        (COMMAND_RESULT_TOPIC, 1),
    ])
    logger.info("Connected to Pi MQTT broker %s:%s", MQTT_BROKER, MQTT_PORT)
    with state_lock:
        snapshot = dict(latest_lora_snapshot) if latest_lora_snapshot else None
    if snapshot:
        client.publish(LORA_DATA_TOPIC, json.dumps(snapshot), qos=1, retain=True)


def local_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    with state_lock:
        connections["local_mqtt_connected"] = False
    logger.warning("Disconnected from Pi MQTT: %s", reason_code)


def local_on_message(client, userdata, message):
    payload = message.payload.decode("utf-8", errors="replace").strip()
    if message.topic == COMMAND_RESULT_TOPIC:
        result = json_load(payload)
        if isinstance(result, dict) and result.get("request_id"):
            with pending_lock:
                pending = pending_commands.get(str(result["request_id"]))
                if pending:
                    pending["result"] = result
                    pending["event"].set()
        return

    if message.topic in (TEMP_TOPIC, HUMIDITY_TOPIC, DISTANCE_TOPIC):
        if message.retain:
            return
        try:
            value = float(payload)
        except ValueError:
            return
        key = {TEMP_TOPIC: "temperature_c", HUMIDITY_TOPIC: "humidity_pct", DISTANCE_TOPIC: "distance_cm"}[message.topic]
        update_state(**{key: value})
        return

    if message.topic == LED_TOPIC:
        value = payload.lower()
        if value in ("on", "off"):
            update_state(led=value, led_output_percent=100 if value == "on" else 0)
        return

    if message.topic == LORA_DATA_TOPIC:
        item = json_load(payload)
        if not isinstance(item, dict) or not lora_is_newer(item.get("received_at")):
            return
        detected = item.get("obstacle_detected")
        available = item.get("obstacle_sensor_available") is True and isinstance(detected, bool)
        update_state(
            lora_received_at=item.get("received_at"),
            lora_f_cnt=item.get("f_count"),
            lora_f_port=item.get("f_port"),
            lora_payload_hex=item.get("payload_hex"),
            obstacle_detected=detected if available else None,
            obstacle_sensor_available=available,
            obstacle_received_at=item.get("received_at") if available else None,
            obstacle_f_cnt=item.get("f_count") if available else None,
            obstacle_payload_hex=item.get("payload_hex") if available else None,
        )


def publish_local(topic, payload, retain=False):
    if not local_mqtt.is_connected():
        logger.warning("Pi MQTT disconnected; cannot publish %s", topic)
        return False
    info = local_mqtt.publish(topic, payload, qos=1, retain=retain)
    return info.rc == mqtt.MQTT_ERR_SUCCESS


def send_led_command_and_wait(value, timeout=8.0):
    request_id = uuid.uuid4().hex[:12]
    event = Event()
    with pending_lock:
        pending_commands[request_id] = {"event": event, "result": None}
    command = json.dumps({"action": "led", "state": value, "request_id": request_id})
    if not publish_local(COMMAND_TOPIC, command):
        with pending_lock:
            pending_commands.pop(request_id, None)
        return None, "Raspberry Pi MQTT broker is not connected"
    if not event.wait(timeout):
        with pending_lock:
            pending_commands.pop(request_id, None)
        return None, "No acknowledgement from server.py. Check that server.py is running."
    with pending_lock:
        pending = pending_commands.pop(request_id, None)
    result = pending.get("result") if pending else None
    return (result, None) if isinstance(result, dict) else (None, "Invalid acknowledgement from server.py")


def chirpstack_on_connect(client, userdata, flags, reason_code, properties):
    with state_lock:
        connections["chirpstack_connected"] = reason_code == 0
    if reason_code == 0:
        result, mid = client.subscribe(CHIRPSTACK_TOPIC, qos=1)
        logger.info("Connected to ChirpStack; subscribed mid=%s topic=%s", mid, CHIRPSTACK_TOPIC)
    else:
        logger.error("ChirpStack MQTT connection failed: %s", reason_code)


def chirpstack_on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    with state_lock:
        connections["chirpstack_connected"] = False
    logger.warning("Disconnected from ChirpStack: %s", reason_code)


def chirpstack_on_message(client, userdata, message):
    global latest_lora_snapshot
    try:
        if not any(char in CHIRPSTACK_TOPIC for char in "+#") and message.topic != CHIRPSTACK_TOPIC:
            return
        uplink = json.loads(message.payload.decode("utf-8"))
        if not isinstance(uplink, dict) or int(uplink.get("fPort", -1)) != 1:
            raise ValueError("KY-032 requires ChirpStack fPort 1")
        encoded = uplink.get("data")
        if not isinstance(encoded, str):
            raise ValueError("Missing Base64 uplink data")
        raw = base64.b64decode(encoded, validate=True)
        if len(raw) != 1 or raw[0] not in (0, 1):
            raise ValueError("KY-032 payload must be exactly one byte: 0x00 or 0x01")

        received_at = uplink.get("time") or datetime.now().astimezone().isoformat(timespec="milliseconds")
        if not lora_is_newer(received_at):
            logger.info("Ignored older KY-032 uplink: %s", received_at)
            return
        detected = raw[0] == 1
        snapshot = {
            "sensor": "KY-032",
            "topic": message.topic,
            "received_at": received_at,
            "f_port": 1,
            "f_count": uplink.get("fCnt"),
            "payload_base64": encoded,
            "payload_hex": raw.hex().upper(),
            "obstacle_detected": detected,
            "obstacle_sensor_available": True,
        }
        with state_lock:
            latest_lora_snapshot = snapshot
        update_state(
            lora_received_at=received_at,
            lora_f_cnt=uplink.get("fCnt"),
            lora_f_port=1,
            lora_payload_hex=raw.hex().upper(),
            obstacle_detected=detected,
            obstacle_sensor_available=True,
            obstacle_received_at=received_at,
            obstacle_f_cnt=uplink.get("fCnt"),
            obstacle_payload_hex=raw.hex().upper(),
        )
        sent = publish_local(LORA_DATA_TOPIC, json.dumps(snapshot), retain=True)
        logger.info("KY-032 %s; MQTT snapshot published=%s", "DETECTED" if detected else "CLEAR", sent)
    except Exception as exc:
        logger.warning("Rejected ChirpStack uplink: %s", exc)


local_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=MQTT_CLIENT_ID)
local_mqtt.on_connect = local_on_connect
local_mqtt.on_disconnect = local_on_disconnect
local_mqtt.on_message = local_on_message
local_mqtt.reconnect_delay_set(min_delay=2, max_delay=30)
if MQTT_USERNAME:
    local_mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

chirpstack_mqtt = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=CHIRPSTACK_CLIENT_ID,
    transport="websockets",
)
chirpstack_mqtt.on_connect = chirpstack_on_connect
chirpstack_mqtt.on_disconnect = chirpstack_on_disconnect
chirpstack_mqtt.on_message = chirpstack_on_message
chirpstack_mqtt.reconnect_delay_set(min_delay=2, max_delay=30)
chirpstack_mqtt.ws_set_options(path=CHIRPSTACK_WS_PATH)
chirpstack_mqtt.tls_set(cert_reqs=ssl.CERT_REQUIRED)
if CHIRPSTACK_USERNAME:
    chirpstack_mqtt.username_pw_set(CHIRPSTACK_USERNAME, CHIRPSTACK_PASSWORD or None)


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "IoTDashboardREST/1.0"

    def log_message(self, fmt, *args):
        logger.info("%s - %s", self.address_string(), fmt % args)

    def send_bytes(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, data):
        self.send_bytes(status, json.dumps(data, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def do_OPTIONS(self):
        self.send_bytes(204, b"", "text/plain; charset=utf-8")

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            try:
                body = INDEX_FILE.read_bytes()
            except OSError:
                self.send_json(500, {"success": False, "message": "index.html not found"})
                return
            self.send_bytes(200, body, "text/html; charset=utf-8")
            return
        if self.path == "/api/health":
            with state_lock:
                result = dict(connections)
            self.send_json(200, {"success": True, **result})
            return
        if self.path == "/api/telemetry":
            with state_lock:
                result = dict(state)
                result.update(connections)
            lora_age = age_seconds(result.get("lora_received_at"))
            obstacle_age = age_seconds(result.get("obstacle_received_at"))
            result["lora_age_seconds"] = round(lora_age, 1) if lora_age is not None else None
            result["lora_status"] = "no_uplink" if lora_age is None else "fresh" if lora_age <= LORA_STALE_AFTER else "stale"
            result["obstacle_age_seconds"] = round(obstacle_age, 1) if obstacle_age is not None else None
            result["obstacle_status"] = (
                "not_available" if not result.get("obstacle_sensor_available")
                else "stale" if obstacle_age is None or obstacle_age > LORA_STALE_AFTER
                else "detected" if result.get("obstacle_detected") is True
                else "clear"
            )
            self.send_json(200, result)
            return
        self.send_json(404, {"success": False, "message": "Not found"})

    def do_POST(self):
        if self.path != "/api/led":
            self.send_json(404, {"success": False, "message": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 8192:
                raise ValueError("Invalid request body size")
            request = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(request, dict):
                raise ValueError("Request body must be a JSON object")
            action = str(request.get("action", "led")).strip().lower()
            value = str(request.get("state", "")).strip().lower()
            if action != "led" or value not in ("on", "off"):
                raise ValueError('Expected {"action":"led","state":"on|off"}')
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.send_json(400, {"success": False, "message": str(exc)})
            return

        result, error = send_led_command_and_wait(value)
        if error:
            self.send_json(503 if "MQTT" in error else 504, {"success": False, "message": error})
            return
        if not result.get("success"):
            self.send_json(502, {"success": False, "message": result.get("message", "LED command failed")})
            return
        confirmed = result.get("state", value)
        update_state(led=confirmed, led_output_percent=100 if confirmed == "on" else 0)
        self.send_json(200, {
            "success": True,
            "message": result.get("message", f"LED turned {confirmed}"),
            "state": confirmed,
            "request_id": result.get("request_id"),
        })


def start_mqtt_clients():
    try:
        local_mqtt.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
        local_mqtt.loop_start()
        logger.info("Starting local MQTT connection")
    except Exception:
        logger.exception("Could not start local MQTT connection")
    try:
        chirpstack_mqtt.connect_async(CHIRPSTACK_HOST, CHIRPSTACK_PORT, keepalive=60)
        chirpstack_mqtt.loop_start()
        logger.info("Starting ChirpStack connection")
    except Exception:
        logger.exception("Could not start ChirpStack connection")


def main():
    start_mqtt_clients()
    server = ThreadingHTTPServer((API_HOST, API_PORT), DashboardHandler)
    logger.info("Dashboard API listening on http://%s:%s", API_HOST, API_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping dashboard API")
    finally:
        server.server_close()
        for client in (local_mqtt, chirpstack_mqtt):
            try:
                client.loop_stop()
                client.disconnect()
            except Exception:
                logger.exception("MQTT shutdown failed")


if __name__ == "__main__":
    main()

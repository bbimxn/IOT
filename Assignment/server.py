import base64
import json
import logging
import os
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
from smbus2 import SMBus
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

# ---------------------------------------------------------------------------
# Hardware configuration (BCM GPIO numbering)
# ---------------------------------------------------------------------------
LIGHT = 4
TRIG_PIN = 23
ECHO_PIN = 24

# SHT31 I2C address is commonly 0x44; some modules use 0x45.
I2C_BUS_NUMBER = int(os.getenv("SHT31_I2C_BUS", "1"))
SHT31_ADDRESS = int(os.getenv("SHT31_ADDRESS", "0x44"), 0)

# ---------------------------------------------------------------------------
# MQTT configuration
# ---------------------------------------------------------------------------
BROKER = os.getenv("MQTT_BROKER", "mqtt-dashboard.com")
PORT = int(os.getenv("MQTT_PORT", "1883"))
DEVICE_ID = os.getenv("MQTT_CLIENT_ID", "ras67070102")

COMMAND_TOPIC = os.getenv("MQTT_COMMAND_TOPIC", "ras67070102/command")
COMMAND_RESULT_TOPIC = os.getenv(
    "MQTT_COMMAND_RESULT_TOPIC", "ras67070102/command/result"
)
TEMP_TOPIC = os.getenv("MQTT_TEMPERATURE_TOPIC", "ras67070102/temperature")
HUMIDITY_TOPIC = os.getenv("MQTT_HUMIDITY_TOPIC", "ras67070102/humidity")
DISTANCE_TOPIC = os.getenv("MQTT_DISTANCE_TOPIC", "ras67070102/distance")
LED_TOPIC = os.getenv("MQTT_LED_TOPIC", "ras67070102/led")
BRIGHTNESS_TOPIC = os.getenv("MQTT_BRIGHTNESS_TOPIC", "ras67070102/brightness")
LORA_DATA_TOPIC = os.getenv("MQTT_LORA_DATA_TOPIC", "ras67070102/lora")
TELEMETRY_TOPIC = os.getenv("MQTT_TELEMETRY_TOPIC", "ras67070102/telemetry")
SHT31_INTERVAL = max(1, int(os.getenv("MQTT_SHT31_INTERVAL", "3")))
DISTANCE_INTERVAL = max(0.2, float(os.getenv("MQTT_DISTANCE_INTERVAL", "0.25")))
LORA_STALE_AFTER_SECONDS = max(15, int(os.getenv("LORA_STALE_AFTER_SECONDS", "60")))

# ChirpStack v4 commonly publishes uplinks to:
# application/<application_id>/device/<dev_eui>/event/up
# Set this to the correct application ID (or keep + as wildcard).

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("RaspberryPi-IoT")

# Shared MQTT state
latest_mqtt_temperature = None
latest_lora_message = None
latest_temperature_c = None
latest_humidity_pct = None
latest_distance_cm = None
latest_temperature_timestamp = None
latest_humidity_timestamp = None
latest_distance_timestamp = None
data_lock = threading.Lock()
distance_lock = threading.Lock()
led_lock = threading.Lock()
led_state = "off"
led_pwm_duty_percent = 0
led_pwm_updated_at = None
brightness_raw = None
# Use full brightness until the first real LoRa potentiometer uplink arrives.
brightness_percent = 100
brightness_source = "default"

# Hardware setup
GPIO.setmode(GPIO.BCM)
GPIO.setup(LIGHT, GPIO.OUT, initial=GPIO.LOW)
# GPIO4 uses RPi.GPIO software PWM; potentiometer brightness is applied here.
led_pwm = GPIO.PWM(LIGHT, 1000)
led_pwm.start(0)
GPIO.setup(TRIG_PIN, GPIO.OUT, initial=GPIO.LOW)
GPIO.setup(ECHO_PIN, GPIO.IN)

# I2C bus is opened lazily, when a temperature/humidity tool is called.
_i2c_bus = None
_i2c_lock = threading.Lock()


def get_i2c_bus():
    """Open the Raspberry Pi I2C bus once and reuse it."""
    global _i2c_bus
    if _i2c_bus is None:
        _i2c_bus = SMBus(I2C_BUS_NUMBER)
    return _i2c_bus


def _sht31_crc(data):
    """Return the SHT31 CRC-8 for two bytes (polynomial 0x31, init 0xFF)."""
    crc = 0xFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = ((crc << 1) ^ 0x31) & 0xFF
            else:
                crc = (crc << 1) & 0xFF
    return crc


def read_sht31():
    """Read temperature (C) and relative humidity (%) from an SHT31."""
    with _i2c_lock:
        bus = get_i2c_bus()
        # SHT31 command is exactly two bytes: 0x24 0x00.
        # Use raw I2C messages because SMBus block-read adds an extra command
        # byte, which is not the read protocol expected by the SHT31.
        write_msg = __import__("smbus2").i2c_msg.write(
            SHT31_ADDRESS, [0x24, 0x00]
        )
        bus.i2c_rdwr(write_msg)
        time.sleep(0.020)
        read_msg = __import__("smbus2").i2c_msg.read(SHT31_ADDRESS, 6)
        bus.i2c_rdwr(read_msg)
        raw = list(read_msg)

    if len(raw) != 6:
        raise RuntimeError(f"Expected 6 bytes from SHT31, received {len(raw)}")

    if _sht31_crc(raw[0:2]) != raw[2]:
        raise RuntimeError("SHT31 temperature CRC check failed")
    if _sht31_crc(raw[3:5]) != raw[5]:
        raise RuntimeError("SHT31 humidity CRC check failed")

    raw_temp = (raw[0] << 8) | raw[1]
    raw_humidity = (raw[3] << 8) | raw[4]

    temperature_c = -45 + (175 * raw_temp / 65535.0)
    humidity_pct = 100 * raw_humidity / 65535.0
    humidity_pct = max(0.0, min(100.0, humidity_pct))
    return temperature_c, humidity_pct


def _now_iso():
    """Return a local, timezone-aware timestamp with millisecond precision."""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _store_sht31_reading(temperature_c, humidity_pct, publish=True):
    """Update both SHT31 values atomically from one physical sensor sample."""
    global latest_temperature_c, latest_humidity_pct
    global latest_temperature_timestamp, latest_humidity_timestamp

    temperature_c = round(float(temperature_c), 2)
    humidity_pct = round(float(humidity_pct), 2)
    timestamp = _now_iso()
    with data_lock:
        latest_temperature_c = temperature_c
        latest_humidity_pct = humidity_pct
        latest_temperature_timestamp = timestamp
        latest_humidity_timestamp = timestamp

    if publish:
        publish_value(TEMP_TOPIC, temperature_c)
        publish_value(HUMIDITY_TOPIC, humidity_pct)
        _publish_telemetry_snapshot()
    return temperature_c, humidity_pct, timestamp


def _store_distance_reading(distance_cm, publish=True, snapshot=False):
    """Update the latest ultrasonic value and its measurement timestamp."""
    global latest_distance_cm, latest_distance_timestamp
    distance_cm = round(float(distance_cm), 1)
    timestamp = _now_iso()
    with data_lock:
        latest_distance_cm = distance_cm
        latest_distance_timestamp = timestamp
    if publish:
        publish_value(DISTANCE_TOPIC, distance_cm)
    if snapshot:
        _publish_telemetry_snapshot()
    return distance_cm, timestamp


def _get_led_status_snapshot():
    """Read the last successfully applied GPIO4 software-PWM output state."""
    with data_lock:
        enable_state = led_state
        brightness = int(brightness_percent)
        pwm_duty = int(led_pwm_duty_percent)
        source = brightness_source
        raw = brightness_raw
        updated_at = led_pwm_updated_at
    return {
        "output_state": "ON" if pwm_duty > 0 else "OFF",
        "enable_state": enable_state.upper(),
        "pwm_duty_percent": pwm_duty,
        "brightness_percent": brightness,
        "brightness_source": source,
        "potentiometer_raw": raw,
        "state_updated_at": updated_at,
    }


def _parse_timestamp_age_seconds(timestamp):
    """Return age from an ISO timestamp, or None if it cannot be parsed."""
    if not timestamp:
        return None
    try:
        value = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.astimezone()
        return max(0.0, (datetime.now(value.tzinfo) - value).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        # LoRa uplinks arrive through rest_api.py, which bridges decoded values
        # to this broker. This server remains responsible for GPIO/PWM control.
        subscriptions = [
            (COMMAND_TOPIC, 1),
            (TEMP_TOPIC, 1),
            (LORA_DATA_TOPIC, 1),
        ]
        client.subscribe(subscriptions)
        logger.info(
            "MQTT connected; subscribed to command=%s, temperature=%s, LoRa data=%s",
            COMMAND_TOPIC,
            TEMP_TOPIC,
            LORA_DATA_TOPIC,
        )
    else:
        logger.error("MQTT connection failed: %s", reason_code)


def publish_command_result(request_id, success, message, **details):
    """Publish a non-retained acknowledgement for a web/API command."""
    payload = {
        "device_id": DEVICE_ID,
        "request_id": request_id,
        "success": bool(success),
        "message": message,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    payload.update(details)
    try:
        if not mqtt_client.is_connected():
            logger.warning("Cannot publish command result: MQTT is disconnected")
            return False
        info = mqtt_client.publish(
            COMMAND_RESULT_TOPIC,
            json.dumps(payload, ensure_ascii=False),
            qos=1,
            retain=False,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning("Failed to publish command result: rc=%s", info.rc)
            return False
        return True
    except Exception:
        logger.exception("Failed to publish command result")
        return False


def _apply_led_pwm():
    """Serialize PWM writes and record only duty cycles accepted by RPi.GPIO."""
    global led_pwm_duty_percent, led_pwm_updated_at
    with led_lock:
        with data_lock:
            enabled = led_state == "on"
            level = int(brightness_percent) if enabled else 0
        led_pwm.ChangeDutyCycle(float(level))
        with data_lock:
            led_pwm_duty_percent = level
            led_pwm_updated_at = _now_iso()


def apply_led_state(state):
    """Enable or disable the LED without changing its stored LoRa brightness."""
    normalized = str(state).strip().lower()
    if normalized not in ("on", "off"):
        raise ValueError("LED state must be 'on' or 'off'")

    global led_state
    with data_lock:
        led_state = normalized
    _apply_led_pwm()
    publish_value(LED_TOPIC, normalized)
    _publish_telemetry_snapshot()
    logger.info("Raspberry Pi LED control turned %s", normalized.upper())
    return normalized


def apply_led_brightness(raw_value):
    """Store a validated 10-bit LoRa ADC value and apply it as PWM brightness."""
    global brightness_raw, brightness_percent, brightness_source
    try:
        raw = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Potentiometer value must be an integer from 0 to 1023") from exc
    if not 0 <= raw <= 1023:
        raise ValueError("Potentiometer value must be from 0 to 1023")

    percent = int(round(raw * 100 / 1023))
    with data_lock:
        brightness_raw = raw
        brightness_percent = percent
        brightness_source = "lora"
    _apply_led_pwm()
    publish_value(BRIGHTNESS_TOPIC, percent)
    _publish_telemetry_snapshot()
    logger.info("LoRa potentiometer set LED brightness to %s%% (ADC=%s)", percent, raw)
    return percent


def handle_mqtt_command(payload):
    """Parse commands published by the website/backend to COMMAND_TOPIC.

    Supported examples:
      {"action":"led","state":"on"}
      {"action":"led","state":"off","request_id":"abc123"}
      {"led":"on"}
      on
      off
      {"action":"brightness","raw":512}
      {"action":"refresh_telemetry","request_id":"abc123"}
    """
    request_id = None
    try:
        try:
            command = json.loads(payload)
        except json.JSONDecodeError:
            command = payload.strip().lower()

        if isinstance(command, str):
            action = "led"
            state = command
        elif isinstance(command, dict):
            request_id = command.get("request_id") or command.get("id")
            action = str(command.get("action") or command.get("command") or "").strip().lower()
            state = command.get("state", command.get("value", command.get("led")))
            if not action and state is not None:
                action = "led"
        else:
            raise ValueError("Command must be a JSON object or the text 'on'/'off'")

        if action in ("led", "set_led", "light"):
            normalized = apply_led_state(state)
            publish_command_result(
                request_id,
                True,
                f"LED turned {normalized}",
                action="led",
                state=normalized,
            )
        elif action in ("brightness", "set_brightness"):
            if not isinstance(command, dict):
                raise ValueError("Brightness command must be a JSON object")
            raw = command.get("raw", command.get("potentiometer_raw"))
            percent = apply_led_brightness(raw)
            publish_command_result(
                request_id,
                True,
                f"LED brightness set to {percent}%",
                action="brightness",
                brightness_percent=percent,
            )
        elif action in ("refresh", "refresh_telemetry", "publish_telemetry"):
            publish_telemetry()
            publish_command_result(
                request_id,
                True,
                "Sensor telemetry refresh requested and published",
                action="refresh_telemetry",
            )
        else:
            raise ValueError(
                "Unsupported action. Use action 'led' with state 'on'/'off', "
                "or action 'refresh_telemetry'."
            )
    except Exception as exc:
        logger.warning("Rejected MQTT command on %s: %s", COMMAND_TOPIC, exc)
        publish_command_result(
            request_id,
            False,
            str(exc),
        )


def on_message(client, userdata, msg):
    global latest_mqtt_temperature, latest_lora_message

    payload = msg.payload.decode("utf-8", errors="replace")

    # Commands and sensor packets are processed without holding data_lock
    # across a hardware write; the handlers acquire that lock themselves.
    if msg.topic == COMMAND_TOPIC:
        logger.info("MQTT command received on %s: %s", COMMAND_TOPIC, payload)
        handle_mqtt_command(payload)
        return

    lora_raw_to_apply = None
    with data_lock:
        if msg.topic == TEMP_TOPIC:
            latest_mqtt_temperature = payload
            logger.info("MQTT temperature updated: %s", payload)
        elif msg.topic == LORA_DATA_TOPIC:
            # rest_api.py retains the decoded LoRa packet. Applying its ADC
            # here as well makes startup/reconnect safe if the one-shot command
            # was published while this server was offline.
            try:
                lora_data = json.loads(payload)
            except json.JSONDecodeError:
                lora_data = {"raw_payload": payload}
            latest_lora_message = {
                "topic": lora_data.get("topic", LORA_DATA_TOPIC),
                "received_at": lora_data.get("received_at", _now_iso()),
                "received_monotonic": time.monotonic(),
                "payload": lora_data,
            }
            candidate = lora_data.get("potentiometer_raw")
            received_at = lora_data.get("received_at", _now_iso())
            lora_age = _parse_timestamp_age_seconds(received_at)
            if (
                isinstance(candidate, int)
                and not isinstance(candidate, bool)
                and lora_age is not None
                and lora_age <= LORA_STALE_AFTER_SECONDS
            ):
                lora_raw_to_apply = candidate
            elif candidate is not None:
                logger.warning(
                    "LoRa potentiometer packet is stale or has no valid timestamp; "
                    "not applying its retained value (age=%s seconds)",
                    lora_age,
                )
            logger.info(
                "LoRa potentiometer update received: %s%%",
                lora_data.get("brightness_percent"),
            )

    # In normal operation the command arrives first. This retained-state path
    # is a no-op for an ADC value already applied, but recovers missed commands.
    if lora_raw_to_apply is not None:
        with data_lock:
            already_applied = (
                brightness_raw == lora_raw_to_apply and brightness_source == "lora"
            )
        if not already_applied:
            try:
                apply_led_brightness(lora_raw_to_apply)
                logger.info(
                    "Applied retained LoRa potentiometer value after MQTT delivery: ADC=%s",
                    lora_raw_to_apply,
                )
            except ValueError as exc:
                logger.warning("Ignored invalid retained LoRa potentiometer value: %s", exc)



mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=DEVICE_ID,
)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


def connect_mqtt():
    try:
        mqtt_client.connect(BROKER, PORT, 60)
        mqtt_client.loop_start()
        logger.info("Connecting to MQTT broker %s:%s", BROKER, PORT)
    except Exception:
        logger.exception("Could not connect to MQTT broker %s:%s", BROKER, PORT)


def publish_value(topic, value):
    """Publish the latest value and retain it for web clients that subscribe later."""
    try:
        if not mqtt_client.is_connected():
            logger.warning("MQTT not connected; cannot publish to %s yet", topic)
            return False
        info = mqtt_client.publish(topic, str(value), qos=1, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning("MQTT publish failed for %s: rc=%s", topic, info.rc)
            return False
        return True
    except Exception:
        logger.exception("MQTT publish error for %s", topic)
        return False


def _publish_telemetry_snapshot():
    """Publish the latest cached readings and current LED state as one JSON record."""
    with data_lock:
        telemetry = {
            "device_id": DEVICE_ID,
            "timestamp": _now_iso(),
            "temperature_c": latest_temperature_c,
            "humidity_pct": latest_humidity_pct,
            "distance_cm": latest_distance_cm,
            "temperature_timestamp": latest_temperature_timestamp,
            "humidity_timestamp": latest_humidity_timestamp,
            "distance_timestamp": latest_distance_timestamp,
            "led": led_state,
            "potentiometer_raw": brightness_raw,
            "brightness_percent": brightness_percent,
            "brightness_source": brightness_source,
        }
    publish_value(LED_TOPIC, telemetry["led"])
    publish_value(BRIGHTNESS_TOPIC, telemetry["brightness_percent"])
    publish_value(TELEMETRY_TOPIC, json.dumps(telemetry, ensure_ascii=False))


def publish_sht31_readings():
    """Read once from SHT31 and publish temperature and humidity together."""
    try:
        temperature_c, humidity_pct = read_sht31()
        temperature_c, humidity_pct, timestamp = _store_sht31_reading(
            temperature_c, humidity_pct, publish=True
        )
        logger.info(
            "Published SHT31 readings: temperature=%.2f C, humidity=%.2f%% at %s",
            temperature_c,
            humidity_pct,
            timestamp,
        )
        return temperature_c, humidity_pct, timestamp
    except Exception as exc:
        logger.warning("Could not read/publish SHT31 readings: %s", exc)
        return None


def publish_distance_reading():
    """Read and publish the ultrasonic distance independently of SHT31."""
    try:
        result = measure_distance_cm()
        if not isinstance(result, (int, float)):
            logger.warning("Ultrasonic reading failed: %s", result)
            return None
        distance_cm, timestamp = _store_distance_reading(
            result, publish=True, snapshot=False
        )
        logger.info("Published ultrasonic distance: %.1f cm at %s", distance_cm, timestamp)
        return distance_cm, timestamp
    except Exception as exc:
        logger.warning("Could not read/publish ultrasonic distance: %s", exc)
        return None


def publish_telemetry():
    """Force one immediate read of both sensor groups (used by refresh commands)."""
    publish_sht31_readings()
    publish_distance_reading()
    _publish_telemetry_snapshot()


def _run_periodic_sensor_loop(read_function, interval):
    """Run a sensor publisher on a fixed start-to-start interval."""
    while True:
        started = time.monotonic()
        read_function()
        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval - elapsed))


def sht31_loop():
    """Publish temperature and humidity every three seconds by default."""
    _run_periodic_sensor_loop(publish_sht31_readings, SHT31_INTERVAL)


def distance_loop():
    """Publish ultrasonic distance four times per second by default."""
    _run_periodic_sensor_loop(publish_distance_reading, DISTANCE_INTERVAL)

# Create MCP Server
mcp = MCPServer("RaspberryPi-IoT")


@mcp.tool()
def set_led(action: str) -> str:
    """CONTROL TOOL: physically turn the Raspberry Pi LED on or off.

    Use only when the user asks to turn the LED on or turn it off.
    For questions asking whether the LED is currently ON or OFF, use
    get_led_status instead; do not infer the state from this control tool.
    """
    try:
        normalized = apply_led_state(action)
        status = _get_led_status_snapshot()
        return (
            f"LED command applied on Raspberry Pi GPIO{LIGHT}. "
            f"Output={status['output_state']}; enable={status['enable_state']}; "
            f"PWM={status['pwm_duty_percent']}%; "
            f"brightness={status['brightness_percent']}%."
        )
    except ValueError as exc:
        return f"Error: {exc}"
    except Exception as exc:
        logger.exception("LED control failed")
        return f"Error applying LED command on Raspberry Pi: {exc}"


@mcp.tool()
def get_led_status() -> str:
    """READ-ONLY LIVE STATUS TOOL for the Raspberry Pi LED.

    Call this whenever the user asks whether the light/LED is currently on or
    off, how bright it is, or asks for its present state. It reads the current
    GPIO4 PWM state stored by the running Raspberry Pi controller and does not
    issue an ON/OFF command. Never answer an LED status question from memory,
    a previous tool response, or a cached web value.
    """
    try:
        status = _get_led_status_snapshot()
        _publish_telemetry_snapshot()
        return json.dumps(status, ensure_ascii=False)
    except Exception as exc:
        logger.exception("Could not read current LED status")
        return f"Error reading current Raspberry Pi LED status: {exc}"


def measure_distance_cm():
    """Read one current HC-SR04 distance measurement in cm."""
    with distance_lock:
        GPIO.output(TRIG_PIN, GPIO.LOW)
        time.sleep(0.000002)
        GPIO.output(TRIG_PIN, GPIO.HIGH)
        time.sleep(0.000010)
        GPIO.output(TRIG_PIN, GPIO.LOW)

        wait_start = time.monotonic()
        while GPIO.input(ECHO_PIN) == 0:
            if time.monotonic() - wait_start > 0.04:
                return "Error: ultrasonic sensor timeout while waiting for ECHO HIGH."

        pulse_start = time.monotonic()
        pulse_end = pulse_start
        while GPIO.input(ECHO_PIN) == 1:
            pulse_end = time.monotonic()
            if pulse_end - pulse_start > 0.04:
                return "Error: ultrasonic sensor timeout while waiting for ECHO LOW."
        distance_cm = (pulse_end - pulse_start) * 34300.0 / 2.0
        if not 2.0 <= distance_cm <= 400.0:
            return f"Distance out of range: {distance_cm:.1f} cm"
        return distance_cm


@mcp.tool()
def get_distance() -> str:
    """READ THE CURRENT HC-SR04 ultrasonic sensor now.

    Always trigger a fresh measurement for every user distance question. Do
    not answer from cached MQTT telemetry or an earlier reading.
    """
    try:
        result = measure_distance_cm()
        if not isinstance(result, (int, float)):
            return str(result)
        distance_cm, timestamp = _store_distance_reading(
            result, publish=True, snapshot=True
        )
        return f"Live HC-SR04 distance: {distance_cm:.1f} cm; measured_at={timestamp}."
    except Exception as exc:
        logger.exception("Ultrasonic read failed")
        return f"Error reading current HC-SR04 distance: {exc}"


@mcp.tool()
def get_temperature() -> str:
    """READ CURRENT TEMPERATURE DIRECTLY FROM THE PHYSICAL SHT31 NOW.

    Call for every temperature question. This performs a new I2C sensor read;
    it does not use the periodic cache or the last MQTT value. The same fresh
    sample also updates humidity, timestamps, and the shared MQTT telemetry.
    """
    try:
        temperature_c, humidity_pct = read_sht31()
        temperature_c, humidity_pct, timestamp = _store_sht31_reading(
            temperature_c, humidity_pct, publish=True
        )
        return (
            f"Live SHT31 temperature: {temperature_c:.2f} °C; "
            f"same-sample humidity: {humidity_pct:.2f}%; measured_at={timestamp}."
        )
    except Exception as exc:
        logger.exception("SHT31 temperature read failed")
        return (
            f"Error reading current SHT31 temperature: {exc}. "
            "Check I2C, sensor wiring, address (0x44/0x45), and smbus2."
        )


@mcp.tool()
def get_humidity() -> str:
    """READ CURRENT RELATIVE HUMIDITY DIRECTLY FROM THE PHYSICAL SHT31 NOW.

    Call for every humidity question. This performs a new I2C sensor read; it
    does not use the periodic cache or the last MQTT value. The same fresh
    sample also updates temperature, timestamps, and shared MQTT telemetry.
    """
    try:
        temperature_c, humidity_pct = read_sht31()
        temperature_c, humidity_pct, timestamp = _store_sht31_reading(
            temperature_c, humidity_pct, publish=True
        )
        return (
            f"Live SHT31 relative humidity: {humidity_pct:.2f}%; "
            f"same-sample temperature: {temperature_c:.2f} °C; measured_at={timestamp}."
        )
    except Exception as exc:
        logger.exception("SHT31 humidity read failed")
        return (
            f"Error reading current SHT31 humidity: {exc}. "
            "Check I2C, sensor wiring, address (0x44/0x45), and smbus2."
        )


@mcp.tool()
def get_lora_data() -> str:
    """READ THE MOST RECENT RECEIVED LoRaWAN POTENTIOMETER UPLINK.

    This is the last value delivered by the LoRaWAN network, not a continuous
    ADC reading. Always report its received timestamp and age; clearly mark it
    stale when it is older than the configured freshness window.
    """
    with data_lock:
        message = dict(latest_lora_message) if latest_lora_message else None

    if message is None:
        return (
            "No LoRa potentiometer uplink has been received by this Raspberry Pi yet. "
            "Check the Arduino OTAA join, ChirpStack uplink topic, and rest_api.py bridge."
        )

    payload = message.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            pass

    age_seconds = _parse_timestamp_age_seconds(message.get("received_at"))
    freshness = (
        "unknown" if age_seconds is None
        else "fresh" if age_seconds <= LORA_STALE_AFTER_SECONDS
        else "stale"
    )
    result = {
        "source": "last LoRaWAN uplink via ChirpStack MQTT",
        "freshness": freshness,
        "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "stale_after_seconds": LORA_STALE_AFTER_SECONDS,
        "topic": message.get("topic"),
        "received_at": message.get("received_at"),
        "payload": payload,
    }
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def get_sensor_status() -> str:
    """READ A FRESH LIVE SNAPSHOT OF ALL RASPBERRY PI SENSORS AND LED.

    Use when the user asks for all current sensor values, the device status,
    or multiple readings at once. This tool physically reads SHT31 temperature
    and humidity and triggers a new HC-SR04 distance measurement. It also reads
    the current GPIO4 PWM-controlled LED state and reports the last received
    LoRa potentiometer uplink with its timestamp/freshness. Never substitute
    old cached values for a failed physical sensor read; return the sensor error.
    """
    result = {
        "device_id": DEVICE_ID,
        "snapshot_at": _now_iso(),
    }

    # Each sensor is queried independently so one failed device does not hide
    # valid readings from the other devices.
    try:
        temperature_c, humidity_pct = read_sht31()
        temperature_c, humidity_pct, timestamp = _store_sht31_reading(
            temperature_c, humidity_pct, publish=True
        )
        result["sht31"] = {
            "status": "ok",
            "temperature_c": temperature_c,
            "humidity_pct": humidity_pct,
            "measured_at": timestamp,
        }
    except Exception as exc:
        logger.exception("SHT31 read failed during live device snapshot")
        result["sht31"] = {"status": "error", "error": str(exc)}

    try:
        distance = measure_distance_cm()
        if not isinstance(distance, (int, float)):
            raise RuntimeError(str(distance))
        distance_cm, timestamp = _store_distance_reading(
            distance, publish=True, snapshot=False
        )
        result["ultrasonic"] = {
            "status": "ok",
            "distance_cm": distance_cm,
            "measured_at": timestamp,
        }
    except Exception as exc:
        logger.exception("Ultrasonic read failed during live device snapshot")
        result["ultrasonic"] = {"status": "error", "error": str(exc)}

    with data_lock:
        lora_message = dict(latest_lora_message) if latest_lora_message else None
    if lora_message is None:
        result["lora_potentiometer"] = {
            "status": "no_uplink",
            "message": "No LoRa uplink received yet.",
        }
    else:
        lora_payload = lora_message.get("payload")
        if isinstance(lora_payload, str):
            try:
                lora_payload = json.loads(lora_payload)
            except json.JSONDecodeError:
                pass
        age_seconds = _parse_timestamp_age_seconds(lora_message.get("received_at"))
        result["lora_potentiometer"] = {
            "status": (
                "unknown" if age_seconds is None
                else "fresh" if age_seconds <= LORA_STALE_AFTER_SECONDS
                else "stale"
            ),
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "received_at": lora_message.get("received_at"),
            "data": lora_payload,
        }

    result["led"] = _get_led_status_snapshot()
    result["snapshot_at"] = _now_iso()
    _publish_telemetry_snapshot()
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def get_mqtt_temperature() -> str:
    """Compatibility alias: perform a fresh physical SHT31 temperature read.

    Do not return the old MQTT cache. Use get_temperature for new integrations.
    """
    return get_temperature()


if __name__ == "__main__":
    security = TransportSecuritySettings(
        allowed_hosts=[
            "wise-willow-fd1f401c.tunnl.gg",
            "wise-willow-fd1f401c.tunnl.gg:*",
            "localhost:8000",
            "127.0.0.1:8000",
            "0.0.0.0:8000",
        ],
        allowed_origins=[
            "https://wise-willow-fd1f401c.tunnl.gg",
            "https://wise-willow-fd1f401c.tunnl.gg/mcp",
            "http://wise-willow-fd1f401c.tunnl.gg/",
        ],
    )

    logger.info(
        "Sensor publish intervals: SHT31=%ss (temperature/humidity), distance=%ss",
        SHT31_INTERVAL,
        DISTANCE_INTERVAL,
    )
    connect_mqtt()
    threading.Thread(
        target=sht31_loop,
        name="mqtt-sht31-publisher",
        daemon=True,
    ).start()
    threading.Thread(
        target=distance_loop,
        name="mqtt-distance-publisher",
        daemon=True,
    ).start()
    try:
        mcp.run(
            transport="streamable-http",
            transport_security=security,
            host="127.0.0.1",
            port=8000,
        )
    finally:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
        except Exception:
            logger.exception("Error while stopping MQTT client")
        try:
            led_pwm.stop()
        except Exception:
            logger.exception("Error while stopping LED PWM")
        try:
            if _i2c_bus is not None:
                _i2c_bus.close()
        except Exception:
            logger.exception("Error while closing I2C bus")
        GPIO.cleanup()

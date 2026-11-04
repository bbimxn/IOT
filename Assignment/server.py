import json
import logging
import os
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
import RPi.GPIO as GPIO
from smbus2 import SMBus, i2c_msg
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings


# ============================================================
# HARDWARE CONFIGURATION
# ============================================================

LIGHT = 4
TRIG_PIN = 23
ECHO_PIN = 24

I2C_BUS_NUMBER = int(os.getenv("SHT31_I2C_BUS", "1"))
SHT31_ADDRESS = int(os.getenv("SHT31_ADDRESS", "0x44"), 0)


# ============================================================
# MQTT CONFIGURATION
# ============================================================

BROKER = os.getenv("MQTT_BROKER", "mqtt-dashboard.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
DEVICE_ID = os.getenv("MQTT_CLIENT_ID", "ras67070102")

COMMAND_TOPIC = os.getenv(
    "MQTT_COMMAND_TOPIC", "ras67070102/command"
)
COMMAND_RESULT_TOPIC = os.getenv(
    "MQTT_COMMAND_RESULT_TOPIC", "ras67070102/command/result"
)
TEMP_TOPIC = os.getenv(
    "MQTT_TEMPERATURE_TOPIC", "ras67070102/temperature"
)
HUMIDITY_TOPIC = os.getenv(
    "MQTT_HUMIDITY_TOPIC", "ras67070102/humidity"
)
DISTANCE_TOPIC = os.getenv(
    "MQTT_DISTANCE_TOPIC", "ras67070102/distance"
)
LED_TOPIC = os.getenv(
    "MQTT_LED_TOPIC", "ras67070102/led"
)
LORA_DATA_TOPIC = os.getenv(
    "MQTT_LORA_DATA_TOPIC", "ras67070102/lora"
)


# ============================================================
# INTERVAL CONFIGURATION
# ============================================================

SHT31_INTERVAL = max(
    1.0, float(os.getenv("MQTT_SHT31_INTERVAL", "3"))
)

DISTANCE_INTERVAL = max(
    2.0, float(os.getenv("MQTT_DISTANCE_INTERVAL", "2"))
)

LORA_STALE_AFTER = max(
    15, int(os.getenv("LORA_STALE_AFTER_SECONDS", "60"))
)

SENSOR_STALE_AFTER = max(
    10.0, 3.0 * max(SHT31_INTERVAL, DISTANCE_INTERVAL)
)


# HC-SR04 timeout settings

ECHO_READY_TIMEOUT = 0.01
ECHO_RISE_TIMEOUT = 0.03
ECHO_FALL_TIMEOUT = 0.03


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("RaspberryPi-IoT")


# ============================================================
# THREAD LOCKS AND STATE
# ============================================================

state_lock = threading.Lock()
distance_lock = threading.Lock()
led_lock = threading.Lock()
i2c_lock = threading.Lock()

mqtt_state = {
    "temperature_c": None,
    "humidity_pct": None,
    "distance_cm": None,
    "led": None,
    "ky032": None,
}

_i2c_bus = None

# LED software-controlled state
led_state = "off"
led_duty = 0


# ============================================================
# GPIO INITIALIZATION
# ============================================================

GPIO.setmode(GPIO.BCM)

GPIO.setup(LIGHT, GPIO.OUT, initial=GPIO.LOW)

led_pwm = GPIO.PWM(LIGHT, 1000)
led_pwm.start(0)

GPIO.setup(TRIG_PIN, GPIO.OUT, initial=GPIO.LOW)
GPIO.setup(ECHO_PIN, GPIO.IN)


# ============================================================
# COMMON FUNCTIONS
# ============================================================

def now_iso():
    return datetime.now().astimezone().isoformat(
        timespec="milliseconds"
    )


def parse_age(timestamp):
    if not timestamp:
        return None

    try:
        value = datetime.fromisoformat(
            str(timestamp).replace("Z", "+00:00")
        )

        if value.tzinfo is None:
            value = value.astimezone()

        return max(
            0.0,
            (datetime.now(value.tzinfo) - value).total_seconds()
        )

    except (TypeError, ValueError, OverflowError):
        return None


def get_i2c_bus():
    global _i2c_bus

    if _i2c_bus is None:
        _i2c_bus = SMBus(I2C_BUS_NUMBER)

    return _i2c_bus


# ============================================================
# SHT31 SENSOR
# ============================================================

def sht31_crc(data):
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
    with i2c_lock:
        bus = get_i2c_bus()

        write_msg = i2c_msg.write(
            SHT31_ADDRESS,
            [0x24, 0x00]
        )

        bus.i2c_rdwr(write_msg)
        time.sleep(0.020)

        read_msg = i2c_msg.read(SHT31_ADDRESS, 6)
        bus.i2c_rdwr(read_msg)

        raw = list(read_msg)

    if len(raw) != 6:
        raise RuntimeError(
            f"Expected 6 bytes from SHT31, received {len(raw)}"
        )

    if (
        sht31_crc(raw[0:2]) != raw[2]
        or sht31_crc(raw[3:5]) != raw[5]
    ):
        raise RuntimeError("SHT31 CRC check failed")

    raw_temp = (raw[0] << 8) | raw[1]
    raw_humidity = (raw[3] << 8) | raw[4]

    temperature = -45 + 175 * raw_temp / 65535.0

    humidity = max(
        0.0,
        min(100.0, 100 * raw_humidity / 65535.0)
    )

    return round(temperature, 2), round(humidity, 2)


# ============================================================
# MQTT PUBLISHING
# ============================================================

def publish_value(topic, value, retain=True):
    if not mqtt_client.is_connected():
        logger.warning(
            "MQTT disconnected; cannot publish %s",
            topic
        )
        return False

    info = mqtt_client.publish(
        topic,
        str(value),
        qos=1,
        retain=retain
    )

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        logger.warning(
            "MQTT publish failed for %s: rc=%s",
            topic,
            info.rc
        )
        return False

    return True


def publish_command_result(
    request_id,
    success,
    message,
    **details
):
    payload = {
        "device_id": DEVICE_ID,
        "request_id": request_id,
        "success": bool(success),
        "message": message,
        "timestamp": now_iso(),
    }

    payload.update(details)

    return publish_value(
        COMMAND_RESULT_TOPIC,
        json.dumps(payload),
        retain=False
    )


def set_mqtt_value(key, value):
    with state_lock:
        mqtt_state[key] = {
            "value": value,
            "received_at": now_iso(),
            "received_monotonic": time.monotonic(),
        }


# ============================================================
# LED CONTROL
# ============================================================

def apply_led_state(value):
    global led_state, led_duty

    normalized = str(value).strip().lower()

    if normalized not in ("on", "off"):
        raise ValueError(
            "LED state must be 'on' or 'off'"
        )

    duty = 100 if normalized == "on" else 0

    with led_lock:
        led_pwm.ChangeDutyCycle(float(duty))

        led_state = normalized
        led_duty = duty

    if publish_value(LED_TOPIC, normalized):
        set_mqtt_value("led", normalized)

    logger.info(
        "LED GPIO%d set to %s (PWM duty=%d%%)",
        LIGHT,
        normalized.upper(),
        duty
    )

    return normalized


def read_actual_led_status():
    """
    READ ONLY.

    Reads the software-controlled PWM state.
    This function NEVER changes GPIO or PWM.
    """

    with led_lock:
        return {
            "state": led_state.upper(),
            "gpio": LIGHT,
            "duty_cycle": led_duty,
            "source": "raspberry_pi_pwm_state",
            "read_only": True
        }


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
):
    if reason_code != 0:
        logger.error(
            "MQTT connection failed: %s",
            reason_code
        )
        return

    client.subscribe([
        (COMMAND_TOPIC, 1),
        (TEMP_TOPIC, 1),
        (HUMIDITY_TOPIC, 1),
        (DISTANCE_TOPIC, 1),
        (LED_TOPIC, 1),
        (LORA_DATA_TOPIC, 1),
    ])

    logger.info(
        "MQTT connected; subscribed to sensor, LED, command and KY-032 topics"
    )

    # Publish current software-controlled LED state.
    with led_lock:
        current_led = led_state

    if publish_value(LED_TOPIC, current_led):
        set_mqtt_value("led", current_led)


def handle_command(payload):
    request_id = None

    try:
        try:
            command = json.loads(payload)
        except json.JSONDecodeError:
            command = payload.strip().lower()

        if isinstance(command, str):
            action = "led"
            value = command

        elif isinstance(command, dict):
            request_id = (
                command.get("request_id")
                or command.get("id")
            )

            action = str(
                command.get("action")
                or command.get("command")
                or ""
            ).strip().lower()

            value = command.get(
                "state",
                command.get("value", command.get("led"))
            )

            if not action and value is not None:
                action = "led"

        else:
            raise ValueError(
                "Command must be 'on'/'off' or a JSON object"
            )

        if action in ("led", "set_led", "light"):
            result = apply_led_state(value)

            publish_command_result(
                request_id,
                True,
                f"LED turned {result}",
                action="led",
                state=result
            )

        elif action in (
            "refresh",
            "refresh_telemetry",
            "publish_telemetry"
        ):
            temp_ok = publish_sht31_readings()
            distance_ok = publish_distance_reading()

            publish_command_result(
                request_id,
                temp_ok and distance_ok,
                "Sensor refresh completed",
                action="refresh_telemetry",
                temperature_ok=temp_ok,
                distance_ok=distance_ok
            )

        else:
            raise ValueError(
                "Supported commands: LED on/off or refresh_telemetry"
            )

    except Exception as exc:
        logger.warning(
            "Rejected MQTT command: %s",
            exc
        )

        publish_command_result(
            request_id,
            False,
            str(exc)
        )


def on_message(client, userdata, message):
    topic = message.topic

    payload = message.payload.decode(
        "utf-8",
        errors="replace"
    ).strip()

    if topic == COMMAND_TOPIC:

        # Never execute old retained commands.
        if message.retain:
            logger.warning(
                "Ignored retained MQTT command: %s",
                payload
            )
            return

        handle_command(payload)
        return

    # Ignore old retained sensor readings.
    if (
        message.retain
        and topic in (
            TEMP_TOPIC,
            HUMIDITY_TOPIC,
            DISTANCE_TOPIC
        )
    ):
        return

    if topic in (
        TEMP_TOPIC,
        HUMIDITY_TOPIC,
        DISTANCE_TOPIC
    ):
        try:
            value = float(payload)
        except ValueError:
            logger.warning(
                "Ignored invalid MQTT number on %s: %r",
                topic,
                payload
            )
            return

        if topic == TEMP_TOPIC and not -40 <= value <= 125:
            return

        if topic == HUMIDITY_TOPIC and not 0 <= value <= 100:
            return

        if topic == DISTANCE_TOPIC and not 2 <= value <= 400:
            return

        key = {
            TEMP_TOPIC: "temperature_c",
            HUMIDITY_TOPIC: "humidity_pct",
            DISTANCE_TOPIC: "distance_cm",
        }[topic]

        set_mqtt_value(key, value)
        return

    if topic == LED_TOPIC:
        # LED topic is telemetry only.
        # Incoming LED messages must never control the GPIO.
        if (
            not message.retain
            and payload.lower() in ("on", "off")
        ):
            set_mqtt_value("led", payload.lower())

        return

    if topic == LORA_DATA_TOPIC:
        try:
            item = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning("Ignored invalid KY-032 JSON")
            return

        if not isinstance(item, dict):
            return

        if item.get("obstacle_sensor_available") is not True:
            return

        if not isinstance(item.get("obstacle_detected"), bool):
            return

        if parse_age(item.get("received_at")) is None:
            return

        if str(item.get("f_port")) != "1":
            return

        set_mqtt_value("ky032", item)

        logger.info(
            "KY-032 MQTT state: %s",
            "DETECTED"
            if item["obstacle_detected"]
            else "CLEAR"
        )


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=DEVICE_ID
)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


def connect_mqtt():
    try:
        mqtt_client.reconnect_delay_set(
            min_delay=2,
            max_delay=30
        )

        mqtt_client.connect_async(
            BROKER,
            MQTT_PORT,
            keepalive=60
        )

        mqtt_client.loop_start()

        logger.info(
            "Starting MQTT connection to %s:%s",
            BROKER,
            MQTT_PORT
        )

    except Exception:
        logger.exception(
            "Could not start MQTT connection"
        )


# ============================================================
# HC-SR04 ULTRASONIC SENSOR
# ============================================================

def wait_for_echo_level(level, timeout):
    """
    Wait until ECHO reaches the requested GPIO level.
    Returns the time.monotonic() timestamp.
    """

    deadline = time.monotonic() + timeout

    while GPIO.input(ECHO_PIN) != level:
        if time.monotonic() >= deadline:
            return None

    return time.monotonic()


def measure_distance_cm():
    with distance_lock:

        # Step 1: Ensure ECHO is LOW before starting.
        ready_time = wait_for_echo_level(
            GPIO.LOW,
            ECHO_READY_TIMEOUT
        )

        if ready_time is None:
            raise RuntimeError(
                "ECHO is stuck HIGH before trigger"
            )

        # Step 2: Send 10 microsecond TRIG pulse.
        GPIO.output(TRIG_PIN, GPIO.LOW)
        time.sleep(0.000002)

        GPIO.output(TRIG_PIN, GPIO.HIGH)
        time.sleep(0.000010)
        GPIO.output(TRIG_PIN, GPIO.LOW)

        # Step 3: Wait for ECHO HIGH.
        pulse_start = wait_for_echo_level(
            GPIO.HIGH,
            ECHO_RISE_TIMEOUT
        )

        if pulse_start is None:
            raise RuntimeError(
                "Timeout waiting for ECHO HIGH"
            )

        # Step 4: Wait for ECHO LOW.
        pulse_end = wait_for_echo_level(
            GPIO.LOW,
            ECHO_FALL_TIMEOUT
        )

        if pulse_end is None:
            raise RuntimeError(
                "Timeout waiting for ECHO LOW; check ECHO wiring, voltage divider and sensor"
            )

        # Step 5: Calculate distance.
        pulse_duration = pulse_end - pulse_start

        distance = (
            pulse_duration * 34300.0 / 2.0
        )

        if not 2 <= distance <= 400:
            raise RuntimeError(
                f"HC-SR04 distance out of range: {distance:.1f} cm"
            )

        return round(distance, 1)


# ============================================================
# SENSOR PUBLISH FUNCTIONS
# ============================================================

def publish_sht31_readings():
    try:
        temperature, humidity = read_sht31()

        temp_ok = publish_value(
            TEMP_TOPIC,
            temperature
        )

        humidity_ok = publish_value(
            HUMIDITY_TOPIC,
            humidity
        )

        if temp_ok and humidity_ok:
            logger.info(
                "SHT31 published: %.2f C, %.2f%%",
                temperature,
                humidity
            )
            return True

        logger.warning(
            "SHT31 read successfully, but MQTT publish failed"
        )
        return False

    except Exception:
        logger.exception(
            "SHT31 read/publish failed"
        )
        return False


def publish_distance_reading():
    try:
        distance = measure_distance_cm()

        published = publish_value(
            DISTANCE_TOPIC,
            distance
        )

        if published:
            logger.info(
                "HC-SR04 published: %.1f cm",
                distance
            )
            return True

        return False

    except Exception as exc:
        logger.warning(
            "HC-SR04 read/publish failed: %s",
            exc
        )
        return False


# ============================================================
# SENSOR THREAD
# ============================================================

def sensor_loop(read_function, interval):
    while True:
        started = time.monotonic()

        try:
            read_function()
        except Exception:
            logger.exception(
                "Unexpected sensor loop error"
            )

        elapsed = time.monotonic() - started

        time.sleep(
            max(0.0, interval - elapsed)
        )


# ============================================================
# MQTT STATE HELPERS
# ============================================================

def get_mqtt_value(key):
    with state_lock:
        item = mqtt_state.get(key)

        return (
            dict(item)
            if isinstance(item, dict)
            else None
        )


def mqtt_status(key):
    item = get_mqtt_value(key)

    if item is None:
        return {
            "status": "no_data",
            "value": None
        }

    age = (
        time.monotonic()
        - item["received_monotonic"]
    )

    status = (
        "stale"
        if age > SENSOR_STALE_AFTER
        else "fresh"
    )

    return {
        "status": status,
        "value": item["value"],
        "received_at": item["received_at"],
        "age_seconds": round(age, 1),
    }


def format_sensor_value(
    key,
    label,
    unit,
    digits=2
):
    item = get_mqtt_value(key)

    if item is None:
        return (
            f"No MQTT data received for {label} yet."
        )

    age = (
        time.monotonic()
        - item["received_monotonic"]
    )

    if age > SENSOR_STALE_AFTER:
        return (
            f"{label} MQTT data is stale "
            f"({age:.1f} seconds old). "
            f"Last value: {item['value']} {unit}."
        )

    return (
        f"Current {label}: "
        f"{item['value']:.{digits}f} {unit}; "
        f"received_at={item['received_at']}."
    )


# ============================================================
# MCP SERVER AND TOOLS
# ============================================================

mcp = MCPServer("RaspberryPi-IoT")


@mcp.tool()
def set_led(action: str) -> str:
    """
    CONTROL TOOL.

    Only call this tool when the user explicitly asks to turn
    the LED ON or OFF.

    Do not call this tool when the user asks whether the LED
    is currently ON or OFF.
    """

    try:
        result = apply_led_state(action)

        return (
            f"LED command executed. "
            f"Raspberry Pi LED is now {result.upper()} "
            f"(GPIO{LIGHT}, PWM {100 if result == 'on' else 0}%)."
        )

    except Exception as exc:
        return f"Error controlling LED: {exc}"


@mcp.tool()
def get_led_status() -> str:
    """
    READ-ONLY TOOL.

    Only reads the current software-controlled PWM state.
    NEVER turns the LED ON or OFF.
    NEVER calls set_led().
    NEVER changes GPIO or PWM.

    Use this tool whenever the user asks:
    - Is the LED ON or OFF?
    - What is the current LED status?
    - ตอนนี้ไฟเปิดหรือปิดอยู่?
    """

    try:
        status = read_actual_led_status()

        logger.info(
            "LED STATUS READ ONLY: %s (PWM %d%%)",
            status["state"],
            status["duty_cycle"]
        )

        return json.dumps(
            status,
            ensure_ascii=False
        )

    except Exception as exc:
        return json.dumps(
            {
                "status": "error",
                "message": str(exc),
                "read_only": True
            },
            ensure_ascii=False
        )


@mcp.tool()
def get_temperature() -> str:
    """Read the latest SHT31 temperature published to MQTT. Read-only."""

    return format_sensor_value(
        "temperature_c",
        "temperature",
        "°C"
    )


@mcp.tool()
def get_humidity() -> str:
    """Read the latest SHT31 relative humidity published to MQTT. Read-only."""

    return format_sensor_value(
        "humidity_pct",
        "humidity",
        "%"
    )


@mcp.tool()
def get_distance() -> str:
    """Read the latest HC-SR04 distance published to MQTT. Read-only."""

    return format_sensor_value(
        "distance_cm",
        "distance",
        "cm",
        digits=1
    )


# ============================================================
# KY-032 STATUS
# ============================================================

def obstacle_status():
    item = get_mqtt_value("ky032")

    if item is None:
        return {
            "sensor": "KY-032",
            "status": "no_uplink",
            "obstacle_detected": None,
            "message": "No KY-032 LoRa data has been received yet."
        }

    payload = item["value"]

    age = parse_age(
        payload.get("received_at")
    )

    detected = payload.get(
        "obstacle_detected"
    )

    if age is None or age > LORA_STALE_AFTER:
        status = "stale"
    else:
        status = (
            "detected"
            if detected
            else "clear"
        )

    return {
        "sensor": "KY-032",
        "status": status,
        "obstacle_detected": (
            detected if status != "stale" else None
        ),
        "label": (
            "DETECTED"
            if detected
            else "CLEAR"
        ) if status != "stale" else "UNKNOWN",
        "age_seconds": (
            round(age, 1)
            if age is not None
            else None
        ),
        "received_at": payload.get("received_at"),
        "f_port": payload.get("f_port"),
        "f_count": payload.get("f_count"),
        "payload_hex": payload.get("payload_hex"),
    }


@mcp.tool()
def get_obstacle_status() -> str:
    """Read the latest KY-032 obstacle status received through LoRa and MQTT. Read-only."""

    return json.dumps(
        obstacle_status(),
        ensure_ascii=False
    )


@mcp.tool()
def get_lora_data() -> str:
    """
    Read-only tool for KY-032 data received through LoRa
    and MQTT topic ras67070102/lora.

    payload_hex 01 means obstacle detected.
    payload_hex 00 means path clear.

    Never controls LED or any other hardware.
    """

    return json.dumps(
        obstacle_status(),
        ensure_ascii=False
    )


@mcp.tool()
def get_sensor_status() -> str:
    """Read all latest sensor values and actual software-controlled LED state. Read-only."""

    result = {
        "device_id": DEVICE_ID,
        "snapshot_at": now_iso()
    }

    for key in (
        "temperature_c",
        "humidity_pct",
        "distance_cm"
    ):
        result[key] = mqtt_status(key)

    # Read-only LED state.
    result["led"] = read_actual_led_status()

    result["ky032"] = obstacle_status()

    return json.dumps(
        result,
        ensure_ascii=False
    )


# ============================================================
# MCP SERVER CONFIGURATION
# ============================================================

MCP_PUBLIC_HOST = os.getenv(
    "MCP_PUBLIC_HOST",
    "pure-oak-b2d9d874.tunnl.gg"
).strip()

MCP_PUBLIC_ORIGIN = os.getenv(
    "MCP_PUBLIC_ORIGIN",
    f"https://{MCP_PUBLIC_HOST}"
).strip()

MCP_HOST = os.getenv(
    "MCP_HOST",
    "127.0.0.1"
)

MCP_PORT = int(
    os.getenv("MCP_PORT", "8000")
)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    allowed_hosts = [
        "localhost:8000",
        "127.0.0.1:8000",
        "localhost",
        "127.0.0.1"
    ]

    if MCP_PUBLIC_HOST:
        allowed_hosts.extend([
            MCP_PUBLIC_HOST,
            f"{MCP_PUBLIC_HOST}:*"
        ])

    allowed_origins = [
        "http://localhost:8000",
        "http://127.0.0.1:8000"
    ]

    if MCP_PUBLIC_ORIGIN:
        allowed_origins.append(
            MCP_PUBLIC_ORIGIN
        )

    security = TransportSecuritySettings(
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins
    )

    connect_mqtt()

    threading.Thread(
        target=sensor_loop,
        args=(
            publish_sht31_readings,
            SHT31_INTERVAL
        ),
        daemon=True
    ).start()

    threading.Thread(
        target=sensor_loop,
        args=(
            publish_distance_reading,
            DISTANCE_INTERVAL
        ),
        daemon=True
    ).start()

    try:
        mcp.run(
            transport="streamable-http",
            transport_security=security,
            host=MCP_HOST,
            port=MCP_PORT
        )

    finally:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

            led_pwm.stop()

            if _i2c_bus is not None:
                _i2c_bus.close()

        finally:
            GPIO.cleanup()

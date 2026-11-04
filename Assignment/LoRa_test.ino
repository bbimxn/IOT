
#include <WiFiS3.h>
#include <WiFiUdp.h>
#include <NTPClient.h>
#include <PubSubClient.h>
#include <time.h>

// ============================================================
// Arduino UNO R4 WiFi + KY-032
// WiFi test only: NO LoRa
//
// KY-032:
//   OUT -> D3
//   VCC -> 5V
//   GND -> GND
//   EN jumper installed
//
// KY-032 output:
//   LOW  = obstacle detected
//   HIGH = path clear
//
// Publishes to:
//   mqtt-dashboard.com:1883
//   ras67070102/lora
// ============================================================

// ใส่ Wi-Fi ของตัวเองตรงนี้
const char* WIFI_SSID = "Baimon";
const char* WIFI_PASSWORD = "baimon22";

// MQTT broker
const char* MQTT_HOST = "mqtt-dashboard.com";
const uint16_t MQTT_PORT = 1883;

// MQTT topic ให้ตรงกับระบบ Raspberry Pi
const char* MQTT_TOPIC = "ras67070102/lora";

// KY-032 signal pin
const uint8_t KY032_PIN = 3;

// Device identity
const char* DEVICE_ID = "arduino-uno-r4-wifi-ky032-test";

// Timing
const unsigned long PUBLISH_INTERVAL = 10000UL;
const unsigned long WIFI_RETRY_INTERVAL = 10000UL;
const unsigned long MQTT_RETRY_INTERVAL = 5000UL;

// Debounce
const uint8_t SAMPLE_COUNT = 5;
const uint8_t OBSTACLE_THRESHOLD = 3;
const unsigned long SAMPLE_DELAY_MS = 2;

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);

WiFiUDP ntpUDP;
NTPClient timeClient(ntpUDP, "pool.ntp.org", 0, 60000);

unsigned long lastPublish = 0;
unsigned long lastWiFiRetry = 0;
unsigned long lastMQTTRetry = 0;

uint32_t frameCount = 0;

bool lastObstacleState = false;
bool hasPreviousState = false;

// ------------------------------------------------------------
// Read KY-032 with simple majority debounce
// ------------------------------------------------------------
bool readObstacle() {
  uint8_t lowCount = 0;

  for (uint8_t i = 0; i < SAMPLE_COUNT; i++) {
    if (digitalRead(KY032_PIN) == LOW) {
      lowCount++;
    }
    delay(SAMPLE_DELAY_MS);
  }

  return lowCount >= OBSTACLE_THRESHOLD;
}

// ------------------------------------------------------------
// Format UTC timestamp as ISO 8601
// Example: 2026-11-04T16:30:00Z
// ------------------------------------------------------------
String getIsoTimestamp() {
  time_t epoch = (time_t)timeClient.getEpochTime();

  struct tm* utc = gmtime(&epoch);

  if (utc == nullptr || epoch < 1700000000) {
    return "";
  }

  char buffer[25];

  snprintf(
    buffer,
    sizeof(buffer),
    "%04d-%02d-%02dT%02d:%02d:%02dZ",
    utc->tm_year + 1900,
    utc->tm_mon + 1,
    utc->tm_mday,
    utc->tm_hour,
    utc->tm_min,
    utc->tm_sec
  );

  return String(buffer);
}

// ------------------------------------------------------------
// Connect Wi-Fi
// ------------------------------------------------------------
void connectWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  if (strlen(WIFI_SSID) == 0) {
    Serial.println("Please enter WiFi SSID and password.");
    return;
  }

  unsigned long now = millis();

  if (now - lastWiFiRetry < WIFI_RETRY_INTERVAL) {
    return;
  }

  lastWiFiRetry = now;

  Serial.print("Connecting to WiFi: ");
  Serial.println(WIFI_SSID);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
}

// ------------------------------------------------------------
// Connect MQTT
// ------------------------------------------------------------
void connectMQTT() {
  if (WiFi.status() != WL_CONNECTED || mqttClient.connected()) {
    return;
  }

  unsigned long now = millis();

  if (now - lastMQTTRetry < MQTT_RETRY_INTERVAL) {
    return;
  }

  lastMQTTRetry = now;

  String clientId = "KY032-UNO-R4-";
  clientId += String((uint32_t)random(0x7FFFFFFF), HEX);

  Serial.print("Connecting MQTT... ");

  if (mqttClient.connect(clientId.c_str())) {
    Serial.println("connected");
  } else {
    Serial.print("failed, state=");
    Serial.println(mqttClient.state());
  }
}

// ------------------------------------------------------------
// Publish KY-032 state
// ------------------------------------------------------------
void publishObstacle(bool obstacle) {
  if (!mqttClient.connected()) {
    Serial.println("MQTT disconnected; cannot publish.");
    return;
  }

  String timestamp = getIsoTimestamp();

  if (timestamp.length() == 0) {
    Serial.println("Waiting for valid NTP time.");
    return;
  }

  frameCount++;

  const char* payloadHex = obstacle ? "01" : "00";
  const char* payloadBase64 = obstacle ? "AQ==" : "AA==";

  String json = "{";
  json += "\"sensor\":\"KY-032\",";
  json += "\"device_id\":\"";
  json += DEVICE_ID;
  json += "\",";
  json += "\"topic\":\"arduino-wifi-test\",";
  json += "\"received_at\":\"";
  json += timestamp;
  json += "\",";
  json += "\"f_port\":1,";
  json += "\"f_count\":";
  json += String(frameCount);
  json += ",";
  json += "\"payload_base64\":\"";
  json += payloadBase64;
  json += "\",";
  json += "\"payload_hex\":\"";
  json += payloadHex;
  json += "\",";
  json += "\"obstacle_detected\":";
  json += obstacle ? "true" : "false";
  json += ",";
  json += "\"obstacle_sensor_available\":true";
  json += "}";

  bool ok = mqttClient.publish(
    MQTT_TOPIC,
    json.c_str(),
    true
  );

  if (ok) {
    Serial.println("--------------------------------");
    Serial.println("KY-032 MQTT publish successful");
    Serial.print("Obstacle: ");
    Serial.println(obstacle ? "DETECTED" : "CLEAR");
    Serial.print("Payload: ");
    Serial.println(payloadHex);
    Serial.print("MQTT topic: ");
    Serial.println(MQTT_TOPIC);
    Serial.print("JSON: ");
    Serial.println(json);
  } else {
    Serial.println("MQTT publish failed.");
  }
}

// ------------------------------------------------------------
// Setup
// ------------------------------------------------------------
void setup() {
  Serial.begin(115200);

  pinMode(KY032_PIN, INPUT);

  randomSeed(analogRead(A0));

  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setBufferSize(512);
  mqttClient.setKeepAlive(30);

  Serial.println();
  Serial.println("================================");
  Serial.println("UNO R4 WiFi - KY-032 TEST");
  Serial.println("LoRa is NOT used.");
  Serial.println("KY-032 signal pin: D3");
  Serial.println("================================");

  connectWiFi();

  timeClient.begin();
}

// ------------------------------------------------------------
// Main loop
// ------------------------------------------------------------
void loop() {
  connectWiFi();

  if (WiFi.status() == WL_CONNECTED) {
    timeClient.update();
    connectMQTT();
  }

  mqttClient.loop();

  bool obstacle = readObstacle();

  if (!hasPreviousState || obstacle != lastObstacleState) {
    hasPreviousState = true;
    lastObstacleState = obstacle;

    Serial.print("KY-032 state changed: ");
    Serial.println(obstacle ? "OBSTACLE DETECTED" : "CLEAR");

    if (mqttClient.connected()) {
      publishObstacle(obstacle);
      lastPublish = millis();
    }
  }

  unsigned long now = millis();

  if (now - lastPublish >= PUBLISH_INTERVAL) {
    lastPublish = now;
    publishObstacle(obstacle);
  }

  delay(20);
}

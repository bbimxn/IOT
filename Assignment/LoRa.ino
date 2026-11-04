#include <lmic.h>
#include <hal/hal.h>
#include <SPI.h>
#include <string.h>

// ------------------------------------------------------------
// Hardware configuration
// Arduino UNO R4 + Cytron LoRa Shield
// KY-032 S/OUT -> D3
// KY-032 + -> 5V, - -> GND
// Keep the KY-032 EN jumper installed (always enabled).
// ------------------------------------------------------------
const uint8_t OBSTACLE_PIN = 3;

// Wait after the previous TX/RX cycle completes.
const uint32_t SEND_INTERVAL_SECONDS = 10UL;
const uint32_t TX_RETRY_SECONDS = 2UL;

// ------------------------------------------------------------
// OTAA credentials (original values preserved)
// ------------------------------------------------------------
static const u1_t PROGMEM APPEUI[8] = {
  0xB1, 0xA4, 0x1C, 0x6C, 0x82, 0x29, 0x24, 0x73
};

static const u1_t PROGMEM DEVEUI[8] = {
  0xD9, 0x81, 0xE4, 0x6C, 0x17, 0x4B, 0x26, 0x93
};

static const u1_t PROGMEM APPKEY[16] = {
  0x29, 0xA7, 0x56, 0x3F,
  0xC7, 0x85, 0x84, 0xC0,
  0xE0, 0xAA, 0xD7, 0x1D,
  0xA8, 0x8A, 0x8B, 0xBA
};

void os_getArtEui(u1_t* buf) {
  memcpy_P(buf, APPEUI, sizeof(APPEUI));
}

void os_getDevEui(u1_t* buf) {
  memcpy_P(buf, DEVEUI, sizeof(DEVEUI));
}

void os_getDevKey(u1_t* buf) {
  memcpy_P(buf, APPKEY, sizeof(APPKEY));
}

// ------------------------------------------------------------
// Cytron LoRa Shield pin mapping
// ------------------------------------------------------------
const lmic_pinmap lmic_pins = {
  .nss = 10,
  .rxtx = LMIC_UNUSED_PIN,
  .rst = 7,
  .dio = {2, 5, 6},
};

static osjob_t sendjob;
static bool joined = false;
static uint32_t uplinkQueuedCount = 0;

void do_send(osjob_t* job);

void schedule_next_uplink(uint32_t delaySeconds) {
  os_setTimedCallback(
    &sendjob,
    os_getTime() + sec2osticks(delaySeconds),
    do_send
  );
}

// ------------------------------------------------------------
// KY-032 digital output
// LOW = obstacle detected
// HIGH = clear
// ------------------------------------------------------------
bool readObstacleDetected() {
  uint8_t lowCount = 0;
  const uint8_t sampleCount = 5;

  for (uint8_t i = 0; i < sampleCount; i++) {
    if (digitalRead(OBSTACLE_PIN) == LOW) {
      lowCount++;
    }
    delayMicroseconds(1000);
  }

  return lowCount >= 3;
}

// ------------------------------------------------------------
// Uplink payload: ONE byte, fPort 1
// 0x01 = obstacle detected
// 0x00 = path clear
// ------------------------------------------------------------
void do_send(osjob_t* job) {
  (void)job;

  if (!joined) {
    Serial.println(F("Not joined yet; waiting for OTAA."));
    return;
  }

  if (LMIC.opmode & OP_TXRXPEND) {
    Serial.println(F("TX/RX pending; retrying in 2 seconds."));
    schedule_next_uplink(TX_RETRY_SECONDS);
    return;
  }

  const bool obstacleDetected = readObstacleDetected();

  // FIX: Must be non-const for this LMIC library version.
  uint8_t payload[1] = {
    (uint8_t)(obstacleDetected ? 0x01 : 0x00)
  };

  // Unconfirmed uplink
  LMIC_setTxData2(1, payload, sizeof(payload), 0);

  uplinkQueuedCount++;

  Serial.print(F("Uplink #"));
  Serial.print(uplinkQueuedCount);
  Serial.print(F(" queued | KY-032: "));
  Serial.print(obstacleDetected ? F("DETECTED") : F("CLEAR"));
  Serial.print(F(" | payload: 0x"));
  Serial.print(payload[0], HEX);
  Serial.println();
}

// ------------------------------------------------------------
// LMIC event handler
// ------------------------------------------------------------
void onEvent(ev_t event) {
  Serial.print(os_getTime());
  Serial.print(F(": "));

  switch (event) {

    case EV_JOINING:
      Serial.println(F("EV_JOINING: requesting OTAA join"));
      break;

    case EV_JOINED:
      Serial.println(F("EV_JOINED: OTAA successful"));
      joined = true;
      LMIC_setLinkCheckMode(0);
      do_send(&sendjob);
      break;

    case EV_JOIN_FAILED:
      Serial.println(F("EV_JOIN_FAILED: OTAA join failed"));
      joined = false;
      break;

    case EV_TXSTART:
      Serial.println(F("EV_TXSTART: uplink transmission started"));
      break;

    case EV_TXCOMPLETE:
      Serial.println(F("EV_TXCOMPLETE: TX/RX cycle completed"));

      if (LMIC.txrxFlags & TXRX_ACK) {
        Serial.println(F("Received ACK"));
      }

      if (LMIC.dataLen) {
        Serial.print(F("Downlink received: "));
        Serial.print(LMIC.dataLen);
        Serial.println(F(" bytes"));
      }

      if (joined) {
        Serial.print(F("Next obstacle uplink queued in "));
        Serial.print(SEND_INTERVAL_SECONDS);
        Serial.println(F(" seconds."));

        schedule_next_uplink(SEND_INTERVAL_SECONDS);
      }
      break;

    default:
      Serial.print(F("LMIC event: "));
      Serial.println((unsigned)event);
      break;
  }
}

// ------------------------------------------------------------
// Setup
// ------------------------------------------------------------
void setup() {
  Serial.begin(115200);

  pinMode(OBSTACLE_PIN, INPUT);

  os_init();
  LMIC_reset();
  LMIC_setLinkCheckMode(0);
  LMIC_startJoining();

  Serial.println(F("LoRa KY-032-only transmitter started."));
  Serial.println(F("Payload: 1 byte; 0x01=DETECTED, 0x00=CLEAR; fPort=1."));

  Serial.print(F("Wait after TX/RX completion: "));
  Serial.print(SEND_INTERVAL_SECONDS);
  Serial.println(F(" seconds."));
}

// ------------------------------------------------------------
// Main loop
// ------------------------------------------------------------
void loop() {
  os_runloop();
}

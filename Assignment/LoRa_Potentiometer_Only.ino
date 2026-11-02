#include <lmic.h>
#include <hal/hal.h>
#include <SPI.h>
#include <string.h>

// ------------------------------------------------------------
// Hardware configuration
// Arduino UNO R4 + Cytron LoRa Shield
// Potentiometer: middle pin -> A0
// The switch input has been removed completely.
// ------------------------------------------------------------
const uint8_t POT_PIN = A0;
const unsigned long SEND_INTERVAL = 15000UL;  // 15 seconds after each TX completes

// ------------------------------------------------------------
// OTAA credentials
// DEVEUI and APPEUI are stored in little-endian byte order for LMIC.
// APPKEY is stored in big-endian byte order as provided from ChirpStack.
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
// Cytron LoRa Shield pin mapping (kept from the existing setup)
// ------------------------------------------------------------
const lmic_pinmap lmic_pins = {
  .nss = 10,
  .rxtx = LMIC_UNUSED_PIN,
  .rst = 7,
  .dio = {2, 5, 6},
};

static osjob_t sendjob;
static bool joined = false;

// Read the potentiometer and send exactly 2 bytes:
// byte 0 = ADC high byte, byte 1 = ADC low byte.
// The receiving side reconstructs it with (byte0 << 8) | byte1.
void do_send(osjob_t* job) {
  (void)job;

  if (!joined) {
    Serial.println(F("Not joined yet; waiting for OTAA."));
    return;
  }

  if (LMIC.opmode & OP_TXRXPEND) {
    Serial.println(F("TX/RX pending; uplink skipped."));
    return;
  }

  int potValue = analogRead(POT_PIN);
  potValue = constrain(potValue, 0, 1023);

  uint8_t payload[2];
  payload[0] = (uint8_t)((potValue >> 8) & 0xFF);
  payload[1] = (uint8_t)(potValue & 0xFF);

  LMIC_setTxData2(1, payload, sizeof(payload), 0);

  Serial.print(F("Potentiometer ADC: "));
  Serial.print(potValue);
  Serial.println(F(" | Uplink queued (2 bytes)"));
}

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
      Serial.println(F("EV_TXSTART"));
      break;

    case EV_TXCOMPLETE:
      Serial.println(F("EV_TXCOMPLETE"));
      if (LMIC.txrxFlags & TXRX_ACK) {
        Serial.println(F("Received ACK"));
      }
      if (LMIC.dataLen) {
        Serial.print(F("Downlink received: "));
        Serial.print(LMIC.dataLen);
        Serial.println(F(" bytes"));
      }

      if (joined) {
        // Schedule the next uplink 15 seconds after this TX/RX cycle completes.
        os_setTimedCallback(
          &sendjob,
          os_getTime() + sec2osticks(SEND_INTERVAL / 1000UL),
          do_send
        );
      }
      break;

    default:
      Serial.print(F("LMIC event: "));
      Serial.println((unsigned)event);
      break;
  }
}

void setup() {
  Serial.begin(115200);
  pinMode(POT_PIN, INPUT);
  // Match the two-byte payload decoder on the Raspberry Pi (ADC 0..1023).
  analogReadResolution(10);

  os_init();
  LMIC_reset();
  LMIC_setLinkCheckMode(0);
  LMIC_startJoining();

  Serial.println(F("LoRa potentiometer-only transmitter started."));
  Serial.println(F("Uplink payload: 2 bytes, ADC range 0-1023."));
}

void loop() {
  os_runloop();
}

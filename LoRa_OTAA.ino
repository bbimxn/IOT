
#include <lmic.h>
#include <hal/hal.h>
#include <SPI.h>
#include <string.h>

// =====================================================
// 1. OTAA credentials for ChirpStack device: test3
// =====================================================

// Device EUI shown in ChirpStack:
// 93264b176ce481d9
// LMIC requires LSB-first order.
static const u1_t PROGMEM DEVEUI[8] = {
  0xD9, 0x81, 0xE4, 0x6C,
  0x17, 0x4B, 0x26, 0x93
};

// Join EUI from the device creation page:
// 732429826c1ca4b1
// LMIC requires LSB-first order.
static const u1_t PROGMEM APPEUI[8] = {
  0xB1, 0xA4, 0x1C, 0x6C,
  0x82, 0x29, 0x24, 0x73
};

// Application Key from the test3 OTAA keys page.
// AppKey remains in MSB-first order.
static const u1_t PROGMEM APPKEY[16] = {
  0x29, 0xA7, 0x56, 0x3F,
  0xC7, 0x85, 0x84, 0xC0,
  0xE0, 0xAA, 0xD7, 0x1D,
  0xA8, 0x8A, 0x8B, 0xBA
};

// =====================================================
// 2. OTAA credential callbacks
// =====================================================

void os_getDevEui(u1_t* buf) {
  memcpy_P(buf, DEVEUI, 8);
}

void os_getArtEui(u1_t* buf) {
  memcpy_P(buf, APPEUI, 8);
}

void os_getDevKey(u1_t* buf) {
  memcpy_P(buf, APPKEY, 16);
}

// =====================================================
// 3. Cytron LoRa Shield pin mapping
// NSS = 10, RESET = 7, DIO0 = 2, DIO1 = 5, DIO2 = 6
// =====================================================

const lmic_pinmap lmic_pins = {
  .nss = 10,
  .rxtx = LMIC_UNUSED_PIN,
  .rst = 7,
  .dio = {2, 5, 6}
};

// =====================================================
// 4. Uplink settings
// =====================================================

static osjob_t sendjob;
static osjob_t joinjob;

const unsigned TX_INTERVAL_SECONDS = 60;
const unsigned JOIN_RETRY_SECONDS = 30;

const uint8_t TX_PORT = 1;

static uint8_t payload[] = "Hello, world!";

// Function declarations
void do_send(osjob_t* j);
void startJoining(osjob_t* j);

// =====================================================
// 5. Start or retry OTAA joining
// =====================================================

void startJoining(osjob_t* j) {
  Serial.println(F("Starting OTAA join attempt"));

  // Reset LMIC state before another attempt.
  LMIC_reset();
  LMIC_setLinkCheckMode(0);

  // Explicitly start OTAA joining.
  LMIC_startJoining();
}

// =====================================================
// 6. LMIC event handler
// =====================================================

void onEvent(ev_t ev) {
  Serial.print(os_getTime());
  Serial.print(F(": "));

  switch (ev) {

    case EV_RESET:
      Serial.println(F("EV_RESET"));
      break;

    case EV_JOINING:
      Serial.println(F("EV_JOINING: sending JoinRequest"));
      break;

    case EV_JOIN_TXCOMPLETE:
      Serial.println(
        F("EV_JOIN_TXCOMPLETE: waiting for JoinAccept")
      );
      break;

    case EV_JOINED:
      Serial.println(F("EV_JOINED: OTAA successful"));

      LMIC_setLinkCheckMode(0);

      Serial.println(F("Device joined successfully"));
      do_send(&sendjob);
      break;

    case EV_JOIN_FAILED:
      Serial.println(
        F("EV_JOIN_FAILED: check keys, Region and Gateway")
      );

      Serial.print(F("Retrying OTAA in "));
      Serial.print(JOIN_RETRY_SECONDS);
      Serial.println(F(" seconds"));

      os_setTimedCallback(
        &joinjob,
        os_getTime() + sec2osticks(JOIN_RETRY_SECONDS),
        startJoining
      );
      break;

    case EV_REJOIN_FAILED:
      Serial.println(F("EV_REJOIN_FAILED"));
      break;

    case EV_TXSTART:
      Serial.println(F("EV_TXSTART"));
      break;

    case EV_TXCOMPLETE:
      Serial.println(F("EV_TXCOMPLETE"));

      if (LMIC.txrxFlags & TXRX_ACK) {
        Serial.println(F("Received ACK"));
      }

      if (LMIC.dataLen > 0) {
        Serial.print(F("Downlink received: "));
        Serial.print(LMIC.dataLen);
        Serial.println(F(" bytes"));
      }

      // Schedule the next uplink.
      os_setTimedCallback(
        &sendjob,
        os_getTime() + sec2osticks(TX_INTERVAL_SECONDS),
        do_send
      );
      break;

    case EV_LINK_DEAD:
      Serial.println(F("EV_LINK_DEAD"));
      break;

    case EV_LINK_ALIVE:
      Serial.println(F("EV_LINK_ALIVE"));
      break;

    default:
      Serial.print(F("Other event: "));
      Serial.println((unsigned)ev);
      break;
  }
}

// =====================================================
// 7. Send an uplink
// =====================================================

void do_send(osjob_t* j) {
  // Do not send while another transmission or RX window
  // is still in progress.
  if (LMIC.opmode & OP_TXRXPEND) {
    Serial.println(F("TX/RX pending; send skipped"));
    return;
  }

  LMIC_setTxData2(
    TX_PORT,
    payload,
    sizeof(payload) - 1,
    0
  );

  Serial.println(F("Uplink queued"));
}

// =====================================================
// 8. Arduino setup
// =====================================================

void setup() {
  Serial.begin(9600);
  delay(1500);

  Serial.println();
  Serial.println(F("================================"));
  Serial.println(F("Arduino LoRaWAN OTAA"));
  Serial.println(F("ChirpStack device: test3"));
  Serial.println(F("================================"));

  os_init();
  LMIC_reset();

  // Disable link-check requests for this test.
  LMIC_setLinkCheckMode(0);

  // Begin OTAA explicitly.
  LMIC_startJoining();
}

// =====================================================
// 9. Main loop
// =====================================================

void loop() {
  os_runloop_once();
}

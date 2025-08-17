#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_GFX.h>
#include <Adafruit_ST7735.h>
#include <SPI.h>

// WiFi credentials
const char* ssid = "SystemSea";
const char* password = "123456789";

// MQTT broker
const char* mqtt_server = "192.168.12.12";

WiFiClient espClient;
PubSubClient client(espClient);
String message;
String macAddress;

// TFT Display Pins
#define TFT_CS     D8
#define TFT_RST    D4
#define TFT_DC     D3

#define green     D6
#define yellow    D2
#define red       D1

Adafruit_ST7735 tft = Adafruit_ST7735(TFT_CS, TFT_DC, TFT_RST);

char cardNumber[11];
bool newCardScanned = false;
bool displayUpdated = false;
bool operatorLoggedIn = false;

// Connection stability variables
unsigned long lastReconnectAttempt = 0;
const unsigned long RECONNECT_INTERVAL = 5000; // 5 seconds
const unsigned long HEARTBEAT_INTERVAL = 30000; // 30 seconds
const unsigned long STATUS_CHECK_INTERVAL = 5000; // 5 seconds
unsigned long lastHeartbeat = 0;
unsigned long lastStatusCheck = 0;
unsigned long lastStatusRequest = 0;

// Display functions
void printText(int x, int y, const char* text, uint16_t color, float size, bool clearScreen = true) {
    if (clearScreen) tft.fillScreen(ST77XX_BLACK);
    tft.setCursor(x, y);
    tft.setTextColor(color);
    tft.setTextSize(size);
    tft.println(text);
}

void displayMessage(const char* message, uint16_t bgColor, uint16_t textColor, float textSize = 1.5) {
    tft.fillScreen(bgColor);
    tft.setCursor(10, 50);
    tft.setTextColor(textColor);
    tft.setTextSize(textSize);
    tft.println(message);
}

void showStartupScreen() {
    tft.fillScreen(ST77XX_BLACK);
    tft.setTextColor(ST77XX_WHITE);
    digitalWrite(red, LOW);  
    digitalWrite(yellow, LOW);
    digitalWrite(green, LOW);
    tft.setTextSize(2);
    tft.setCursor(5, 10); tft.print("Welcome To");
    tft.setCursor(4, 40); tft.print("Mahr");
    tft.setCursor(30, 70); tft.print("Textiles");
    tft.setTextSize(1);
    tft.setCursor(20, 110); tft.print("DEV BY: SAMI ULLAH");
    delay(2000);
}

void rebootDevice() {
    displayMessage("System Rebooting...", ST77XX_BLACK, ST77XX_RED, 1.5);
    delay(3000);
    ESP.restart();
}

// WiFi connection
void setup_wifi() {
    digitalWrite(red, LOW);  
    digitalWrite(yellow, LOW);
    digitalWrite(green, HIGH);
    displayMessage("Connecting WiFi...", ST77XX_BLACK, ST77XX_YELLOW, 1.5);

    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, password);
    unsigned long startTime = millis();

    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        if (millis() - startTime > 300000) {
            rebootDevice();
        }
    }

    Serial.println("Connected to WiFi");
    macAddress = WiFi.macAddress();
    displayMessage("WiFi Connected", ST77XX_BLACK, ST77XX_GREEN, 1.5);
    digitalWrite(red, LOW);  
    digitalWrite(yellow, HIGH);
    digitalWrite(green, LOW);  
    delay(1000);
}

// WiFi checking
void checkWiFi() {
    if (WiFi.status() != WL_CONNECTED) {
        displayMessage("Reconnecting WiFi...", ST77XX_BLACK, ST77XX_WHITE, 1.5);
        digitalWrite(red, LOW);  
        digitalWrite(yellow, LOW);
        digitalWrite(green, HIGH); 
        
        WiFi.disconnect();
        delay(100);
        WiFi.begin(ssid, password);
        unsigned long startTime = millis();

        while (WiFi.status() != WL_CONNECTED) {
            delay(500);
            if (millis() - startTime > 300000) {
                rebootDevice();
            }
        }

        displayMessage("WiFi Reconnected", ST77XX_BLACK, ST77XX_GREEN, 1.5);
        digitalWrite(red, LOW);  
        digitalWrite(yellow, HIGH);
        digitalWrite(green, LOW);  
        delay(1000);
    }
}

// Heartbeat function
void sendHeartbeat() {
    if (client.connected()) {
        String topic = "nodemcu/" + macAddress + "/heartbeat";
        client.publish(topic.c_str(), "alive");
        lastHeartbeat = millis();
    }
}

void sendLoginStatusRequest() {
    String payload = "loginstatus " + macAddress;
    client.publish("nodemcu/rfid", payload.c_str());
    lastStatusCheck = millis();
}

void sendWorkstationStatusRequest() {
    if (client.connected() && operatorLoggedIn) {  // Only send if operator is logged in
        String payload = "workstationstatus " + macAddress;
        client.publish("nodemcu/rfid", payload.c_str());
        lastStatusRequest = millis();
    }
}

// MQTT reconnection
bool reconnectMQTT() {
    if (millis() - lastReconnectAttempt < RECONNECT_INTERVAL) {
        return false;
    }
    
    lastReconnectAttempt = millis();
    
    displayMessage("Connecting Server...", ST77XX_BLACK, ST77XX_WHITE, 1.5);
    digitalWrite(red, LOW);  
    digitalWrite(yellow, HIGH);
    digitalWrite(green, LOW);
    
    // Generate unique client ID
    String clientId = "NodeMCU-" + macAddress + "-" + String(millis());
    
    if (client.connect(clientId.c_str())) {
        Serial.println("Connected to MQTT");
        String responseTopic = "nodemcu/" + macAddress + "/response";
        client.subscribe(responseTopic.c_str());

        displayMessage("Server Connected", ST77XX_BLACK, ST77XX_GREEN, 1.5);
        delay(1000);

        sendLoginStatusRequest();
        sendHeartbeat();
        if (operatorLoggedIn) {
            sendWorkstationStatusRequest();
        }

        digitalWrite(red, LOW);  
        digitalWrite(yellow, HIGH);
        digitalWrite(green, HIGH);
        return true;
    }
    
    Serial.print("MQTT connection failed, rc=");
    Serial.print(client.state());
    return false;
}

// Callback function with improved error handling
void callback(char* topic, byte* payload, unsigned int length) {
    String message = "";
    for (int i = 0; i < length; i++) message += (char)payload[i];

    Serial.print("Received: ");
    Serial.println(message);

    // Handle login status
    if (message == "LOW") {
        operatorLoggedIn = true;
        digitalWrite(red, HIGH);
        digitalWrite(green, LOW);
        // displayMessage("Operator Logged In", ST77XX_BLACK, ST77XX_GREEN, 1.5);
        // Request workstation status since operator is now logged in
        sendWorkstationStatusRequest();
    } 
    else if (message == "HIGH") {
        operatorLoggedIn = false;
        digitalWrite(red, LOW);
        digitalWrite(green, HIGH);
        displayMessage("Please ", ST77XX_BLACK, ST77XX_YELLOW, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Login");
        // Set status to red when no operator is logged in
        digitalWrite(red, LOW);
        digitalWrite(yellow, HIGH);
        digitalWrite(green, HIGH);
        // displayMessage("No Operator Logged In", ST77XX_RED, ST77XX_WHITE, 1.5);
    }
    // Handle workstation status
    else if (message == "STATUS_RED") {
        if (operatorLoggedIn) {
            digitalWrite(red, LOW);
            digitalWrite(yellow, HIGH);
            digitalWrite(green, HIGH);
            // displayMessage("Machine Status: RED", ST77XX_RED, ST77XX_WHITE, 1.5);
        }
    } 
    else if (message == "STATUS_YELLOW") {
        if (operatorLoggedIn) {
            digitalWrite(red, HIGH);
            digitalWrite(yellow, LOW);
            digitalWrite(green, HIGH);
            // displayMessage("Machine Status: YELLOW", ST77XX_YELLOW, ST77XX_BLACK, 1.5);
        }
    }
    else if (message == "STATUS_GREEN") {
        if (operatorLoggedIn) {
            digitalWrite(red, HIGH);
            digitalWrite(yellow, HIGH);
            digitalWrite(green, LOW);
            // displayMessage("Machine Status: GREEN", ST77XX_GREEN, ST77XX_WHITE, 1.5);
        }
    }
    // Handle NO_OPERATOR response
    else if (message == "NO_OPERATOR") {
        operatorLoggedIn = false;
        digitalWrite(red, LOW);
        digitalWrite(yellow, HIGH);
        digitalWrite(green, HIGH);
        // displayMessage("No Operator", ST77XX_RED, ST77XX_WHITE, 1.5);
    }
    // Handle server responses
    else if (message.startsWith("LOGIN_SUCCESS")) {
        operatorLoggedIn = true;
        digitalWrite(red, HIGH);
        digitalWrite(green, LOW);
        displayMessage("Login", ST77XX_GREEN, ST77XX_WHITE, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Successful");
        sendWorkstationStatusRequest(); // Request status after successful login
    }
    else if (message.startsWith("LOGIN_EXISTS")) {
        operatorLoggedIn = true;
        displayMessage("Already", ST77XX_BLUE, ST77XX_YELLOW, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Logged In");
        sendWorkstationStatusRequest(); // Request status if already logged in
    }
    else if (message.startsWith("LOGIN_REQUIRED")) {
        operatorLoggedIn = false;
        displayMessage("Login", ST77XX_BLUE, ST77XX_WHITE, 2);
         tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Required");
        digitalWrite(red, LOW);
        digitalWrite(yellow, HIGH);
        digitalWrite(green, HIGH);
    }
    else if (message.startsWith("BUNDLE_STARTED")) {
        displayMessage("Bundle", ST77XX_YELLOW, ST77XX_BLACK, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Started");
    }
    else if (message.startsWith("BUNDLE_ENDED")) {
        displayMessage("Bundle", ST77XX_GREEN, ST77XX_BLACK, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Ended");
    }
    else if (message.startsWith("BUNDLE_ACTIVE_AT")) {
        String otherMac = message.substring(17);
        displayMessage("Active At:", ST77XX_BLUE, ST77XX_WHITE, 2);
         tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println(otherMac);
        
    }
    else if (message.startsWith("PREV_BUNDLE_ACTIVE")) {
        displayMessage("Complete", 0xFFE0, ST77XX_WHITE, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Previous");
    }
    else if (message.startsWith("BUNDLE_COMPLETED")) {
        displayMessage("Bundle ", ST77XX_BLUE, ST77XX_WHITE, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Completed");
    }
    else if (message.startsWith("UNAUTHORIZED_CARD")) {
        displayMessage("Invalid", ST77XX_BLUE, ST77XX_WHITE, 2);
         tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Card");
    }
    else if (message.startsWith("SYSTEM_ERROR")) {
        displayMessage("System ", ST77XX_RED, ST77XX_WHITE, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Error");
    }
    else {
        displayMessage("Unknown", ST77XX_RED, ST77XX_WHITE, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Response");
    }
    
    delay(2000);
    if (operatorLoggedIn) {
        displayMessage("READY...", ST77XX_GREEN, ST77XX_WHITE, 2);
    } else {
        displayMessage("Please", ST77XX_BLACK, ST77XX_YELLOW, 2);
        tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Login");
    }
}

// MQTT checking
void checkMQTT() {
    if (!client.connected()) {
        reconnectMQTT();
    } else {
        client.loop();
        
        // Send heartbeat periodically
        if (millis() - lastHeartbeat > HEARTBEAT_INTERVAL) {
            sendHeartbeat();
        }
        
        // Send login status check periodically
        if (millis() - lastStatusCheck > 30000) {
            sendLoginStatusRequest();
        }
        
        // Send workstation status request periodically only if operator is logged in
        if (operatorLoggedIn && millis() - lastStatusRequest > STATUS_CHECK_INTERVAL) {
            sendWorkstationStatusRequest();
        }
    }
}

void setup() {
    Serial.begin(9600);
    tft.initR(INITR_BLACKTAB);
    tft.setRotation(1);
    tft.fillScreen(ST77XX_BLACK);
    
    pinMode(green, OUTPUT);   
    pinMode(yellow, OUTPUT);
    pinMode(red, OUTPUT);

    digitalWrite(red, HIGH);
    digitalWrite(yellow, LOW);
    digitalWrite(green, LOW);

    showStartupScreen();
    setup_wifi();

    client.setServer(mqtt_server, 1883);
    client.setCallback(callback);
    reconnectMQTT();

    // Initial state - no operator logged in
    operatorLoggedIn = false;
    digitalWrite(red, LOW);
    digitalWrite(yellow, HIGH);
    digitalWrite(green, HIGH);
    displayMessage("Please", ST77XX_BLACK, ST77XX_YELLOW, 2);
     tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Login");
}

void loop() {
    checkWiFi();
    checkMQTT();

    if (Serial.available() >= 14) {
        String rfidData = Serial.readString();
        rfidData.trim();
        rfidData.replace("\x02", "");
        rfidData.replace("\x03", "");

        if (rfidData.length() % 12 == 0) {
            for (int i = 0; i < rfidData.length(); i += 12) {
                String singleTagData = rfidData.substring(i, i + 12);
                String cardDataHex = singleTagData.substring(2, 10);
                unsigned long cardDataDecimal = strtoul(cardDataHex.c_str(), NULL, 16);
                unsigned long printedNumber = cardDataDecimal % 10000000;
                snprintf(cardNumber, sizeof(cardNumber), "%010lu", printedNumber);

                message = "ID: " + String(cardNumber) + " Mac ID: " + macAddress;
                
                newCardScanned = true;
                displayUpdated = false;
            }
        }
        if (client.connected()) {
            client.publish("nodemcu/rfid", message.c_str());
            displayMessage("OK...", ST77XX_GREEN, ST77XX_WHITE, 2);
        } else {
            displayMessage("Server Offline", ST77XX_RED, ST77XX_WHITE, 1.5);
            delay(1000);
        }
    }

    if (!newCardScanned && !displayUpdated) {
        if (operatorLoggedIn) {
            displayMessage("READY...", ST77XX_GREEN, ST77XX_WHITE, 2);
        } else {
            displayMessage("Please", ST77XX_BLACK, ST77XX_YELLOW, 2);
             tft.setCursor(10, 70);
        tft.setTextSize(2);
        tft.println("Login");
        }
        displayUpdated = true;
    }
    
    delay(10); // Small delay to prevent watchdog triggers
}
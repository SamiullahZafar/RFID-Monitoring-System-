RFID Operator & Bundle Tracking System

 Overview
A complete IoT solution for textile factories to:
  Track operator logins via RFID cards
  Monitor bundle processing times (10-minute limit)
  Provide real-time visual alerts (LEDs + display)
  Centralize monitoring through MQTT server
Key Features
  10-minute bundle timer with overdue alerts
  Employee authentication via RFID cards
  Real-time MQTT communication
  Dashboard monitoring** (Python GUI)
 Visual feedback (TFT display + LED indicators)

 Components
1. Server Application (Python)
   MQTT broker interface
   Database integration (Oracle)
   GUI dashboard
   Bundle time monitoring

2. NodeMCU Client (C++)
   RFID card reading
   Status display (ST7735 TFT)
   LED indicators (Red/Yellow/Green)
   MQTT communication

How It Works
1. Operators scan their RFID cards to login
2. Bundles are scanned when work begins
3. System monitors bundle processing time:
   Within 10 mins: Green LED
   Exceeds 10 mins: Blinking Red LED
4. Dashboard shows all active stations and alerts

Installation
   bash
   Server
pip install -r requirements.txt
python mqtt_server.py

 NodeMCU
PlatformIO project included

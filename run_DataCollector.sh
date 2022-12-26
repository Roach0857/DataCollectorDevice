#!/bin/sh
sleep 30
cd /home/pi/DataCollectorDevice/
/usr/bin/screen -dmS "DataCollector" python3 DataCollector.py
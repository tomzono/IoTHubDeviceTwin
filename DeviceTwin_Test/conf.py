#!/usr/bin/python
import os
# Bluetooth adaptor
BT_DEV_ID = 0
# time interval for sensor status evaluation (sec.)
CHECK_SENSOR_STATE_INTERVAL_SECONDS = 300
INACTIVE_TIMEOUT_SECONDS = 60
# VANTIQ
VANTIQ_END_POINT = '#########'
VANTIQ_TOKEN = '#####'
VANTIQ_HEADERS = {
    'Authorization': 'Bearer ' + VANTIQ_TOKEN,
    'content-type': 'application/json'
}

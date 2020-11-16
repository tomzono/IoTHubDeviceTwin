import sys
import os
import argparse
import socket
import datetime
import threading
import struct
import conf
import ble
import json
import random
import time
from azure.iot.device import IoTHubDeviceClient, Message, MethodResponse
import sensor_beacon as envsensor
import DPS_derive_device_key as devicekey
import DPS_register_device as registerdevice

# constant
VER = 1.2
# Global variables
device_id = socket.gethostname()
sensor_list = []
flag_update_sensor_status = False
handling_data_count = 30
VANTIQ_FORWARD_HANDLING_DATA_COUNT = 30
sensordata = {}
RECEIVED_MESSAGES = 0

#Create_IoTHubConection
def iothub_client_init():
    derived_device_key = devicekey.derive_device_key(device_id)
    registration_result = registerdevice.register_device(device_id,derived_device_key)
    print ("The status was : {}".format(registration_result.status))
    print ("The etag is : {}".format(registration_result.registration_state.etag))
    print ("The assigned IoT_hub : {}".format(registration_result.registration_state.assigned_hub))

    if registration_result.status == "assigned":
        print ("Will send telemetry from the provisioned device with id {id}".format(id=device_id))
        device_client = IoTHubDeviceClient.create_from_symmetric_key(
            symmetric_key=derived_device_key,
            hostname=registration_result.registration_state.assigned_hub,
            device_id=registration_result.registration_state.device_id,
        )
    return device_client

#Send message to IoTHub and update DeviceTwin
def iothub_SendMessage(str):
    result = False
    message = str
    dict_message = json.loads(message)
    global VANTIQ_FORWARD_HANDLING_DATA_COUNT

    #Send DeviceTwin to IoTHub
    try:
        # get the twin
        twin = client.get_twin()
        #setting handling_data_count
        VANTIQ_FORWARD_HANDLING_DATA_COUNT = twin['desired']['intervaal']
        print ("VANTIQ_FORWARD_HANDLING_DATA_COUNT : {}".format(VANTIQ_FORWARD_HANDLING_DATA_COUNT))
        reported_properties = {
            "temperature" : dict_message['temperature'],
            "noise" : dict_message['noise'],
            "eco2" : dict_message['eco2'],
            "heat" : dict_message['heat'],
            "sensor_type" : dict_message['sensor_type'],
            "time" : dict_message['time'],
            "bt_address" : dict_message['bt_address'],
            "gateway" : dict_message['gateway']
        }
        client.patch_twin_reported_properties(reported_properties)
    except Exception as e:
        print ("Failed to send DeviceTwin to IoTHub")
        print("---------------------------------------------------------------------------")

    #send Message to IoTHub
    try:
        client.send_message(message)
        print ("Send data to IoTHub")
        print("---------------------------------------------------------------------------")
        result = True
    except Exception as e:
        print ("Failed to send Message to IoTHub")
        print("---------------------------------------------------------------------------")

    return result

#recive Message from IoTHub
def message_listener(client):
    global RECEIVED_MESSAGES
    while True:
        message = client.receive_message()
        RECEIVED_MESSAGES += 1
        print ("\nMessage received:")
        #print data and both system and application (custom) properties
        for property in vars(message).items():
            print ("    {0}".format(property))
        print ( "Total calls received: {}".format(RECEIVED_MESSAGES))



def method_request_handler(method_request):
    # Determine how to respond to the method request based on the method name
    if method_request.name == "system_reboot":
        payload = {"result": True, "status":"RebootingOS"}  # set response payload
        status = 200  # set return status code
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        client.send_method_response(method_response)
        print ("executed method1:system reboot")
        client.disconnect()
        print ("disconectin clients")
        time.sleep(10)
        print ("system reboot")
        os.system('sudo reboot')

    elif method_request.name == "method2":
        payload = {"result": True, "data": 1234}  # set response payload
        status = 200  # set return status code
        print("executed method2")
    else:
        payload = {"result": False, "data": "unknown method"}  # set response payload
        status = 400  # set return status code
        print("executed unknown method: " + method_request.name)

    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    client.send_method_response(method_response)




def parse_events(sock, loop_count=10):
    global sensor_list
    pkt = sock.recv(255)
    parsed_packet = ble.hci_le_parse_response_packet(pkt)

    if "bluetooth_le_subevent_name" in parsed_packet and \
            (parsed_packet["bluetooth_le_subevent_name"]
                == 'EVT_LE_ADVERTISING_REPORT'):

        if debug:
            for report in parsed_packet["advertising_reports"]:
                print ("----------------------------------------------------")
                print ("Found BLE device:", report['peer_bluetooth_address'])
                print ("Raw Advertising Packet:")
                print (ble.packet_as_hex_string(pkt, flag_with_spacing=True,
                                               flag_force_capitalize=True))
                print ("")
                for k, v in report.items():
                    if k == "payload_binary":
                        continue
                    print ("\t%s: %s" % (k, v))
                print ("")

        for report in parsed_packet["advertising_reports"]:
            if (ble.verify_beacon_packet(report)):
                sensor = envsensor.SensorBeacon(
                    report["peer_bluetooth_address_s"],
                    ble.classify_beacon_packet(report),
                    device_id,
                    report["payload_binary"])

                index = find_sensor_in_list(sensor, sensor_list)

                if debug:
                    print ("\t--- sensor data ---")
                    sensor.debug_print()
                    print ("")

                lock = threading.Lock()
                lock.acquire()

                if (index != -1):  # BT Address found in sensor_list
                    if sensor.check_diff_seq_num(sensor_list[index]):
                        handling_data(sensor)
                    sensor.update(sensor_list[index])
                else:  # new SensorBeacon
                    sensor_list.append(sensor)
                    handling_data(sensor)
                lock.release()
            else:
                pass
    else:
        pass
    return


# data handling
def handling_data(sensor):
    global handling_data_count
    if handling_data_count >= VANTIQ_FORWARD_HANDLING_DATA_COUNT:
        sensordata = sensor.forward_vantiq()
        messageResult = iothub_SendMessage(str(sensordata))
        handling_data_count = 0
    else:
        handling_data_count += 1

# check timeout sensor and update flag
def eval_sensor_state():
    global flag_update_sensor_status
    global sensor_list
    nowtick = datetime.datetime.now()
    for sensor in sensor_list:
        if (sensor.flag_active):
            pastSec = (nowtick - sensor.tick_last_update).total_seconds()
            if (pastSec > conf.INACTIVE_TIMEOUT_SECONDS):
                if debug:
                    print ("timeout sensor : " + sensor.bt_address)
                sensor.flag_active = False
    flag_update_sensor_status = True
    timer = threading.Timer(conf.CHECK_SENSOR_STATE_INTERVAL_SECONDS,
                            eval_sensor_state)
    timer.setDaemon(True)
    timer.start()


def print_sensor_state():
    print("---------------------------------------------------------------------------")
    print ("sensor status : %s (Intvl. %ssec)" % (datetime.datetime.today(),
           conf.CHECK_SENSOR_STATE_INTERVAL_SECONDS))

    for sensor in sensor_list:
        print (" " + sensor.bt_address, ": %s :" % sensor.sensor_type, \
            ("ACTIVE" if sensor.flag_active else "DEAD"), \
            "(%s)" % sensor.tick_last_update)
    print("---------------------------------------------------------------------------")


#  Utility function ###
def return_number_packet(pkt):
    myInteger = 0
    multiple = 256
    for c in pkt:
        myInteger += struct.unpack("B", c)[0] * multiple
        multiple = 1
    return myInteger


def return_string_packet(pkt):
    myString = ""
    for c in pkt:
        myString += "%02x" % struct.unpack("B", c)[0]
    return myString


def find_sensor_in_list(sensor, List):
    index = -1
    count = 0
    for i in List:
        if sensor.bt_address == i.bt_address:
            index = count
            break
        else:
            count += 1
    return index

# command line argument
def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help='debug mode',
                        action='store_true')
    parser.add_argument('--version', action='version',
                        version='%(prog)s ' + str(VER))
    args = parser.parse_args()
    return args


# main function
if __name__ == "__main__":
    try:
        flag_scanning_started = False

        #command line argument
        debug = False
        args = arg_parse()
        if args.debug:
            debug = True

        # reset bluetooth functionality
        try:
            if debug:
                print ("-- reseting bluetooth device")
            ble.reset_hci()
            if debug:
                print ("-- reseting bluetooth device : success")
        except Exception as e:
            print ("error enabling bluetooth device")
            print (str(e))
            sys.exit(1)


        # initialize bluetooth socket
        try:
            if debug:
                print ("-- open bluetooth device")
            sock = ble.bluez.hci_open_dev(conf.BT_DEV_ID)
            if debug:
                print ("-- ble thread started")
        except Exception as e:
            print ("error accessing bluetooth device: ", str(conf.BT_DEV_ID))
            print (str(e))
            sys.exit(1)

        # set ble scan parameters
        try:
            if debug:
                print ("-- set ble scan parameters")
            ble.hci_le_set_scan_parameters(sock)
            if debug:
                print ("-- set ble scan parameters : success")
        except Exception as e:
            print ("failed to set scan parameter!!")
            print (str(e))
            sys.exit(1)

        # start ble scan
        try:
            if debug:
                print ("-- enable ble scan")
            ble.hci_le_enable_scan(sock)
            if debug:
                print ("-- ble scan started")
        except Exception as e:
            print ("failed to activate scan!!")
            print (str(e))
            sys.exit(1)

        flag_scanning_started = True
        print ("envsensor_observer : complete initialization")
        print ("")

        # activate timer for sensor status evaluation
        timer = threading.Timer(conf.CHECK_SENSOR_STATE_INTERVAL_SECONDS,
                                eval_sensor_state)
        timer.setDaemon(True)
        timer.start()

        # preserve old filter setting
        old_filter = sock.getsockopt(ble.bluez.SOL_HCI,
                                     ble.bluez.HCI_FILTER, 14)
        # perform a device inquiry on bluetooth device #0
        # The inquiry should last 8 * 1.28 = 10.24 seconds
        # before the inquiry is performed, bluez should flush its cache of
        # previously discovered devices
        flt = ble.bluez.hci_filter_new()
        ble.bluez.hci_filter_all_events(flt)
        ble.bluez.hci_filter_set_ptype(flt, ble.bluez.HCI_EVENT_PKT)
        sock.setsockopt(ble.bluez.SOL_HCI, ble.bluez.HCI_FILTER, flt)

        client = iothub_client_init()
        # Set the method request handler on the client
        client.on_method_request_received = method_request_handler
        print ( "IoT Hub device sending periodic messages, press Ctrl-C to exit" )
        message_listener_thread = threading.Thread(target=message_listener, args=(client,))
        message_listener_thread.daemon = True
        message_listener_thread.start()
        print("---------------------------------------------------------------------------")
        time.sleep(5)
        while True:
            # parse ble event
            parse_events(sock)
            if flag_update_sensor_status:
                print_sensor_state()
                flag_update_sensor_status = False

    except KeyboardInterrupt:
        print ("IoTHubClient stopped")

    finally:
        if flag_scanning_started:
            # restore old filter setting
            sock.setsockopt(ble.bluez.SOL_HCI, ble.bluez.HCI_FILTER,
                            old_filter)
            ble.hci_le_disable_scan(sock)
        print ("Exit")

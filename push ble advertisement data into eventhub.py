import sys
import logging
import datetime
import time
import os
import json
import socket
import subprocess


from azure.eventhub import EventHubClient, Sender, EventData
from bluepy.btle import Scanner, DefaultDelegate, BTLEException

class ScanDelegate(DefaultDelegate):
    def __init__(self):
        DefaultDelegate.__init__(self)

#    def handleDiscovery(self, dev, isNewDev, isNewData):
#        if isNewDev:
#            print ("Discovered devicessss", dev.addr)
#        elif isNewData:
#            print ("Received new data from", dev.addr)



logger = logging.getLogger("azure")
hostname=socket.gethostname()

# Address can be in either of these formats:
# "amqps://<URL-encoded-SAS-policy>:<URL-encoded-SAS-key>@<mynamespace>.servicebus.windows.net/myeventhub"
# "amqps://<mynamespace>.servicebus.windows.net/myeventhub"
# For example:
ADDRESS = "amqps://xxxxxxx.servicebus.windows.net/xxxxxxxxxxx"

# SAS policy and key are not required if they are encoded in the URL
USER = "RootManageSharedAccessKey"
KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    try:
        
        start_time = time.time()
        
        while True:
            
            try:
             scanner = Scanner().withDelegate(ScanDelegate())
             devices = scanner.scan(0.33)
             for dev in devices:
                 if (dev.addr[0]=='d' and dev.addr[1]=='3'):
                     print("%d;%s;%s;%d" % (time.time(),hostname, dev.addr, dev.rssi))
                     sender.send(EventData(str("%d;%s;%s;%d" % (time.time(),hostname, dev.addr, dev.rssi))))
            
            #handle issue with BLE by restarting device and continue        
            except BTLEException:
                print("BLE exception occured")
                bash = "sudo hciconfig hci0 down && sudo hciconfig hci0 up"
                output = subprocess.check_output(['bash','-c', bash])
                time.sleep(20)
                print("BLE restarted")
                #raise
                #pass
                continue
    except:
        print("Other exception")
        raise
        
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass

import json

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import UpdateThingShadowRequest
from py_linq.py_linq import Enumerable

from Entity.NodeID import NodeID

class Handle():
    def __init__(self, readInfo, logger):
        self.logger = logger
        self.timeout = 10
        self.readInfo = readInfo
        self.nodeID = NodeID(readInfo)
        self.thingName = readInfo['awsIot']['thingName']
        self.shadowName = readInfo['awsIot']['shadowName']
        self.ipcClient = awsiot.greengrasscoreipc.connect(timeout=60.0)
        
    def Update(self, shadow):
        try:
            request = UpdateThingShadowRequest()
            request.thing_name = self.thingName
            request.shadow_name = self.shadowName
            request.payload = bytes(json.dumps(shadow), "utf-8")
            clientReault = self.ipcClient.new_update_thing_shadow()
            clientReault.activate(request)
            clientResponse = clientReault.get_response()
            result = clientResponse.result(self.timeout)
            self.logger.info("Update Shadow {0}".format(result.payload))
            return result.payload
        
        except Exception as ex:
            self.logger.warning("Shadow Update, ex: {0} | ".format(ex))
            raise ex
    
    def MakeDeviceHeartbeat(self, heartbeatPacket):
        heartbeat = None
        if len(heartbeatPacket) > 0:
            heartbeat = heartbeatPacket[0]['content']['object']['deviceHB']
        result = {"state":{"desired": {"welcome": "aws-iot"}}}
        listDevice = {}
        listDevice.update(self.GetDeviceState("inv", heartbeat))
        listDevice.update(self.GetDeviceState("sp", heartbeat))
        listDevice.update(self.GetDeviceState("irr", heartbeat))
        listDevice.update(self.GetDeviceState("temp", heartbeat))
        reported = {}
        reported['nodeID'] = self.nodeID.me
        reported['state'] = True
        reported['clientDevice'] = listDevice
        reported['thingName']=self.readInfo['awsIot']['thingName']
        result['state']['reported'] = reported
        return result
        
    def GetDeviceState(self, deviceType, heartbeat):
        result = {}
        result = Enumerable(self.readInfo['device'][deviceType]).to_dictionary(lambda x: x['devID'], lambda x:False)
        if heartbeat != None:
            if deviceType in heartbeat:
                heartbeatResult = Enumerable(heartbeat[deviceType]).to_dictionary(lambda x: x['deviceID'], lambda x:True)
                result.update(heartbeatResult)
        return result
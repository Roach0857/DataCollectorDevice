import json
import sys

import awsiot.greengrasscoreipc
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
from awsiot.greengrasscoreipc.model import UpdateThingShadowRequest
from itertools import groupby
from Entity.NodeID import NodeID


class Handle():
    def __init__(self, readInfo, settingInfo, logger):
        self.logger = logger
        self.timeout = 10
        self.readInfo = readInfo
        self.settingInfo = settingInfo
        self.nodeID = NodeID(readInfo)
        self.thingName = readInfo['awsIot']['thingName']
        self.shadowName = readInfo['awsIot']['shadowName']
        self.ipcClient = awsiot.greengrasscoreipc.connect(timeout=60.0)
        self.mqtt = MqttHandle(readInfo, logger)
        self.mqtt.Connect()

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
            self.logger.warning(
                "Shadow Update, ex: {0} | ".format(ex), exc_info=True)
            raise ex

    def MakeDeviceHeartbeat(self, heartbeatPacket):
        totalDevice = {}
        for deviceInfo in self.readInfo['device'].values():
            getTotalDevice = dict(map(lambda x:(x['devID'], {"state":False}), deviceInfo))
            totalDevice.update(getTotalDevice)
        heartbeat = None
        if len(heartbeatPacket) > 0:
            heartbeat = heartbeatPacket[0]['content']['object']['deviceHB']
        result = {"state": {"desired": {"welcome": "aws-iot"}}}
        totalHeartbeat = {}
        for heartbeatInfo in heartbeat.values():
            getHeartbeatInfo = dict(map(lambda x:(x['deviceID'], {"state":True}), heartbeatInfo))
            totalHeartbeat.update(getHeartbeatInfo)
        totalDevice.update(totalHeartbeat)
        reported = {}
        reported['nodeID'] = self.nodeID.me
        reported['state'] = True
        reported['clientDevice'] = totalDevice
        reported['thingName'] = self.readInfo['awsIot']['thingName']
        result['state']['reported'] = reported
        return result

    def MakeDeviceErrorCode(self, errPacket):
        shadowData = {}
        totalErr = sum(list(map(lambda x:x['content']['object']['err']['inv'], errPacket)),[])
        sortedErr = sorted(totalErr, key=lambda x:x['deviceID'])
        groupbyErr = groupby(sortedErr, key=lambda x:x['deviceID'])
        for deviceID, errList in groupbyErr:
            err = {}
            err['eventCode'] = {
                err['address']:hex(err['errCode'])[2:].zfill(4).upper() 
                for err in errList
                }
            shadowData[deviceID] = err  
        result = {"state": {"desired": {"welcome": "aws-iot"}}}
        reported = {}
        reported['nodeID'] = self.nodeID.me
        reported['state'] = True
        reported['clientDevice'] = shadowData
        reported['thingName'] = self.readInfo['awsIot']['thingName']
        result['state']['reported'] = reported
        return result

class MqttHandle():
    def __init__(self, readInfo, logger):
        self.logger = logger
        self.readInfo = readInfo
        self.nodeID = NodeID(readInfo)

    def on_connection_interrupted(self, connection, error, **kwargs):
        print("Connection interrupted. error: {0}".format(error))

    def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        print("Connection resumed. return_code: {0} session_present: {1}".format(
            return_code, session_present))
        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            print("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = connection.resubscribe_existing_topics()
            resubscribe_future.add_done_callback(self.on_resubscribe_complete)

    def on_resubscribe_complete(self, resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {0}".format(resubscribe_results))
        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

    def Connect(self):
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
        reported = {"state": {
            "reported": {
                "nodeID": self.nodeID.me,
                "state": False,
                "clientDevice": {},
                "thingName": self.readInfo['awsIot']['thingName']}}}
        lwt = mqtt.Will(topic="lwt/{}/update".format(self.readInfo['awsIot']['thingName']), qos=0, payload=bytes(
            json.dumps(reported), 'utf-8'), retain=False)
        mqttConnection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.readInfo['awsIot']['endpoint'],
            port=443,
            cert_filepath=self.readInfo['awsIot']['certificatePath'],
            pri_key_filepath=self.readInfo['awsIot']['privateKeyPath'],
            client_bootstrap=client_bootstrap,
            on_connection_interrupted=self.on_connection_interrupted,
            on_connection_resumed=self.on_connection_resumed,
            client_id="lwt-{}".format(self.readInfo['awsIot']['thingName']),
            clean_session=False,
            keep_alive_secs=30,
            http_proxy_options=None)
        mqttConnection.will = lwt
        mqttConnection.connect()
        self.logger.info("Connecting to {} with client ID '{}'...".format(
            self.readInfo['awsIot']['endpoint'], "lwt-{}".format(self.readInfo['awsIot']['thingName'])))

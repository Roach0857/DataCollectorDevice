from asyncio import Future
import json
import sys
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder

class Handle():
    def __init__(self, readInfo, nodeID):
        self.readInfo = readInfo
        self.nodeID = nodeID
        self.awsInfo = readInfo['awsIot']
        self.event_loop_group = io.EventLoopGroup(1)
        self.host_resolver = io.DefaultHostResolver(self.event_loop_group)
        self.connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.awsInfo['endpoint'],
            port=443,
            cert_filepath=self.awsInfo['certificatePath'],
            pri_key_filepath=self.awsInfo['privateKeyPath'],
            client_bootstrap=io.ClientBootstrap(
                self.event_loop_group, self.host_resolver),
            on_connection_interrupted=self.__on_connection_interrupted,
            on_connection_resumed=self.__on_connection_resumed,
            client_id="awsiot-{}".format(self.awsInfo['thingName']),
            clean_session=False,
            keep_alive_secs=30,
            http_proxy_options=None)

    def __on_connection_interrupted(self, error, **kwargs):
        print("Connection interrupted. error: {0}".format(error))
        
    def __on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        print("Connection resumed. return_code: {0} session_present: {1}".format(return_code, session_present))
        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            print("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = self.connection.resubscribe_existing_topics()
            resubscribe_future.add_done_callback(self.on_resubscribe_complete)
            
    def on_resubscribe_complete(self, resubscribe_future:Future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {0}".format(resubscribe_results))
        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))
                
    def Connect(self):
        reported = {"state": {
            "reported": {
                "nodeID": self.nodeID.me,
                "state": False,
                "clientDevice":{},
                "thingName":self.readInfo['awsIot']['thingName']}}}
        lwt = mqtt.Will(topic="lwt/{}/update".format(self.readInfo['awsIot']['thingName']), qos=0, payload=bytes(json.dumps(reported),'utf-8'), retain=False)
        self.connection.will = lwt
        connect_future = self.connection.connect()
        connect_future.result()
        print("Connecting to {} with client ID '{}'...".format(self.readInfo['awsIot']['endpoint'], "lwt-{}".format(self.readInfo['awsIot']['thingName'])))
        
    def Publish(self, payload: str):
        topic = f"rfdme/things/{self.readInfo['awsIot']['thingName']}/log"
        message = {"message":payload}
        publishFuture, packet_id = self.connection.publish(topic=topic, qos=mqtt.QoS.AT_MOST_ONCE, payload=json.dumps(message))
        publishResult = publishFuture.result()
        print(publishResult)
        print(f"Published {topic}, packetID:{packet_id}, payload:{payload}")

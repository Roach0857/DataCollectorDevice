# -*- coding: UTF-8 -*-
import os
import time

import Handler.AwsIot as HA
from Entity.GPIOInfo import GPIOInfo
from Entity.NodeErr import NodeErr
from Entity.NodeID import NodeID


class Mutual():
    def __init__(self, config, readInfo, shadowHandler:HA.Handle, restFunction, logger):
        self.config = config
        self.readInfo = readInfo
        self.mode = readInfo['nodeModel']
        self.backupObject = None
        self.shadowHandler = shadowHandler
        self.restFunction = restFunction
        self.logger = logger
        self.initialize()
        
    def initialize(self):
        self.backupObject = BackupObject(self.readInfo)
        
    def Process(self):
        if self.PingServer():
            self.backupObject.gpioInfo.myInternetPin.Set(True)
            return True
        else:
            self.backupObject.gpioInfo.myInternetPin.Set(False)
            self.logger.info("Ping Fialed")
            return False
        
    def PingServer(self):
        flag = False
        majorFlag = os.system('ping -c 1 www.google.com')
        backupFlag = os.system('ping -c 1 1.1.1.1')
        if ((majorFlag == 0) or (backupFlag == 0)):
            flag = True
        return flag

class MutualSlave(Mutual):
    def Process(self):
        if self.CheckMaster():
            shadowRequest = {
                "state": {
                    "reported": {
                        "nodeID": self.shadowHandler.nodeID.me,
                        "state": True,
                        "clientDevice":{},
                        "thingName":self.shadowHandler.thingName}
                    }}
            self.shadowHandler.Update(shadowRequest)
            time.sleep(1)
            return False
        else:
            return True
        
    def CheckAlive(self, inputGpio):
        if inputGpio.Get():
            return True
        else:
            return False

    def CheckMaster(self):
        flag = True
        if not self.CheckAlive(self.backupObject.gpioInfo.yourWorkPin):
            flag = False
            return flag
        internetFlag = self.CheckAlive(self.backupObject.gpioInfo.yourInternetPin)
        self.backupObject.nodeErr.Make(internetFlag, self.backupObject.nodeID.you)
        self.logger.info("internetFlag:{0}".format(internetFlag))
        if not internetFlag:
            flag = False
        return flag
    
class MutualSingle(Mutual):
    def initialize(self):
        self.backupObject = BackupObjectSingle(self.readInfo)
        
    def Process(self):
        self.logger.info("Internet Status:{0}".format(self.PingServer()))
        return True
        
class BackupObject():
    def __init__(self, readInfo):
        self.readInfo = readInfo
        self.nodeID = NodeID(readInfo)
        self.nodeErr = NodeErr()
        self.gpioInfo = None
        self.initialize()
        
    def initialize(self):
        self.gpioInfo = GPIOInfo(self.readInfo['nodeModel'])
        
class BackupObjectSingle(BackupObject):
    def initialize(self):
        self.gpioInfo = None
        
class AbsMutualInterface():
    def __init__(self, Mutual:Mutual):
        self.Mutual = Mutual
        self.backupObject = Mutual.backupObject
        
    def Process(self):
        return self.Mutual.Process()
        
    def PingServer(self):
        return self.Mutual.PingServer()
    
def MutualFactory(config, readInfo, shadowHandler, restFunction, logger):
    factory = {"master":Mutual, "slave":MutualSlave, "single":MutualSingle}
    if readInfo['nodeModel'] in factory:
        result = factory[readInfo['nodeModel']](config, readInfo, shadowHandler, restFunction, logger)
    else:
        logger.info("Config Setting Wrong!, Please Check Config!")
    return AbsMutualInterface(result)
        
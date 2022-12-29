# -*- coding: UTF-8 -*-
from queue import Queue
import sys, os
import time

from apscheduler.schedulers.background import BackgroundScheduler
from Entity.NodeID import NodeID

import Entity.Period as EP
import Handler.Config as HC
import Handler.Logger as HL
import Handler.Packet as HP
import Handler.Read as HR
import Handler.Mqtt as HM
from Entity.GPIOInfo import GPIO


class ModeOperation():
    def __init__(self, config, settingInfo, readInfo, logger):
        self.config = config
        self.readInfo = readInfo
        self.settingInfo = settingInfo
        self.logger = logger
        self.period = EP.Period(**self.config['period'])
    
    def initialize(self, packet:HP.Handle):
        for name, selector in packet.extraSelector.items():
            selector.flag = self.config[name]['flag']

    def Main(self):
        kinesisData = Queue()
        socketData = Queue()
        packet = HP.Handle(self.config, self.settingInfo, self.readInfo, socketData, kinesisData, self.period, self.logger)
        readHandler = HR.Handle(self.settingInfo, self.readInfo, self.period, packet.parserData, packet.backupObject, self.logger)
        mainJob = BackgroundScheduler()
        mainJob.add_job(readHandler.DoRead, 'cron', minute='*', id='DoRead')
        mainJob.add_job(packet.DoSelect, 'cron', second='*/15', id='DoSelect')
        mainJob.start()
        while(True):
            systemFlag = packet.mutualHandler.Process()
            readHandler.systemFlag = systemFlag
            packet.systemFlag = systemFlag
            self.logger.info("System Flag:{0}".format(systemFlag))
            time.sleep(60)
                        
if __name__ == '__main__':
    try:
        # debug
        # thingName = "rfdme-raspberry-T0000-01"
        thingName = sys.argv[1]
        configHandler = HC.Handle(thingName)
        readInfo = configHandler.readInfo
        settingInfo = configHandler.settingInfo
        config = configHandler.config
        mqtt = HM.Handle(readInfo, NodeID(readInfo))
        mqtt.Connect()
        processorLogger = HL.Handle(settingInfo['Log'], mqtt)
        logger = processorLogger.GetLogger()
        try:
            mo = ModeOperation(config, settingInfo, readInfo, logger)
            mo.Main()
        except Exception as ex:
            logger.critical("DataCollector, ex: {0} |".format(ex))
            HL.SystemExceptionInfo()
        finally:
            GPIO.cleanup()
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_obj)
        print("{0} | Line:{1} | {2}".format(fname, exc_tb.tb_lineno, exc_type))
        raise ex
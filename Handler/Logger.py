# -*- coding: UTF-8 -*-
import logging
import logging.handlers
import os
import re
import sys
import Handler.Mqtt as HM

def SystemExceptionInfo():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    return f"{fname} | Line:{exc_tb.tb_lineno} | {exc_type}"

class ECULogger():
    def __init__(self, logger:logging.Logger, mqtt:HM.Handle):
        self.logger = logger
        self.mqtt = mqtt

    def debug(self, string:str):
        self.logger.debug(string)

    def info(self, string:str):
        self.logger.info(string)

    def warning(self, string:str):
        self.mqtt.Publish(string)
        self.logger.warning(string, exc_info=True)

    def critical(self, string:str):
        self.mqtt.Publish(string)
        self.logger.critical(string, exc_info=True)

    def error(self, string:str):
        self.mqtt.Publish(string)
        self.logger.error(string, exc_info=True)

class Handle():
    def __init__(self, settingInfo, mqtt):
        self.settingInfo = settingInfo
        self.mqtt = mqtt

    def GetLogger(self):
        logger = logging.getLogger("DataCollector")
        logger.setLevel(self.settingInfo['ConsoleLevel'])
        logFormat = logging.Formatter('%(asctime)s - %(thread)d | %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        if not os.path.isdir(self.settingInfo['Path']):
            os.makedirs(self.settingInfo['Path'])
        fileLogHandler = logging.handlers.TimedRotatingFileHandler(filename=self.settingInfo['Path'] + "DataCollector", when="MIDNIGHT", interval=1, backupCount=7, encoding='utf-8')
        fileLogHandler.suffix = "%Y-%m-%d.log"
        fileLogHandler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        fileLogHandler.setFormatter(logFormat)
        fileLogHandler.setLevel(self.settingInfo['FileLevel'])
        logger.addHandler(fileLogHandler)
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setLevel(self.settingInfo['ConsoleLevel'])
        streamHandler.setFormatter(logFormat)
        logger.addHandler(streamHandler)
        return ECULogger(logger, self.mqtt)
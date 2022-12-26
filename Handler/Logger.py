# -*- coding: UTF-8 -*-
import logging
import logging.handlers
import os
import re
import sys


def SystemExceptionInfo():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print("{0} | Line:{1} | {2}".format(fname, exc_tb.tb_lineno, exc_type))
    
class Handle():
    def __init__(self, settingInfo):
        self.settingInfo = settingInfo
        
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
        return logger
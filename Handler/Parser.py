# -*- coding: UTF-8 -*-
from copy import deepcopy
from datetime import datetime
from logging import Logger

import Factory.Parser as FP
import Handler.Calculator as HC
import Handler.Logger as HL
import Handler.OldParser as HOP
import Processor.TienJi as PTJ
from Entity.ModbusData import ModbusData
from Entity.RawData import RawData
from Entity.TotalModbusData import TotalModbusData


class Handle():
    def __init__(self, settingInfo, readInfo, logger:Logger):
        self.settingInfo = settingInfo
        self.readInfo = readInfo
        self.logger = logger
        self.label = None
        self.strID = None
        self.objectID = readInfo['projectID']
        self.deviceID = {"inv" :'01',"sp" :'00',"irr" :'04',"temp" :'05'}
        self.dataProcessor = [PTJ.Handle(self.settingInfo, self.objectID, self.readInfo['oldFlag'])]

    def DoParser(self, rawData:RawData, startTime:datetime, backupObject):
        parserRawDataResult = {}
        processResult = {'heartbeat':None, 'node':None}
        packetParserResult = {'device':None, 'heartbeat':None, 'oqc':None, 'err':None, 'node':None}
        self.logger.info("Start Parser Data")
        processTime = startTime.strftime("%Y-%m-%d %H:%M:%S")
        parserRawDataResult['heartbeat'] = self.ParserRawData(rawData.currentData['device'], 'device')
        parserRawDataResult['device'] = self.ParserRawData(rawData.mergeData['device'], 'device')
        parserRawDataResult['err'] = self.ParserRawData(rawData.mergeData['err'], 'err')
        
        processResult['oqc'] = {'data':deepcopy(parserRawDataResult['device']), 'objectID':self.objectID}
        processResult['heartbeat'] = [{'data':deepcopy(parserRawDataResult['heartbeat']), 'objectID':self.objectID}]
        for processor in self.dataProcessor:
            processResult['device'] = processor.Process(parserRawDataResult['device'])
            processResult['err'] = processor.Process(parserRawDataResult['err'])
        
        for dataType in packetParserResult.keys():
            packetParser = FP.PacketFactory(dataType, self.readInfo, processResult[dataType], processTime, backupObject, self.settingInfo['err'])
            packetParserResult[dataType] = packetParser.Process()
        
        if self.readInfo['oldFlag']:
            oldParser = HOP.Handle(packetParserResult)
            packetParserResult['device'] = oldParser.Process()
            
        self.logger.info("Finish Parser Data")
        return packetParserResult, startTime
    
    def ParserRawData(self, rawData:TotalModbusData, dataType):
        try:
            resultObject = {}
            resultObject['inv'] = self.ParserDevice(rawData.inv, dataType, 'inv')
            resultObject['sp'] = self.ParserDevice(rawData.sp, dataType, 'sp')
            resultObject['irr'] = self.ParserDevice(rawData.irr, dataType, 'irr')
            resultObject['temp'] = self.ParserDevice(rawData.temp, dataType, 'temp')
            return resultObject
        except Exception as ex:
            self.logger.warning("ParserRawData, ex: {0} | ".format(ex), exc_info=True)
            HL.SystemExceptionInfo()
    
    def ParserDevice(self, rawData:list[ModbusData], dataType, deviceType):
        result = []
        if len(rawData) != 0:
            deviceInfo = self.readInfo[dataType][deviceType]
            for count in range(len(deviceInfo)):
                self.label = deviceInfo[count]['label']
                self.strID = self.deviceID[deviceType] + str(count).zfill(3)
                parserInfo = self.settingInfo[dataType][deviceType][self.label]['Parse']
                if len(rawData) == len(deviceInfo):
                    if rawData[count].data != [None]:
                        result.append(self.ParserModbusData(rawData[count], parserInfo, deviceType, dataType))
                    else:
                        result.append({})
                else:
                    result.append({})
            return self.ConvertToObject(result, rawData, deviceInfo)
        else:
            return None

    def ParserModbusData(self, rawData:ModbusData, parserInfo, deviceType, dataType):
        try:
            resultDict = {"objectID":self.objectID}
            for info in parserInfo:
                if rawData.data[info["StartSite"]] != None:
                    parser = FP.ParserFactory(rawData.data, info, dataType, deviceType, self.label)
                    resultDict[info['Field']] = parser.Process()
            filter = HC.CalculatorFactory(dataType, self.label, self.strID, self.logger)
            resultDict = filter.Process(resultDict)
            return resultDict
        except Exception as ex:
            self.logger.warning("ParserModbusData, ex: {0} | ".format(ex), exc_info=True)
            HL.SystemExceptionInfo()

    def ConvertToObject(self, parserResult,rawData:list[ModbusData], deviceInfo):
        try:
            result = {}
            count = 0
            for i in deviceInfo:
                if parserResult[count] != {}:
                    parserResult[count]['deviceID'] = i['devID']
                    parserResult[count]['flag'] = i['flag']
                    parserResult[count]['time'] = rawData[count].readTime
                    result[i['flag']] = parserResult[count]
                else:
                    result[i['flag']] = None
                count += 1
            return result
        except Exception as ex:
            self.logger.warning("ParserProcessor_ResultToJson, ex: {0} | ".format(ex), exc_info=True)
            HL.SystemExceptionInfo()
    
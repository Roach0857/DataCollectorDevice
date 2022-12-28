import datetime
import time
from copy import deepcopy
from logging import Logger
from queue import Queue

import Factory.Read as FR
import Handler.Logger as HL
import Handler.Parser as HP
from Entity.ModbusData import ModbusData
from Entity.Period import Period
from Entity.RawData import RawData
from Entity.TotalModbusData import TotalModbusData
from Handler.Mutual import BackupObject


class Handle():
    def __init__(self, settingInfo:dict, readInfo:dict, period:Period, parserData:Queue, backupObject:BackupObject, logger:Logger):
        self.settingInfo = settingInfo
        self.readInfo = readInfo
        self.period = period
        self.parserData = parserData
        self.systemFlag = True
        self.backupObject = backupObject
        self.logger = logger
        self.parserHandler = HP.Handle(settingInfo, readInfo, logger)
        self.deviceNumber = self.GetDeviceNumber()
        self.strSerial = readInfo['serial']
        self.rawData = RawData()
        self.rawDataObject = {}
        self.timeout = datetime.datetime.now()
        
    def ReadDevice(self, dataType, deviceType):
        deviceContent = self.rawDataObject[dataType].__dict__[deviceType]
        result = [ModbusData() for i in range(len(deviceContent))]
        try:
            if deviceContent != []:
                deviceInfoList = self.readInfo[dataType][deviceType]
                deviceSettingInfo = self.settingInfo[dataType][deviceType]
                for index in range(len(deviceContent)):
                    label = deviceInfoList[index]['label']
                    readSetting = deviceSettingInfo[label]['Read']
                    modbusReader = FR.ReaderFactory(self.strSerial, dataType, readSetting, deviceInfoList[index], deviceType, label, self.logger)
                    if deviceContent[index].CheckData():
                        result[index].data = deviceContent[index].data
                        result[index].readTime = deviceContent[index].readTime
                        self.logger.debug("\n{0} {1} {2} already has data".format(dataType, deviceType, deviceInfoList[index]['macaddress']))
                    else:
                        modbusResult = modbusReader.read()
                        result[index].data = modbusResult 
                        result[index].readTime = int(time.mktime(datetime.datetime.now().timetuple()))
                        self.logger.info("\n{0} {1} {2} {3}".format(dataType, deviceType, deviceInfoList[index]['macaddress'], modbusResult))
                    if self.TimeoutAction():
                        return False
            else:
                self.logger.info("{0} No Device Need To Read".format(deviceType))
            return True
        except Exception as ex:
            self.logger.error(f"ReadProcessor_ReadDevice, ex: {ex} | {HL.SystemExceptionInfo()}")
        finally:
            for index in range(len(result)):
                if result[index].data != [None]:
                    deviceContent[index] = result[index]
            
    def TimeoutAction(self):
        if datetime.datetime.now() >= self.timeout:
            return True
        return False
    
    def Process(self):
        try:
            self.rawDataObject = {'device':TotalModbusData(), 'err':TotalModbusData()}
            self.MakeContainer()
            while(True):
                for dataType, dataContain in self.rawDataObject.items():
                    for deviceType in dataContain.__dict__.keys():
                        if not self.ReadDevice(dataType, deviceType):
                            return False
                if self.CheckFullData() :
                    return True
        except Exception as ex:
            self.logger.error(f"ReadProcessor_Process, ex: {ex} | {HL.SystemExceptionInfo()}")

    def CheckFullData(self):
        flag = False
        totalNumber = 0
        for dRvalue in self.rawDataObject['device'].__dict__.values():
            for v in dRvalue:
                if v.CheckData():
                    totalNumber += 1
        if(totalNumber >= self.deviceNumber):
            flag = True
        return flag
    
    def GetDeviceNumber(self):
        Result = 0
        for dRI in self.readInfo['device'].values():
            Result += len(dRI)
        return Result

    def MergeOldData(self, oldData:dict[str,TotalModbusData]) -> dict[str,TotalModbusData]:
        result = deepcopy(self.rawDataObject)
        if oldData != None:
            for dataType, dataContent in self.rawDataObject.items():
                for deviceType, deviceContent in dataContent.__dict__.items():
                    index = 0
                    for content in deviceContent:
                        if oldData[dataType].__dict__[deviceType][index].data != [None]:
                            if content == [None]:
                                result[dataType].__dict__[deviceType][index] = oldData[dataType].__dict__[deviceType][index]
                            elif dataType == 'err' and content == [0,0]:
                                result[dataType].__dict__[deviceType][index] = oldData[dataType].__dict__[deviceType][index]
                        index += 1
        return result

    def MakeContainer(self):
        for dataType in self.rawDataObject.keys():
            for deviceType, infoList in self.readInfo[dataType].items():
                for item in range(len(infoList)):
                    self.rawDataObject[dataType].__dict__[deviceType].append(ModbusData())
    
    def DoRead(self):
        try:
            if self.systemFlag:
                self.logger.info("Start Read Data")
                startTime = datetime.datetime.now()
                self.timeout = startTime + datetime.timedelta(seconds=(self.period.read - 5))
                resultFlag = self.Process()
                self.logger.info("Finish Read Data")
                if resultFlag:
                    self.logger.info("Read All Device Complete")
                else:
                    self.logger.info("Read Device Timeout")
                self.rawData.currentData = self.rawDataObject
                if self.rawData.Any():
                    self.rawData.mergeData = self.MergeOldData(self.rawData.mergeData)
                else:
                    self.rawData.mergeData = self.rawDataObject
                self.parserData.put(self.parserHandler.DoParser(self.rawData, startTime, self.backupObject)) 
                if startTime.minute % self.period.device == 0:
                    self.rawData.mergeData = None
            else:
                self.logger.info("Mutual Flag:{}, Do Not Read".format(self.systemFlag))
        except Exception as ex:
            self.logger.warning(f"DoRead, ex: {ex} | {HL.SystemExceptionInfo()}")

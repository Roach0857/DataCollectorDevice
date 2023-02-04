import datetime
import json
import threading as th
from copy import deepcopy
from queue import Queue

from py_linq.py_linq import Enumerable
from Entity.SendData import SendData

import Handler.AwsIot as HA
import Handler.Logger as HL
import Handler.Mutual as HM
import Handler.Send as HS
from Entity.Period import Period


class Handle(HS.Handle):
    def __init__(self, config, settingInfo, readInfo, socketData: Queue, kinesisData: Queue, period:Period, logger):
        super().__init__(config, settingInfo, readInfo, socketData, kinesisData, logger)
        self.settingInfo = settingInfo
        self.readInfo = readInfo
        self.period = period
        self.logger = logger
        self.shadowHandler = HA.Handle(readInfo, settingInfo, logger)
        self.mutualHandler = HM.MutualFactory(config, readInfo, self.shadowHandler, self.SendPacket, logger)
        self.backupObject = self.mutualHandler.backupObject
        self.parserData = Queue()
        self.rawErrData = None
        self.systemFlag = True
        self.loadFalg = False
        if self.readInfo['awsIot']['thingName'].split("-")[1] == "load":
            self.loadFalg = True
        self.extraSelector = {"oqc":PacketProcessor(self.PostToApi)}
        sendKinesisJob = th.Thread(target=self.DoSend, args=('KinesisDataPath',))
        sendKinesisJob.start()
        
    def DoSelect(self):
        data = None
        self.logger.info("Start Select")
        try:
            if not self.parserData.empty():
                data = self.parserData.get()
                self.logger.info(f"Parser Data -> Device Data:{data[0]['device']}")
                self.logger.info(f"Parser Data -> Err Data:{data[0]['err']}")
                if self.systemFlag:
                    self.SelectData(data)
                else:
                    self.logger.info("Mutual Flag:{}, Do Not Send Data".format(self.systemFlag))
                    data = None
            else:
                self.logger.info("No Data, System Rest")
        except Exception as ex:
            self.logger.warning("DoSelect, ex: {0} | ".format(ex), exc_info=True)
            HL.SystemExceptionInfo()
                            
    def SelectData(self, dataTuple:tuple[dict,datetime.datetime]):
        self.rawErrData = dataTuple[0]["err"]
        for dataType, dataList in dataTuple[0].items():
            if self.period.__dict__[dataType] != 0:
                if self.readInfo['projectID'] == 'L8168' and dataTuple[1].minute % 3 == 0:
                    if dataType == 'device':
                        self.KinesisDataProcess(dataType, dataList)
                    elif dataType == 'heartbeat':
                        shadowRequest = self.shadowHandler.MakeDeviceHeartbeat(dataList)
                        self.shadowHandler.Update(shadowRequest)
                elif dataTuple[1].minute % self.period.__dict__[dataType] == 0:
                    if self.loadFalg:
                        if dataType == 'device':
                            self.KinesisDataProcess(dataType, dataList)
                        elif dataType == 'heartbeat':
                            shadowRequest = self.shadowHandler.MakeDeviceHeartbeat(dataList)
                            self.shadowHandler.Update(shadowRequest)
                    else:
                        if dataType == 'device':
                            if (20 > dataTuple[1].hour >= 5):
                                self.KinesisDataProcess(dataType, dataList)
                        elif dataType == 'heartbeat':
                            shadowRequest = self.shadowHandler.MakeDeviceHeartbeat(dataList)
                            self.shadowHandler.Update(shadowRequest)
                        elif dataType == 'err':
                            self.KinesisDataProcess(dataType, dataList)
                            shadowRequest = self.shadowHandler.MakeDeviceErrorCode(dataList)
                            self.shadowHandler.Update(shadowRequest)
                            
    def KinesisDataProcess(self, dataType, dataList):
        contentType = dataType
        if dataType == 'device':
            contentType = 'data'
        for data in dataList:
            result = []
            if contentType == 'data':
                dataObject = self.GetKinesisDataErr(data)
            else:
                dataObject = data
            for deviceType, deviceData in dataObject['content']['object'][contentType].items():
                if len(deviceData) != 0:
                    kinesisData = self.GetKinesisData(contentType, deviceType, deviceData)
                    if contentType == 'data' and not self.loadFalg:
                        kinesisData.append(self.GetKinesisDataTotal(kinesisData, deviceType, dataObject['content']['object']['objectID']))
                    result.extend(kinesisData)
            
            if len(result) > 0:
                if self.sendFlag:
                    self.WriteKinesisDataFile(result, contentType, dataObject)
                else:
                    if not self.SendKinesis(result):
                        self.WriteKinesisDataFile(result, contentType, dataObject)
                        sendKinesisJob = th.Thread(target=self.DoSend, args=('KinesisDataPath',))
                        sendKinesisJob.start()
                    
    def WriteKinesisDataFile(self, data, contentType, dataObject):
        strTime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        strData = json.dumps(data)
        fileName = f"{contentType}_{dataObject['content']['object']['objectID']}_{strTime}"
        with open(f"{self.settingInfo['KinesisDataPath']}{fileName}.txt", 'w') as f:
            f.write(strData)
            f.close
        self.kinesisData.put(SendData(strData, f"{self.settingInfo['KinesisDataPath']}{fileName}.txt"))
        self.logger.info(f"Kinesis Data -> FileName:{fileName}, Data:{strData}")
        
    def GetKinesisData(self, contentType, deviceType, deviceData:list[dict]):
        result = []
        for data in deviceData:
            if contentType == 'err':
                resultData = {"type":contentType}
            else:
                if deviceType == 'sp':
                    if self.loadFalg:
                        resultData = {"type":'load'}
                    else:
                        resultData = {"type":'dm'}
                else:
                    resultData = {"type":deviceType}
            for key, value in data.items():
                if key == "flag":
                    continue
                if key == 'errCode':
                    value = hex(value)[2:].zfill(4).upper()
                resultData[key] = value
            result.append(resultData)
        if deviceType == 'err':
            return list(filter(lambda x:x['errCode'] != 0, result))
        return result
    
    def GetKinesisDataTotal(self, kinesisData, deviceType, objectID):
        if deviceType == 'sp':
            result = {"type":"dm-sum",
                  "objectID":f"{objectID}"}
        else:
            result = {"type":f"{deviceType}-sum",
                    "objectID":f"{objectID}"}
        for data in kinesisData:
            flag = list(filter(lambda x:x['devID'] == data['deviceID'], self.readInfo['device'][deviceType]))[0]['flag']
            for key, value in data.items():
                if key == "type":
                    continue
                elif key == "deviceID":
                    continue
                elif key == "objectID":
                    continue
                elif key == "time":
                    if not "time" in result:
                        result[key] = value
                        continue
                    else:
                        continue
                elif key == "status":
                    continue
                if 'err' in key:
                    errData = f"{objectID}_01{str(flag).zfill(3)}:{value}"
                    if key in result:
                        result[key] += f"-{errData}"
                    else:
                        result[key] = errData
                elif key in result:
                    result[key] += value
                else:
                    result[key] = value
        return result
        
    def GetKinesisDataErr(self, data):
        result = deepcopy(data)
        if len(self.rawErrData) != 0:
            projectErr = Enumerable(self.rawErrData).select(
                        lambda x:x['content']['object']).where(
                            lambda x:x['objectID'] == data['content']['object']['objectID'])
            if len(projectErr) != 0:
                for deviceType, deviceData in result['content']['object']['data'].items():
                    for dataContant in deviceData:
                        if projectErr[0] != None:
                            if deviceType in projectErr[0]['err']:
                                deviceErr = Enumerable(projectErr[0]['err'][deviceType]).where(lambda x:x['deviceID'] == dataContant['deviceID']).to_list()
                                for err in deviceErr:
                                    if err["errCode"] != '0000':
                                        if err["errID"] == "1":
                                            dataContant["err1"] = err["errCode"]
                                        elif err["errID"] == "2":
                                            dataContant["err2"] = err["errCode"]
        return result
    
    def SocketDataProcess(self, dataType, dataList):
        for data in dataList:
            if self.SocketDataCheck(dataType, data):
                strTime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                strData = ''
                result = None
                if data['info']['type'] == "data":
                    if type(data['content']) == str:
                        fileName = '{0}_{1}_{2}'.format(data['info']['type'], data['content'][:5], strTime)
                        result = data
                    else:
                        result = self.GetSocketDataErr(data)
                        fileName = '{0}_{1}_{2}'.format(result['info']['type'], result['content']['object']['objectID'], strTime)
                elif data['info']['type'] == "nodeHB":
                    fileName = '{0}_{1}'.format(data['info']['type'], strTime)
                    result = data
                else:
                    fileName = '{0}_{1}_{2}'.format(data['info']['type'], data['content']['object']['objectID'], strTime)
                    result = data
                if not self.SendSocket(result):
                    strData = json.dumps(result)
                    with open(f"{self.settingInfo['LostDataPath']}{fileName}.txt", 'w') as f:
                        f.write(strData)
                        f.close
                    self.socketData.put(SendData(strData, f"{self.settingInfo['LostDataPath']}{fileName}.txt"))
                    self.logger.info(f"Socket Data -> FileName:{fileName}, Data:{strData}")
    
    def SocketDataCheck(self, dataType, data):
        contentType = dataType
        if dataType == 'device':
            contentType = 'data'
        for deviceData in data['content']['object'][contentType].values():
            if len(deviceData) != 0:
                return True
        return False
    
    def GetSocketDataErr(self, data):
        result = deepcopy(data)
        if len(self.rawErrData) != 0:
            projectErr = Enumerable(self.rawErrData).select(
                        lambda x:x['content']['object']).where(
                            lambda x:x['objectID'] == data['content']['object']['objectID'])
            if len(projectErr) != 0:
                for deviceType, deviceData in result['content']['object']['data'].items():
                    for dataContant in deviceData:
                        if projectErr[0] != None:
                            if deviceType in projectErr[0]['err']:
                                deviceErr = Enumerable(projectErr[0]['err'][deviceType]).where(lambda x:x['deviceID'] == dataContant['deviceID']).to_list()
                                dataContant["err"] = self.MakeSocketDataErr(deviceErr)
        return result
    
    def MakeSocketDataErr(self, deviceErr):
        result = []
        for err in deviceErr:
            errResult = {}
            errResult["errID"] = err["errID"]
            errResult["errCode"] = err["errCode"]
            result.append(errResult)
        return result
    
class PacketProcessor():
    def __init__(self, function):
        self.function = function
        self.flag = False

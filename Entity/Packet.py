import json
import datetime
import time
from py_linq import Enumerable

class Period():
    def __init__(self, read, device, err, heartbeat, node, oqc):
        self.read = read
        self.device = device
        self.err = err
        self.heartbeat = heartbeat
        self.node = node
        self.oqc = oqc

class Packet():
    def __init__(self, settingInfo, readInfo, period, oqcFunction, logger):
        self.settingInfo = settingInfo
        self.readInfo = readInfo
        self.period = period
        self.extraSelector = {"oqc":PacketProcessor(oqcFunction)}
        self.logger = logger
        self.rawData = None
        self.currentData = None
        self.rawErrData = None
        self.sendFlag = True
        self.readTime = None

    def CatchTime(self):
        result = None
        self.logger.info("Start Catch Time")
        while(True):
            result = datetime.datetime.now()
            if result.second == 0:
                break
            time.sleep(0.1)
        self.logger.info("readTime:{0}".format(result.strftime("%Y-%m-%d %H:%M:%S")))
        self.readTime = result
        return result

    def Select(self, parserData):
        self.logger.info("Start Select")
        self.rawErrData = parserData['err']
        self.SelectData("device", parserData['device'])
        self.SelectData("err", parserData['err'])
        self.SelectData("heartbeat", parserData['heartbeat'])
        self.SelectData("oqc", parserData['oqc'])
        self.SelectData("node", parserData['node'])
                    
    def SelectData(self, dataType, dataList):
        if self.period.__dict__[dataType] != 0:
            if self.readTime.minute % self.period.__dict__[dataType] == 0:
                if dataType in self.extraSelector:
                    if self.extraSelector[dataType].flag:
                        self.extraSelector[dataType].function(dataList)
                else:
                    if dataType == 'device':
                        if (20 > self.readTime.hour >= 5):
                            self.Write(dataType, dataList)
                            self.rawData = None
                    else:
                        self.Write(dataType, dataList)
                        
    def Write(self, dataType, dataList):
        self.logger.info("Write {0} Packet".format(dataType))
        for data in dataList:
            strTime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            if data['info']['type'] == "data":
                if self.readInfo['oldFlag']:
                    fileName = '{0}_{1}_{2}'.format(data['info']['type'], data['content'][:5], strTime)
                else:
                    data = self.MergeErr(data)
                    fileName = '{0}_{1}_{2}'.format(data['info']['type'], data['content']['project']['projectID'], strTime)
            elif data['info']['type'] == "nodeHB":
                fileName = '{0}_{1}'.format(data['info']['type'], strTime)
            else:
                fileName = '{0}_{1}_{2}'.format(data['info']['type'], data['content']['project']['projectID'], strTime)
            strData = json.dumps(data)
            with open(self.settingInfo['LostDataPath'] + "{0}.txt".format(fileName), 'w') as f:
                f.write(strData)
                f.close
            self.logger.info("Packet Written")
        self.logger.info("End Write Packet")
        
    def MergeErr(self, data):
        if len(self.rawErrData) != 0:
            projectErr = Enumerable(self.rawErrData).select(
                        lambda x:x['content']['project']).where(
                            lambda x:x['projectID'] == data['content']['project']['projectID']).first()
            for deviceType, deviceData in data['content']['project']['data'].items():
                for dataContant in deviceData:
                    if projectErr != None:
                        if deviceType in projectErr['err']:
                            deviceErr = Enumerable(projectErr['err'][deviceType]).where(lambda x:x['devID'] == dataContant['devID']).to_list()
                            dataContant["err"] = self.MakeErr(deviceErr)
        return data
    
    def MakeErr(self, deviceErr):
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

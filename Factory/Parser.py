import datetime
import struct
import uuid
from copy import deepcopy

import Handler.Logger as HL


class Packet():
    def __init__(self, readInfo, rawData, processTime):
        self.readInfo = readInfo
        if readInfo['nodeModel'] == 'single':
            self.nodeID = self.readInfo['nodeID']['master']
        else:
            self.nodeID = self.readInfo['nodeID'][self.readInfo['nodeModel']]
        self.rawData = rawData
        self.processTime = processTime
        self.deviceID = {"inv" :'01',"sp" :'00',"irr" :'04',"temp" :'05'}

    def Major(self, typeData):
        result = {}
        packetData = deepcopy(self.rawData)
        for count in range(len(self.rawData)):
            dataTag = []
            for tag in self.rawData[count].keys():
                if tag == 'objectID':
                    continue
                dataTag.append(tag)

            objectID = packetData[count]['objectID']
            for tag in dataTag:
                for typeDevice, content in self.rawData[count][tag].items():
                    contentList = self.Handle(content)
                    del packetData[count][tag][typeDevice]
                    if contentList != []:
                        packetData[count][tag][typeDevice] = contentList
                    else:
                        packetData[count][tag][typeDevice] = []
                if packetData[count][tag] != {}:
                    if objectID in result:
                        result[objectID][tag] = packetData[count][tag]
                    else:
                        result[objectID] = {
                            tag:packetData[count][tag], 
                            'objectID':objectID}
            
            if typeData != 'data' and result != {}:
                result[objectID][typeData] = result[objectID].pop('data')
            
        return result

    def Handle(self, content):
        contentList = []
        if content != None:
            for data in content.values():
                if data != None:
                    contentList.append(data)
        return contentList

    def Process(self):
        data = self.Major('data')
        result = self.ObjectPackets(data, 'data')
        return result

    def ObjectPackets(self, data, dataType):
        result = []
        if data != {}:
            for value in data.values():
                packetInfo = {
                    "id":str(uuid.uuid4()),
                    "type":dataType, 
                    "isResend": False,
                    "route":[{"nodeID":self.nodeID,
                    "stamp":self.processTime}]}
                packetContent = {
                    "object":None
                }
                resultPacket = {"info":packetInfo, "content":packetContent}
                resultPacket['content']['object'] = value
                result.append(resultPacket)
        return result

class Heartbeat(Packet):
    def Handle(self, content):
        contentList = []
        if content != None:
            for data in content.values():
                if data != None:
                    dataContent = {
                        "stamp":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "deviceID":data['deviceID'],
                        }
                    contentList.append(dataContent)
        return contentList

    def Process(self):
        data = self.Major('deviceHB')
        result = self.ObjectPackets(data, 'deviceHB')
        return result

class Error(Packet):
    def Handle(self, content):
        contentList = []
        if content != None:
            for data in content.values():
                if data != None:
                    for field, errCode in data.items():
                        if (field[:3] == "err" and (errCode != '')):
                            dataContent = {
                                "objectID":data['objectID'],
                                "deviceID":data['deviceID'], 
                                "time":data['time'],
                                "errID":field[3:],
                                "errCode":errCode}
                            contentList.append(dataContent)
        return contentList

    def Process(self):
        data = self.Major('err')
        result = self.ObjectPackets(data, 'err')
        return result

class OQC(Packet):
    def Process(self):
        oqcObject = {}
        oqcData = {'INV':[], 'SP':[], 'IRR':[], 'TEMP':[] }
        for dKey,dValue in self.rawData['data'].items():
            oqcData[dKey.upper()] = []
            if dValue != None:
                for key,value in dValue.items():
                    if value != None:
                        self.rawData = {}
                        self.rawData['dev_id'] = str(key)
                        self.rawData['is_enable'] = True
                        self.rawData['dev_type'] = dKey.upper()
                        oqcData[dKey.upper()].append(self.rawData)
                    else:
                        self.rawData = {}
                        self.rawData['dev_id'] = str(key)
                        self.rawData['is_enable'] = False
                        self.rawData['dev_type'] = dKey.upper()
                        oqcData[dKey.upper()].append(self.rawData)
        oqcObject['data'] = oqcData
        oqcObject['data_collect_time'] = self.processTime
        return oqcObject

class Node():
    def __init__(self, backupObject, processTime):
        self.backupObject = backupObject
        self.processTime = processTime
        self.resultList = []

    def Process(self):
        self.MakeHeartbeat()
        self.MakeErr()
        return self.resultList
    
    def GeneratePacket(self, packetType):
        result = {"info":None, "content":None}
        resultInfo = {
            "id":str(uuid.uuid4()),
            "type":packetType, 
            "isResend": False,
            "route":[{"nodeID":self.backupObject.nodeID.me,
            "stamp":self.processTime}]}
        result['info'] = resultInfo
        return result

    def MakeHeartbeat(self):
        result = self.GeneratePacket("nodeHB")
        resultContent = {
            "node":{
                "nodeHB":[{
                    "nodeID":self.backupObject.nodeID.me,
                    "stamp":self.processTime}]}}
        if self.backupObject.gpioInfo != None:
            if self.backupObject.gpioInfo.yourWorkPin.Get():
                oppositeHeartbeat = {
                    "nodeID":self.backupObject.nodeID.you,
                    "stamp":self.processTime}
                resultContent["node"]['nodeHB'].append(oppositeHeartbeat)
        result['content'] = resultContent
        self.resultList.append(result)
    
    def MakeErr(self):
        if len(self.backupObject.nodeErr.bus) != 0:
            result = self.GeneratePacket("nodeErr")
            nodeContent = {"node":{"nodeErr":[]}}
            for errContent in self.backupObject.nodeErr.bus.values():
                nodeContent['node']['nodeErr'].append(errContent)
            result['content'] = nodeContent
            self.resultList.append(result)
            self.backupObject.nodeErr.bus.clear()

def PacketFactory(dataType, readInfo, rawData, processTime, backupObject):
    factory = {'device':Packet,'heartbeat':Heartbeat,'oqc':OQC,'err':Error, 'node':Node}
    if dataType == 'node':
        result = factory[dataType](backupObject, processTime)
    else:
        result = factory[dataType](readInfo, rawData, processTime)
    return AbsPacketInterface(result)

class AbsPacketInterface():
    def __init__(self, packet = None):
        self.packet = packet
    
    def Process(self):
        return self.packet.Process()
    
class ParserInv():
    def __init__(self, data, parserInfo, dWordFlag = True):
        self.data = data
        self.Field = parserInfo['Field']
        self.StartSite = parserInfo['StartSite']
        self.Lengthght = parserInfo['Length']
        self.Rate = parserInfo['Rate']
        self.dWordFlag = dWordFlag
        
    def Process(self):
        value = 0
        result = None
        if self.Lengthght == 2:
            if self.dWordFlag:
                highWord = self.data[self.StartSite]
                lowWord = self.data[self.StartSite + 1]
            else:
                highWord = self.data[self.StartSite + 1]
                lowWord = self.data[self.StartSite]
            if  highWord > 0 :
                value = (highWord * 65536) + lowWord
            else:
                value = lowWord
        else:
            value = self.data[self.StartSite]
        result = self.CheckResult(value)
        return result

    def ConvertToBin(self, data, flag = False):
        result = ''
        binString = bin(int(data))[2:].zfill(16)
        if flag:
            if binString[0] == '1':
                for binChar in binString[1:]:
                    if binChar == '1':
                        result += '0'
                    else:
                        result += '1'
                return result, False
        return binString, True
    
    def CheckResult(self, value):
        result = None
        if self.Field != "status":
            if type(value) is str :
                resultValue = self.GetFloatString(value)
                if resultValue != None:
                    result = float(resultValue) / self.Rate
            else:
                result = value / self.Rate
        else:
            if type(value) is str :
                result =  str(int(value))
            else:
                result =  str(value)
        return result
    
    def GetFloatString(self, value):
        result = ''
        flag = True
        for v in value:
            if v == '.':
                if flag:
                    flag = False
                else:
                    return None
            elif v not in ('1','2','3','4','5','6','7','8','9','0'):
                continue
            result += v
        if result[-1] == '.':
            return None
        else:
            return result
    
class ParserInvSolaredge(ParserInv):
    def Process(self):
        result = None
        resultString = ''
        value = 0
        if self.Field[-2:] == "SF":
            resultString, flag = self.ConvertToBin(self.data[self.StartSite], True)
        else:
            if self.Lengthght == 2:
                resultStringHigh, flag = self.ConvertToBin(self.data[self.StartSite]) 
                resultStringLow, flag = self.ConvertToBin(self.data[self.StartSite + 1]) 
                resultString = resultStringHigh + resultStringLow
            else:
                resultString, flag = self.ConvertToBin(self.data[self.StartSite])
        if flag:
            value = int(resultString, base = 2)
        else:
            value = 0 - (int(resultString, base = 2) + 1)
        result = self.CheckResult(value)
        return result
    
class ParserInvFronius(ParserInv):
    def Process(self):
        result = None
        resultString = ''
        value = 0
        if self.Lengthght == 2:
            resultStringHigh, flag = self.ConvertToBin(self.data[self.StartSite]) 
            resultStringLow, flag = self.ConvertToBin(self.data[self.StartSite + 1]) 
            resultString = resultStringHigh + resultStringLow
        else:
            resultString, flag = self.ConvertToBin(self.data[self.StartSite])
        if flag:
            value = int(resultString, base = 2)
        else:
            value = 0 - (int(resultString, base = 2) + 1)
        result = self.CheckResult(value)
        return result
    
class ParserTemp(ParserInv):
    def Process(self):
        value = self.data[self.StartSite]
        result = None
        resultString, flag = self.ConvertToBin(self.data[self.StartSite], True)
        if not flag:
            value = 0 - (int(resultString, 2) + 1)
        result = value / self.Rate
        return result

class ParserIrr(ParserInv):
    def Process(self):
        result = None
        if (self.data[self.StartSite] > 32768):
            value = self.data[self.StartSite] - 65536
        else:
            value = self.data[self.StartSite]
        result = value / self.Rate
        return result

class ParserSp(ParserInv):
    def Process(self):
        result = None
        value = 0.0
        dataString = ''
        try:   
            for count in range(self.Lengthght):
                dataString = ('{:04x}'.format(self.data[self.StartSite + count]) + dataString)
            value = round(struct.unpack('!f', bytes.fromhex(dataString))[0],4)
        except Exception as ex:
            print(f"ParserSpData_Basic, ex: {ex} | {HL.SystemExceptionInfo()}")
        result = value / self.Rate
        return result
       
class ParserErrInv(ParserInv):
    def Process(self):
        result = ""
        if self.data[self.StartSite] != 0:
            result = str(self.data[self.StartSite])
        return result

class ParserDM2436AB(ParserInv):
    def Process(self):
        result = 0
        for i in range(self.Lengthght):
            result += self.data[i + self.StartSite] * (65536 ** (3 - i))
        return result / self.Rate

    
def ParserFactory(data, parserInfo, dataType, deviceType, label):
    factory = {'device':
                    {'inv':{'Basic':ParserInv, 'solaredge':ParserInvSolaredge, 'fornius':ParserInvFronius}, 
                     'temp':{'Basic':ParserTemp}, 
                     'irr':{'Basic':ParserIrr}, 
                     'sp':{'Basic':ParserSp, "DM2436AB":ParserDM2436AB}},
               'err':
                    {'inv':{'Basic':ParserErrInv}}}
    if label in factory[dataType][deviceType]:
        result = factory[dataType][deviceType][label](data, parserInfo)
    else:
        if dataType != 'device':
            result = factory[dataType][deviceType]['Basic'](data, parserInfo)
        else:
            if label == 'delta' or label == 'cyberpower':
                result = factory[dataType][deviceType]['Basic'](data, parserInfo, False)
            else:
                result = factory[dataType][deviceType]['Basic'](data, parserInfo)
    return AbsParserInterface(result)

class AbsParserInterface():
    def __init__(self, parser = None):
        self.parser = parser
    
    def Process(self):
        return self.parser.Process()
    
from py_linq import Enumerable


class Handle():
    def __init__(self, rawData):
        self.rawDeviceData = rawData['device']
        self.rawErrData = rawData['err']
        self.deviceID = {"inv" :'01',"sp" :'00',"irr" :'04',"temp" :'05'}
        self.sumErrData = {'1':'','2':''}
        
    def Process(self):
        result = []
        for content in self.rawDeviceData:
            resultContent = {}
            resultContent['info'] = content['info']
            resultContent['content'] = self.ConvertToOldData(content['content'])
            result.append(resultContent)
        return result
    
    def ConvertToOldData(self, rwaData):
        result = '{0}\n'.format(rwaData['project']['projectID'])
        resultData = ''
        projectErr = None
        if len(self.rawErrData) > 0:
            projectErr = Enumerable(self.rawErrData).select(
                    lambda x:x['content']['project']).where(
                        lambda x:x['projectID'] == rwaData['project']['projectID']).first()
        for deviceType, deviceData in rwaData['project']['data'].items():
            sumData = {}
            for dataContant in deviceData:
                stringDevID = self.SetDevId(deviceType, len(deviceData), dataContant['flag'])
                stringData = self.MakeOldData(deviceType,stringDevID, dataContant, projectErr)
                sumData = self.MakeSumData(dataContant, sumData)
                resultData += '{0}\n'.format(stringData[:-1])
            if len(deviceData) > 1:
                sumDevID = '{0}'.format(self.SetDevId(deviceType, len(deviceData), 0))
                stringSumData = self.MakeOldData(deviceType, sumDevID, sumData)
                for devID, errCode in self.sumErrData.items():
                    if errCode != '':
                        stringSumData += "err{0}='{1}',".format(devID,errCode[:-1])
                resultData += '{0}\n'.format(stringSumData[:-1])
        result += resultData[:-1]
        return result
    
    def MakeSumData(self, dataContant, sumData):
        for field, value in dataContant.items():
            if field in sumData:
                sumData[field] += value
            else:
                sumData[field] = value
        return sumData
    
    def SetDevId(self, deviceType, deviceLenght, deviceFlag):
        result = ''
        if deviceType == 'inv':
            result = '{0}'.format(self.deviceID[deviceType] + str(deviceFlag).zfill(3))
        else:
            if deviceLenght > 1:
                result = '{0}'.format(self.deviceID[deviceType] + str(deviceFlag))
            else:
                result = '{0}'.format(self.deviceID[deviceType] + str(deviceFlag - 1))
        return result
    
    def MakeOldData(self, deviceType, stringDevID, dataContant, projectErr = None):
        result = '{0},'.format(stringDevID)
        for field, value in dataContant.items():
            if field[:2] == 'dc':
                result += 'i_{0}={1},'.format(field[-1].lower(), str(value))
            elif field[:2] == 'ac':
                result += 'o_{0}={1},'.format(field[-1].lower(), str(value))
            elif field == 'acActiveEnergy':
                result += 'ed_kwh={0},'.format(str(value))
            elif field == 'acActiveEnergyDaily':
                result += 'tot_kwh={0},'.format(str(value))
            elif field == 'irradiance':
                result += 'irr={0},'.format(str(value))
            elif field == 'temperature':
                result += 'temp={0},'.format(str(value))
        if projectErr != None:
            if deviceType in projectErr['err']:
                deviceErr = Enumerable(projectErr['err'][deviceType]).where(lambda x:x['devID'] == dataContant['devID']).to_list()
                stringErr = self.MakeErrData(stringDevID, deviceErr)
                result +=stringErr
        return result
    
    def MakeErrData(self, stringDevID, deviceErr):
        result = ''
        for errItem in deviceErr:
            if errItem['errID'] == '1':
                result += 'err1={1},'.format(errItem['errID'], errItem['errCode']) 
                self.sumErrData['1'] += '{0}:{1}-'.format(stringDevID, errItem['errCode'])
            elif errItem['errID'] == '2':
                result += 'err2={1},'.format(errItem['errID'], errItem['errCode']) 
                self.sumErrData['2'] += '{0}:{1}-'.format(stringDevID, errItem['errCode'])
        return result
    
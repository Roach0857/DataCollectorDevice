import datetime
import math
import os
import shelve
from copy import deepcopy


def CalculatorFactory(dataType, label, strID, logger):
    factory = {'solaredge':CalculatorSolaredgeData, 'kaco':CalculatorKacoData, 'prime':CalculatorPrimeData, 'fronius':CalculatorFroniusData, 'cyberpower':CalculatorPrimeData}
    if dataType == "device":
        if label in factory:
            result = factory[label](strID, logger)
        elif 'solaredge' in label:
            result = factory['solaredge'](strID, logger)
        elif 'kaco' in label:
            result = factory['kaco'](strID, logger)
        else:
            result = CalculatorData(strID, logger)
    else:
        result = CalculatorData(strID, logger)
    return AbsCalculatorInterface(result)

class CalculatorData():
    def __init__(self, strID, logger):
        self.logger = logger
        self.strID = strID
        
    def Process(self, data):
        return data
    
class CalculatorPrimeData(CalculatorData):
    def Process(self, data):
        return self.GetPower(data)
    
    def GetPower(self, data:dict):
        result = deepcopy(data)
        totalPower = 0
        for field, value in result.items():
            if field[:13] == "dcActivePower":
                totalPower += value
        if totalPower != 0:
            result["dcActivePower"] = totalPower
        return result
    
class CalculatorSolaredgeData(CalculatorData):
    def Process(self, data:dict):
        return self.GetEnergy(data)
        
    def GetEnergy(self, data:dict):
        result = {}
        sfField = {}
        valueField = {}
        writeData = {}
        for field, value in data.items():
            if field[-2:] == 'SF':
                sfField[field[:-2]] = value
            else:
                valueField[field] = value
        for sfField, sfValue in sfField.items():
            for vField, vValue in  valueField.items():
                if sfField in vField:
                    result[vField] = vValue * (10 ** sfValue)
                else:
                    result[vField] = vValue
        ld = LocalData(self.strID)
        localData = ld.Read()
        if localData != None:
            self.logger.info("{}_LocalData tot:{}, day:{}".format(self.strID, localData['totalEnergy'], localData['dailyEnergy']))
            localDataTime = datetime.datetime.strptime(localData['dt'], '%Y/%m/%d %H:%M:%S').date()
            if result['acActiveEnergy'] == 0.0:
                result['acActiveEnergy'] = localData['totalEnergy']
            if (datetime.datetime.now().date() > localDataTime):
                result['acActiveEnergyDaily'] = 0.0
            else:
                result['acActiveEnergyDaily'] = localData['dailyEnergy'] + ( result['acActiveEnergy'] - localData['totalEnergy'])
        else:
            result['acActiveEnergyDaily'] = 0.0
        self.logger.info("{}_ResultData tot:{}, day:{}".format(self.strID, result['acActiveEnergy'], result['acActiveEnergyDaily']))
        writeData["dailyEnergy"] = math.floor(result["acActiveEnergyDaily"] * 1000) / 1000
        writeData["totalEnergy"] = result["acActiveEnergy"]
        ld.Write(writeData)
        return result

class CalculatorFroniusData(CalculatorData):
    def Process(self, data:dict):
        return self.GetEnergy(data)
    
    def GetEnergy(self, data:dict):
        writeData = {}
        ld = LocalData(self.strID)
        localData = ld.Read()
        if localData != None:
            self.logger.info("{}_LocalData tot:{}, day:{}".format(self.strID, localData['totalEnergy'], localData['dailyEnergy']))
            localDataTime = datetime.datetime.strptime(localData['dt'], '%Y/%m/%d %H:%M:%S').date()
            if data['acActiveEnergy'] == 0.0:
                data['acActiveEnergy'] = localData['totalEnergy']
            if (datetime.datetime.now().date() > localDataTime):
                data['acActiveEnergyDaily'] = 0.0
            else:
                data['acActiveEnergyDaily'] = localData['dailyEnergy'] + ( data['acActiveEnergy'] - localData['totalEnergy'])
        else:
            data['acActiveEnergyDaily'] = 0.0
        self.logger.info("{}_ResultData tot:{}, day:{}".format(self.strID, data['acActiveEnergy'], data['acActiveEnergyDaily']))
        writeData["dailyEnergy"] = math.floor(data["acActiveEnergyDaily"] * 1000) / 1000
        writeData["totalEnergy"] = data["acActiveEnergy"]
        ld.Write(writeData)
        return data

class CalculatorKacoData(CalculatorData):
    def Process(self, data:dict):
        return self.GetEnergy(data)
    
    def GetEnergy(self, data:dict):
        writeData = {}
        ld = LocalData(self.strID)
        localData = ld.Read()
        if localData != None:
            self.logger.info("{}_LocalData tot:{}, day:{}".format(self.strID, localData['totalEnergy'], localData['dailyEnergy']))
            if localData['dailyEnergy'] <= data['acActiveEnergyDaily']:
                energyDeviation = data['acActiveEnergyDaily'] - localData['dailyEnergy']
                localData['totalEnergy'] += energyDeviation
            else:
                localData['totalEnergy'] += data['acActiveEnergyDaily']
            data['acActiveEnergy'] = localData['totalEnergy']
        else:
            data['acActiveEnergy'] = data['acActiveEnergyDaily']
        self.logger.info("{}_ResultData tot:{}, day:{}".format(self.strID, data['acActiveEnergy'], data['acActiveEnergyDaily']))
        writeData["dailyEnergy"] = data["acActiveEnergyDaily"]
        writeData["totalEnergy"] = math.floor(data["acActiveEnergy"] * 1000) / 1000
        ld.Write(writeData)
        return data
        
class AbsCalculatorInterface():
    def __init__(self, Filter):
        self.Filter = Filter
        
    def Process(self, data):
        return self.Filter.Process(data)
    
        
class LocalData:
    def __init__(self, ID):
        self.id = ID
        self.tempFilePath = '/home/pi/TempData/TempFile'
        if not os.path.isdir("/home/pi/TempData"):
            os.makedirs("/home/pi/TempData")

    def Read(self):
        result = None
        try:
            tfp = shelve.open(self.tempFilePath)
            if self.id in tfp:
                result = tfp[self.id]
        finally:
            tfp.close()
            return result
        
    def Write(self, data:dict):
        contain = {}
        dt = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        contain['dt'] = dt
        try:
            tfp = shelve.open(self.tempFilePath)
            for field, value in data.items():
                contain[field] = value
            tfp[self.id] = contain
        finally:
            tfp.close()

# -*- coding: UTF-8 -*-
import copy

from py_linq.py_linq import Enumerable


class Handle():
    def __init__(self, settingInfo, projectID, oldFlag):
        self.projectID = projectID
        self.oldFlag = oldFlag
        self.tienjiInfo = settingInfo['TienJi']
        self.SplitUnitDict = {'sp':self.SplitUnitSp, 'inv':self.SplitUnitInv}
        self.unitData = None

    def Process(self, data):
        result = []
        rawData = {'data':data, 'objectID':self.projectID}
        if (self.oldFlag == False):
            self.unitData = copy.deepcopy(rawData)
            for deviceType in self.SplitUnitDict.keys():
                if (data[deviceType]):
                    splitResult = self.SplitAll(rawData, deviceType)
                    if splitResult != None:
                        if len(result) == 0:
                            result.extend(splitResult)
                        else:
                            self.MergeData(result, splitResult)
            if len(result) == 0:
                result.append(rawData)
        else:
            result.append(rawData)
        return result

    def MergeData(self, result, splitResult):
        for sr in splitResult:
            if sr['objectID'] == self.projectID:
                continue
            for r in result:
                if sr['objectID'] == r['objectID']:
                    r['data'].update(sr['data'])

    def SplitAll(self, rawData, deviceType):
        result = None
        if self.projectID in self.tienjiInfo[deviceType]:
            splitProject = self.tienjiInfo[deviceType][self.projectID]
            lowList = Enumerable(splitProject).group_by(key_names=['id'], key=lambda x: x['id']).to_list()
            if deviceType in rawData['data']:
                result = self.SplitLow(lowList, deviceType)
                self.SplitUnitDict[deviceType](lowList)
                self.unitData['rawData'] = copy.deepcopy(rawData['data'])
                result.append(self.unitData)
        return result

    def SplitLow(self, lowList, deviceType):
        result = []
        newSpData = self.unitData['data'][deviceType]
        for lowItem in lowList:
            if lowItem.key.id == self.projectID:
                continue
            publicFrame = {'data':{deviceType:{}},'objectID':lowItem.key.id}
            for lowFlag in lowItem._iterable._data:
                publicFrame['data'][deviceType][int(lowFlag['flag'])] = copy.deepcopy(newSpData[int(lowFlag['flag'])])
                if publicFrame['data'][deviceType][int(lowFlag['flag'])] != None:
                    publicFrame['data'][deviceType][int(lowFlag['flag'])]["objectID"] = lowItem.key.id
            result.append(publicFrame)
        return result

    def SplitUnitSp(self,lowList):
        # only for spm-3
        totalFieldDict={
            "acCurrentL1":0,
            "acCurrentL2":0,
            "acCurrentL3":0,
            "acActivePowerL1":0,
            "acActivePowerL2":0,
            "acActivePowerL3":0,
            "acActiveEnergy":0,
            "reactiveEnergy":0
        }
        for spKey, spValue in self.unitData['data']['sp'].items():
            if spKey == 1:
                continue
            if (spValue != None):
                for field in totalFieldDict.keys():
                    totalFieldDict[field] += spValue[field]
            else:
                del self.unitData['data']['sp']
                self.unitData['data']['sp'] = {}
                self.unitData['data']['sp'][1] = None
                return True
        if self.unitData['data']['sp'][1] != None:
            for field, value in totalFieldDict.items():
                self.unitData['data']['sp'][1][field] = self.unitData['data']['sp'][1][field] - value
        for lowItem in lowList:
            if lowItem.key.id != self.projectID:
                for lowFlag in lowItem._iterable._data:
                    del self.unitData['data']['sp'][int(lowFlag['flag'])]
        return True

    def SplitUnitInv(self,lowList):
        for lowItem in lowList:
            if lowItem.key.id != self.projectID:
                for lowFlag in lowItem._iterable._data:
                    del self.unitData['data']['inv'][int(lowFlag['flag'])]
        return True
    
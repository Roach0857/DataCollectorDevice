# -*- coding: UTF-8 -*-
import datetime
import json
import multiprocessing as mp
import os
import socket
import time
from pathlib import Path
from queue import Queue
import requests
from Entity.SendData import SendData

import Handler.Kinesis as HK
import Handler.Logger as HL


class Handle():
    def __init__(self, config, settingInfo, readInfo, socketData:Queue, kinesisData:Queue, logger):
        self.config = config
        self.readInfo = readInfo
        self.settingInfo = settingInfo
        self.socketData = socketData
        self.kinesisData = kinesisData
        self.logger = logger
        self.kinesisHandler = HK.Handle(logger)
        self.kinesisHandler.kinesisStream.describe(self.settingInfo['kinesisSource'][readInfo['operateModel']])
        self.GetPacket('LostDataPath', self.socketData)
        self.GetPacket('KinesisDataPath', self.kinesisData)
        self.sendFlag = False

    def DoSend(self, folderPath):
        self.sendFlag = True
        self.logger.info("Start DoSend")
        try:
            if folderPath == 'KinesisDataPath':
                self.SendPacket(self.kinesisData, self.SendKinesis)
            else:
                self.SendPacket(self.socketData, self.SendSocket)
        except Exception as ex:
            self.logger.warning(f"DoSend, ex: {ex} | {HL.SystemExceptionInfo()}")
        self.sendFlag = False
            
    def SendPacket(self, queueData:Queue, sendFunction):
        while(not queueData.empty()):
            sendData:SendData
            sendData = queueData.queue[0]
            data = json.loads(sendData.data)
            if sendFunction(data):
                self.DeleteFile(sendData.path)
                queueData.get()
   
    def SendSocket(self, data):
        flag = False
        strData = ''
        if self.readInfo['oldFlag']:
            if data['info']['type'] == 'data':
                strLen = "{:04x}".format(len(data['content']))
                strData = strLen + data['content']
            else:
                self.logger.info("Can not send to old Dataflow")
                return True
        else:
            data['info']['sendTime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if self.readInfo['nodeModel'] == 'slave':
                data['info']['sendFrom'] = self.readInfo['nodeID'][self.readInfo['nodeModel']]
                data['info']['size'] = 0
                strData = json.dumps(data)
                size = len(strData.encode('utf-8'))
                data['info']['size'] = size + len(str(size).encode('utf-8')) - 1
                strData = json.dumps(data)
            else:
                strData = json.dumps(data)
        
        self.logger.info(f"Send Socket -> IP: {self.config['server']['ip']}, Port: {self.config['server']['port']}")
        self.logger.info(f"Send Socket -> Data: {strData}")
        flag = self.SocketProcess(self.config['server']['ip'], self.config['server']['port'], strData)
        if not flag:
            flag = self.SocketProcess(self.config['server_backup']['ip'], self.config['server_backup']['port'], strData)
        if flag:
            self.logger.info("Socket Send OK")
        else:
            self.logger.info("Socket Send Failed")
        
        self.logger.debug("Socket Send Other Server ...")
        if len(self.settingInfo['otherServer']) != 0:
            for s in self.settingInfo['otherServer']:
                p = mp.Process(target=self.SocketProcess(), args=(s['ip'], s['port'], strData))
                p.start()
        return flag
    
    def SendKinesis(self, data):
        if len(data) == 0:
            return True
        try:
            for shard in self.kinesisHandler.kinesisStream.details['Shards']:
                if 'EndingSequenceNumber' not in shard:
                    self.logger.info(f"Send Kinesis -> Shard: {shard}")
                    self.logger.info(f"Send Kinesis -> Data: {data}")
                    result = self.kinesisHandler.kinesisStream.put_records(data)
                    self.logger.info(f"Send Kinesis Result: {result}")
                    if result['FailedRecordCount'] > 0:
                        return False
                    return True
            self.kinesisHandler.kinesisStream.get_shards(self.settingInfo['kinesisSource'])
            return False
        except Exception as ex:
            self.logger.warning("SendKinesis, ex: {0} | ".format(ex))
            return False
        
    def SocketProcess(self, ip, port, data):
        sendFlag = False
        strIP = ''
        intPort = int(port)
        for count in range(3):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    strIP = socket.gethostbyname(ip)
                    sock.settimeout(5)
                    sock.connect((strIP, intPort))
                    sock.sendall(data.encode(encoding='utf_8', errors='strict'))
                    self.logger.debug("[SEND]:{0}".format(data))
                    self.logger.debug("[SEND SUCCESS]")
                    sendFlag = True
                    sock.close()
                    break
            except Exception as ex:
                self.logger.warning(f"SendProcessor_SendMethod, ex: {ex} | {HL.SystemExceptionInfo()}")
        return sendFlag            
        
    def DeleteFile(self, path):
        if os.path.isfile(path) :
            os.remove(path)
            self.logger.debug("Remove {0}".format(path))

    def GetPacket(self, folderPath, queueData:Queue):
        self.logger.debug("Start Get Lost Packet")
        fileList = sorted(Path(self.settingInfo[folderPath]).iterdir(),key=os.path.getmtime)
        if len(fileList) != 0:
            for fL in fileList:
                self.logger.debug("File Name: {0}".format(fL.name))
                fileResult = self.ReadFile(self.settingInfo[folderPath] + fL.name) 
                if fileResult != None:
                    queueData.put(SendData(fileResult, self.settingInfo[folderPath] + fL.name))
                else:
                    self.DeleteFile(self.settingInfo[folderPath] + fL.name)
        self.logger.debug("Finish Get Lost Packet")
    
    def ReadFile(self, filePath):
        result = None
        with open(filePath, 'r') as f:
            readData = f.read()
            if len(readData) != 0:
                try:
                    self.logger.debug("Get Lost Packet: {0}".format(readData))
                    result = readData
                except Exception as ex:
                    self.logger.warning("GetPacket, ex: {0} | ".format(ex))
            f.close()
        return result
        
    def PostToApi(self, data):
        sendFlag = False
        token = self.config['oqc']['token']
        majorFlag = os.system('nc -vz -w5 {0} {1}'.format(self.config['server']['ip'], self.config['server']['port']))
        backupFlag = os.system('nc -vz -w5 {0} {1}'.format(self.config['server_backup']['ip'], self.config['server_backup']['port']))

        if ((majorFlag == 0) or (backupFlag == 0)):
            connect_flag = True
        else:
            connect_flag = False

        data['upload_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data['project'] = self.readInfo['projectID']
        data['ping_connect'] = connect_flag
        data['token'] = token

        try:
            jsonStr = json.dumps(data)
            self.logger.info("jsonSTR:{0}".format(jsonStr))
            r = requests.post(self.config['oqc']['url'], json=jsonStr, timeout = 5)
            self.logger.info("requests:" + r.content.decode())
        except Exception as ex:
            self.logger.warning(f"SendProcessor_PostToApi, ex: {ex} | {HL.SystemExceptionInfo()}")

        return sendFlag
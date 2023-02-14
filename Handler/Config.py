# -*- coding: UTF-8 -*-
import json
import os
import time
import boto3

class Handle():
    def __init__(self, thingName:str):
        self.thingName = thingName
        self.settingInfo = self.GetInfo('Config/SettingInfo')
        self.readInfo = self.GetReadInfo()
        self.config = self.settingInfo['operateModel'][self.readInfo['operateModel']]
        self.CheckFolder(self.settingInfo['LostDataPath'])
        self.CheckFolder(self.settingInfo['KinesisDataPath'])

    def GetInfo(self, filename):
        File = None
        FileData = None
        Result = None
        File = open('{0}.json'.format(filename), 'r')
        FileData = File.read()
        File.close()
        Result = json.loads(FileData)
        return Result

    def PingInternet(self):
        while(True):
            flag = os.system('ping -c 1 www.google.com')
            if flag == 0:
                break
            time.sleep(1)

    def GetReadInfo(self):
        self.PingInternet()
        ssmResult = self.GetSSMParameter()
        if ssmResult != None:
            result = json.loads(ssmResult)
            with open("/home/pi/ReadInfo.json", "w") as file:
                file.write(json.dumps(result, indent=4))
        else:
            result = self.GetInfo('/home/pi/ReadInfo')
        with open("Config/ReadInfo.json", "w") as file:
            file.write(json.dumps(result, indent=4))
        
        return result
    
    def GetSSMParameter(self):
        session = boto3.Session()
        credentials = session.get_credentials()
        try:
            self.RegulateTime()
            ssm_client = boto3.client('ssm', region_name=session.region_name,
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key)
            parameterResult = ssm_client.get_parameter(Name=self.thingName)
            return parameterResult["Parameter"]["Value"]
        except Exception as ex:
            print(ex)
        return None

    def RegulateTime(self):
        ntpServerList = ["tock.stdtime.gov.tw","watch.stdtime.gov.tw","time.stdtime.gov.tw","clock.stdtime.gov.tw","tick.stdtime.gov.tw"]
        flag = False
        if self.thingName.split("-")[-3] != "ecu":
            osResult = os.popen("systemctl stop ntp.service; echo $?").read()
            while(True):
                for ntpServer in ntpServerList:
                    ntpResult = os.popen(f"sudo ntpdate {ntpServer}").read()
                    print("ntpResult:{}".format(ntpResult))
                    if ("offset" in ntpResult):
                        flag = True
                        break
                    time.sleep(1)
                if flag:
                    osResult = os.popen("systemctl start ntp.service; echo $?").read()
                    break
    
    def CheckFolder(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)
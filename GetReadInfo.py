import json
import sys
import os
import time
import boto3

def RegulateTime(thingName):
    if thingName.split("-")[-3] != "ecu":
        osResult = os.popen("systemctl stop ntp.service; echo $?").read()
        while(True):
            count = 1
            ntpServer = ["tock.stdtime.gov.tw",
                        "watch.stdtime.gov.tw",
                        "time.stdtime.gov.tw",
                        "clock.stdtime.gov.tw",
                        "tick.stdtime.gov.tw"]
            ntpResult = None
            ntpResult = os.popen("sudo ntpdate {}".format(ntpServer[count%5])).read()
            print("ntpResult:{}".format(ntpResult))
            if ("offset" in ntpResult):
                break
            elif (count == 60):
                break
            time.sleep(5)
            count +=1
        osResult = os.popen("systemctl start ntp.service; echo $?").read()

if __name__ == '__main__':
    thingName = sys.argv[1]
    session = boto3.Session()
    credentials = session.get_credentials()
    RegulateTime(thingName)
    ssm_client = boto3.client('ssm', region_name=session.region_name,
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key)
    ssmResult = ssm_client.get_parameter(Name=thingName)
    result = json.loads(ssmResult["Parameter"]["Value"])
    with open("/home/pi/ReadInfo.json", "w") as file:
        file.write(json.dumps(result, indent=4))
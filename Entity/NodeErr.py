import datetime


class NodeErr():
    def __init__(self):
        self.bus = {}
        self.internetFlag = None

    def Add(self, errCode, nodeID):
        errObject = {}
        errContent = {"Code": errCode, "stamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        if len(self.bus) == 0:
            errObject['node'] = nodeID
            errObject['err'] = []
            errObject['err'].append(errContent)
            self.bus[nodeID] = errObject
        else:
            if nodeID in self.bus:
                self.bus[nodeID]['err'].append(errContent)

    def Make(self, flag, nodeID):
        if self.internetFlag != None:
            if self.internetFlag:
                if not flag:
                    self.Add("PI-0001", nodeID)
            else:
                if flag:
                    self.Add("PI-0002", nodeID)
                else:
                    self.Add("PI-0001", nodeID)
        self.internetFlag = flag
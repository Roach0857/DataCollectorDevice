class NodeID():
    def __init__(self, readInfo):
        self.flag = 1
        if readInfo['nodeModel'] == 'single':
            self.me = readInfo['nodeID']['master']
        else:
            self.me = readInfo['nodeID'][readInfo['nodeModel']]
            if readInfo['nodeModel'] == 'master':
                self.you = readInfo['nodeID']['slave']
            else:
                self.you = readInfo['nodeID']['master']
                self.flag = 2
                
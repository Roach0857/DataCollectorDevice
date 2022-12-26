class ModbusData():
    def __init__(self):
        self.data = [None]
        self.readTime = 0
        
    def CheckData(self):
        for d in self.data:
            if d == None:
                return False
            else:
                return True
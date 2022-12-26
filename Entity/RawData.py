class RawData:
    def __init__(self):
        self.currentData = None
        self.mergeData = None
    def Any(self):
        if self.currentData == None or self.mergeData == None:
            return False
        return True
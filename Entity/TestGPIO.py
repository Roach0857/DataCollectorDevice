BCM = 'BCM'
OUT = True
IN = False
HIGH = True
LOW = False
allPin = {}

def setmode(mode):
    return 'Set Mode {0}'.format(mode)

def setup(number, mode, initial = None):
    allPin[str(number)] = Pin(initial, mode)
        
def cleanup():
    allPin.clear()
    return 'Cleanup GPIO Pin'

def input(number):
    allPin[str(number)].input()

def output(number, state):
    allPin[str(number)].output(state)
    
class Pin():
    def __init__(self, state = False, mode = None):
        self.state = state
        self.mode = mode
        
    def input(self):
        if self.mode != None:
            if not self.mode:
                return self.state
            else:
                raise ValueError("Can't Input Pin, GPIO Mode Wrong")
        else:
            raise ValueError("Can't Input Pin, GPIO Mode Not Set")
    
    def output(self, state):
        if self.mode != None:
            if self.mode:
                self.state = state
        else:
            raise ValueError("Can't Input Pin, GPIO Mode Not Set")
            
        
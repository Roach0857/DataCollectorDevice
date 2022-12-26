import sys

if sys.argv[1].split("-")[-3] == "ecu":
    import Entity.TestGPIO as GPIO 
else:
    import RPi.GPIO as GPIO 

GPIO.setmode(GPIO.BCM)

class PinInfo():
    def __init__(self, number, mode, init = None):
        self.number = number
        if init != None:
            GPIO.setup(number, mode, initial = init)
        else:
            GPIO.setup(number, mode)

    def Set(self, state):
        GPIO.output(self.number, state)
        
    def Get(self):
        return GPIO.input(self.number)
        
class GPIOInfo():
    def __init__(self, mode):
        if mode == "master":
            self.myWorkPin = PinInfo(16, GPIO.OUT, GPIO.HIGH)
            self.myInternetPin = PinInfo(12, GPIO.OUT)
            self.yourWorkPin = PinInfo(20, GPIO.IN)

        elif mode == "slave":
            self.yourWorkPin = PinInfo(16, GPIO.IN)
            self.yourInternetPin = PinInfo(12, GPIO.IN)
            self.myWorkPin = PinInfo(20, GPIO.OUT, GPIO.HIGH)
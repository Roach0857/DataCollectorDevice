import datetime

import crcmod
import serial
from pymodbus.client.sync import ModbusSerialClient as pyRtu

import Handler.Logger as HL


class ReadDevice:
    def __init__(self, strSerial, dataType, readSetting, deviceItem, deviceType, label, logger):
        self.strSerial = strSerial
        self.dataType = dataType
        self.readSetting = readSetting
        self.deviceItem = deviceItem
        self.deviceType = deviceType
        self.label = label
        self.logger = logger
        self.startDatetime = datetime.datetime.now()
        self.timeout = 0.5
        self.pymc = None
        self.readMethod = None
        self.initialize()
        
    def initialize(self):
        self.pymc = pyRtu(method='rtu', port=self.strSerial, baudrate=9600, timeout=self.timeout, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE)
        self.readMethod = self.pymc.read_holding_registers
        
    def read(self):
        result = []
        checkFlag = False
        self.logger.info("Connect Serial {0}".format(self.pymc.connect()))
        if self.pymc.connect():
            for rs in self.readSetting:
                read = None
                read = self.readMethod(rs['StartBit'], rs['Length'], unit=self.deviceItem['macaddress'])
                self.logger.info(f"Read Result:{read}")
                if not read.isError():
                    for rr in read.registers:
                        result.append(rr)
                    checkFlag = True
                else:
                    for i in range(rs['Length']):
                        result.append(None)
        self.pymc.close()
        if checkFlag:
            return result
        else:
            return [None]
  
class ReadSP(ReadDevice):
    def initialize(self):
        self.pymc = pyRtu(method='rtu', port=self.strSerial, baudrate=9600, timeout=self.timeout, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE)
        self.readMethod = self.pymc.read_input_registers

class ReadINVKaco(ReadDevice):
    def initialize(self):
        self.pymc = serial.Serial(port=self.strSerial, baudrate=9600, timeout=self.timeout, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE,stopbits=serial.STOPBITS_ONE)
        
    def read(self):
        if self.label[5:] == '3':
            return self.ReadKacoStandard()
        else:
            return self.ReadKacoGeneric()
        
    def ReadKacoStandard(self):
        result = []
        queryMacaddress = '{0:02d}'.format(self.deviceItem['macaddress'])
        self.logger.info(" READ START INV: {0}; Macaddress: {1}".format(self.deviceItem['flag'], self.deviceItem['macaddress']))
        try:
            self.pymc.flushInput()
            self.pymc.write('#{0}0\r'.format(queryMacaddress).encode())
            rawData = self.pymc.read_until(b'')
            rawDataSplit = rawData.split()
            for r in rawDataSplit:
                result.append(str(r)[2:-1])
            resultChecksum = int.from_bytes(rawDataSplit[-2],'big')
            if not self.OneByteChecksum(rawData, resultChecksum):
               result = [None]
        except Exception as ex:
            result = [None]
            self.logger.info("readKACOStandard: {0} | ".format(ex))
            HL.SystemExceptionInfo()
        finally:
            self.pymc.close()
            return result
        
    def ReadKacoGeneric(self):
        #read = '*06n 16 50kH3P 47  683.1  0.00    1.8  0.00    1.6  0.00    1.9  0.00     0     0 1.000  45.4      0 '
        #read = '*05n 20 200L32 4   501.0  6.89   3453   516.1   7.31   3775   228.3 10.50  229.2 10.58   228.5 10.8   7228   7158 1.000   47.6  83952'
        result = [None]
        queryMacaddress = '{0:02d}'.format(self.deviceItem['macaddress'])
        self.logger.info(" READ START INV: {0}; Macaddress: {1}".format(self.deviceItem['flag'], self.deviceItem['macaddress']))
        try:
            self.pymc.flushInput()
            self.pymc.write('#{0}0\r'.format(queryMacaddress).encode())
            read = self.CheckCRC(checkCRCFlag=True)
            if (read != None):
                result = read.split()
                self.logger.info(result)
                if queryMacaddress in result[0]:
                    self.logger.info("READ Macaddress: {0} OK<br>".format(self.deviceItem['macaddress']))
                else:
                    self.logger.info("READ Macaddress: {0} NG<br>".format(self.deviceItem['macaddress']))
                    result = [None]
            else:
                self.logger.info("READ Macaddress: {0} NG<br>".format(self.deviceItem['macaddress']))
        except Exception as ex:
            self.logger.info("readGeneric: {0} | ".format(ex))
            HL.SystemExceptionInfo()
        finally:
            self.pymc.close()
            return result
        
    def CheckCRC(self, checkCRCFlag = False):
        readResult = None
        readFlag = False
        startDatetime = None
        receiveFlag = False
        result = []
        resultString = ''
        receiveCRC = 0
        checkCRC = 0
        try:
            readFlag = False
            startDatetime = datetime.datetime.now()
            while ((datetime.datetime.now() - startDatetime).total_seconds() < self.timeout):
                readChar = self.pymc.read(1).decode()
                if (not receiveFlag):
                    if (readChar != ''):
                        if (ord(readChar) == 10): #LF Start
                            receiveFlag = True
                            self.logger.info("Start Recv....")
                elif (readChar != ''):
                    if (ord(readChar) == 13):
                        if receiveFlag:
                            readFlag = True
                            self.logger.info("End Recv....")
                        break
                    else:
                        result.append(readChar)
                else:
                    pass

            if readFlag:
                self.logger.info("Received OK")
                resultString = ''.join(result)
                self.logger.info("First Recv>" + resultString + "<")
                receiveString = resultString[:len(resultString) - 4]
                if checkCRCFlag:
                    receiveCRC = resultString[len(resultString)-4:]
                    crc = crcmod.mkCrcFun(0x11021, 0x0000, True, 0xFFFF)
                    checkCRC = str(hex(crc(receiveString.encode())))[2:].zfill(4).upper()
                    if (receiveCRC == checkCRC):
                        self.logger.info("CRC OK {0} {1}".format(receiveCRC, checkCRC))
                        readResult = receiveString
                    else:
                        self.logger.info("CRC NG {0} {1}".format(receiveCRC, checkCRC))
                else:
                    readResult = receiveString
        except Exception as ex:
            self.logger.info("KacoMethod: {0} | ".format(ex))
            HL.SystemExceptionInfo()
        return readResult

    def OneByteChecksum(self, rawData, resultChecksum):
        print(resultChecksum)
        calculateChecksum = 0
        count = 0
        for r in rawData[1:]:
            if count < 56:
                calculateChecksum += r
                count += 1
            else: 
                break
        calculateChecksum = calculateChecksum % 256
        print(calculateChecksum)
        if resultChecksum == calculateChecksum:
            return True
        else:
            return False
        
class ReadINVDelta(ReadDevice):
    def initialize(self):
        self.pymc = pyRtu(method='rtu', port=self.strSerial, baudrate=9600, timeout=self.timeout, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE)
        self.readMethod = self.pymc.read_input_registers
    
    # old read funtion
    def readDeviceData(self):
        result = [None]
        try:
            registersResult = []
            read = self.pymc.read_input_registers(self.readSetting[0]['StartBit'], self.readSetting[0]['Length'], unit=self.deviceItem['macaddress'])
            if not read.isError():
                registersResult.append(read.registers[1])
                registersResult.append(read.registers[0]) 
                registersResult.append(read.registers[5])
                registersResult.append(read.registers[4])
                self.logger.info("Delta Read Registers:{0}".format(registersResult))
                deltaResult = self.DeltaMethod(1, 3)
                if deltaResult != None:
                    registersResult.extend(deltaResult)
                    deltaResult = self.DeltaMethod(3, 2)
                    if deltaResult != None:
                        registersResult.extend(deltaResult)
                        result = registersResult
        except Exception as ex:
            self.logger.info("ReadINVDelta_readDeviceData: {0} | ".format(ex))
            HL.SystemExceptionInfo()
        finally:
            self.pymc.close()
            return result
        
    def DeltaMethod(self, site, countLenght):
        result = []
        try:
            for count in range(countLenght):
                read = self.pymc.write_register(self.readSetting[site]['StartBit'], self.readSetting[site]['Length'] + count, unit=self.deviceItem['macaddress'])
                self.logger.info("Write Registers Result:{0}".format(read.encode()))
                read = self.pymc.read_input_registers(self.readSetting[site+1]['StartBit'], self.readSetting[site+1]['Length'], unit=self.deviceItem['macaddress'])
                if not read.isError():
                    result.append(read.registers[1])
                    result.append(read.registers[2]) 
                    result.append(read.registers[3])
                    self.logger.info("Delta Read Registers:{0}".format(result))
                else:
                    return None
            return result
        except Exception as ex:
            self.logger.info("ReadINVDelta_read: {0} | ".format(ex))
            HL.SystemExceptionInfo()
            
def ReaderFactory(strSerial, dataType, readSetting, deviceItem, deviceType, label, logger):
    factory = {'kaco':ReadINVKaco,'delta':ReadINVDelta, 'sp':ReadSP, 'device':ReadDevice, 'cyberpower':ReadSP}
    if label in factory:
        result = factory[label](strSerial, dataType, readSetting, deviceItem, deviceType, label, logger)
    elif 'kaco' in label:
        result = factory['kaco'](strSerial, dataType, readSetting, deviceItem, deviceType, label, logger)
    elif 'spm' in label:
        result = factory['sp'](strSerial, dataType, readSetting, deviceItem, deviceType, label, logger)
    else:
        result = factory['device'](strSerial, dataType, readSetting, deviceItem, deviceType, label, logger)
    return AbsReaderInterface(result)

class AbsReaderInterface():
    def __init__(self, reader = None):
        self.reader = reader
    def read(self):
        return self.reader.read()
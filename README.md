# DataCollectorDevice
讀取(RS485)裝置訊號、資料整理、資料傳送

## Python版本
Python 3.9.2

## 安裝套件
1. py-linq 1.2.5
2. pymodbus 2.5.2
3. func-timeout 4.3.5
4. requests 2.25.1

## Config
### 1. config.ini
為了能夠在程式執行時動態切換模式。
### 2. ReadInfo.json
紀錄案場所需讀取(RS485)裝置的設定資訊和執行此服務的(樹莓派)裝置資訊。
### 3. SettingInfo.json
紀錄讀取逆變器、數位電表、日照計、溫度計的各種廠牌的設定資訊，以及天機拆分設定資訊。

## 執行程式
### 說明
```DataCollecotr.py```為雙系統下的主要執行程式，```DataCollectorsingle.py```
### 方法
1. 在終端機執行```./run_DataCollector.sh```指令，15秒後會開啟以DataCollector為命名的screen。
2. 在終端機執行```python3 DataCollector.py```指令，程式即會執行。



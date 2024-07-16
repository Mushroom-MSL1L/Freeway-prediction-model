import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import time
import sklearn
from datetime import datetime

# a list for convenice in calculating which day it is in a year
days = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
"""
2023 calendar, starts from Sunday, 2023/01/01, 1-indexed
    first row:
        0: workday
        1: holiday
        2: weekend
    second row:
        a value for convenice in calculating which hour it is in a holiday
    third row:
        a value represents how long the period is (in hour), 0 represent that it is not in a holiday period
"""
holiday = np.zeros((3, 426))
holiday[0] = [0,
              1, 1, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              1, 0, 0,
                       0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 1,
              1, 1, 1, 
                       0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0,
                                1,
              1, 1, 1, 1, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2, 
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2,
                 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 
                          0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 1, 1, 1,
              1, 0, 0, 0, 0, 0, 
                                2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0,
                    0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 
                             0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 1, 1,
              
              1, 0, 0, 0, 0, 0, 1,
              1, 1, 1, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0,
                       0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 
                             0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 1,
              1, 

                 1, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 
                          0, 0, 2,
              2, 0, 0, 0, 1, 1, 1,
              1, 1, 1, 1, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 1, 0]

holiday[1] = [  0,
               60,  84, 108, 132,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,  24,  48,  72,  96, 120, 144,
              168, 192, 216, 240, 264, 288, 312,
              336, 360, 384,
                             408, 432, 456,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,  24,  48,
               72,  96, 120, 
                             144, 168,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, -12,  12,
                                             36,
               60,  84, 108, 132, 156, 180, 204,
              228,   0,   0,   0,   0,   0,   0, 
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,
                     0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,
                                    0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,  24,  48,  72,  96, 
              120, 144, 168,   0,   0,   0, 
                                              0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,
                          0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0, -12,  12,  36,  60,
              
               84, 108, 132,   0,   0,  24,  48,
               72,  96, 120, 144, 168,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,
                               0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, -12,  12,  36,
               60,
                
                    84, 108, 132,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   
                                    0,   0,   0,
              -12,  12,  36,  60,  84, 108, 132,
              156, 180, 204, 228, 252, 276, 300,
              324,   0,   0,   0,   0,   0,   0,
                0,   0, -12,  12,  36]

holiday[2] = [  0,
              144, 144, 144, 144,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
              480, 480, 480, 480, 480, 480, 480,
              480, 480, 480, 480, 480, 480, 480,
              480, 480, 480,
                             480, 480, 480,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 192, 192, 192,
              192, 192, 192, 
                             192, 192,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 240, 240,
                                            240,
              240, 240, 240, 240, 240, 240, 240,
              240,   0,   0,   0,   0,   0,   0, 
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,
                     0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,
                                    0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0, 192, 192, 192, 192, 192, 
              192, 192, 192,   0,   0,   0, 
                                              0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,
                          0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0, 144, 144, 144, 144,
              
              144, 144, 144,   0, 192, 192, 192,
              192, 192, 192, 192, 192,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,
                               0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 144, 144, 144,
              144,
                  
                   144, 144, 144,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   
                                    0,   0,   0,
              336, 336, 336, 336, 336, 336, 336,
              336, 336, 336, 336, 336, 336, 336,
              336,   0,   0,   0,   0,   0,   0, 
                0,   0,  48,  48,  48]



def convert_and_store_ETagPairLive(store_path, conn):
    """
    Process ETagPairLive data from xml to list of dictionary
    should be xml
    should return a list of dictionary with data we need
    data name : "eTag 配對路徑動態資訊(v2.0)" 的任意一份資料當作格式範例
    reference data link : https://tisvcloud.freeway.gov.tw/history/motc20/ETag/20240302/ETagPairLive_2330.xml.gz
    data source page link : https://tisvcloud.freeway.gov.tw/history/motc20/ETag/
    """
    print("\tProcess ETagPairLive data from file: ", store_path)
    ###開始###
    print("store_path: ", store_path)
    xml_data = open(store_path, 'rb').read()
    root = ET.fromstring(xml_data)
    c = conn.db.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS ETagPairLive (
            ETagPairID TEXT,
            StartETagStatus INTEGER,
            EndETagStatus INTEGER,
            StartTime TEXT,
            EndTime TEXT,
            DataCollectTime TEXT
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS Flow (
            ETagPairID TEXT,
            VehicleType INTEGER,
            TravelTime INTEGER,
            StandardDeviation INTEGER,
            SpaceMeanSpeed INTEGER,
            VehicleCount INTEGER,
            FOREIGN KEY(ETagPairID) REFERENCES ETagPairLive(ETagPairID)
        )
    ''')

    for etag_pair_live in root.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairLive'):
        etag_pair_id = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairID').text
        start_etag_status = int(etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}StartETagStatus').text)
        end_etag_status = int(etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}EndETagStatus').text)
        start_time = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}StartTime').text
        end_time = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}EndTime').text
        data_collect_time = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}DataCollectTime').text

        c.execute('''
            INSERT INTO ETagPairLive (ETagPairID, StartETagStatus, EndETagStatus, StartTime, EndTime, DataCollectTime)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (etag_pair_id, start_etag_status, end_etag_status, start_time, end_time, data_collect_time))

        for flow in etag_pair_live.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}Flow'):
            vehicle_type = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}VehicleType').text)
            travel_time = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}TravelTime').text)
            standard_deviation = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}StandardDeviation').text)
            space_mean_speed = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}SpaceMeanSpeed').text)
            vehicle_count = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}VehicleCount').text)

            c.execute('''
                INSERT INTO Flow (ETagPairID, VehicleType, TravelTime, StandardDeviation, SpaceMeanSpeed, VehicleCount)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (etag_pair_id, vehicle_type, travel_time, standard_deviation, space_mean_speed, vehicle_count))

    conn.db.commit()
    print("Data successfully inserted into the SQLite3 database.")
    ###結束###

def convert_and_store_traffic_accident(store_path, conn):
    """
    Process traffic accident data from xlsx to list of dictionary
    should be xlsx file
    should return a list of dictionary with data we need
    data name : "112年1-10月及113年1-2月交通事故簡訊通報狀況資料" 的分析資料或驗證資料的任意一份資料當作格式範例
    reference data link : https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E7%B0%A1%E8%A8%8A%E9%80%9A%E5%A0%B1%E8%B3%87%E6%96%99.xlsx
    data source page link : https://freeway2024.tw/links#links
    """
    print("\tProcess traffic accident data from file: ", store_path)
    ###開始###
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS traffic_accident (
        年 INTEGER,
        月 INTEGER,
        日 INTEGER,
        時 INTEGER,
        分 INTEGER,
        國道名稱 TEXT,
        方向 TEXT,
        里程 REAL,
        事件發生 TEXT,
        交控中心接獲通報 TEXT,
        CCTV監看現場 TEXT,
        CMS發布資訊 TEXT,
        交控中心通報工務段 TEXT,
        事故處理小組出發 TEXT,
        事故處理小組抵達 TEXT,
        事故處理小組完成 TEXT,
        事件排除 TEXT,
        處理分鐘 INTEGER,
        事故類型 TEXT,
        死亡 INTEGER,
        受傷 INTEGER,
        內路肩 INTEGER,
        內車道 INTEGER,
        中內車道 INTEGER,
        中車道 INTEGER,
        中外車道 INTEGER,
        外車道 INTEGER,
        外路肩 INTEGER,
        匝道 INTEGER,
        簡訊內容 TEXT,
        翻覆事故註記 INTEGER,
        施工事故註記 INTEGER,
        危險物品車輛註記 INTEGER,
        車輛起火註記 INTEGER,
        冒煙車事故註記 INTEGER,
        主線中斷註記 INTEGER,
        肇事車輛 TEXT,
        車輛1 TEXT,
        車輛2 TEXT,
        車輛3 TEXT,
        車輛4 TEXT,
        車輛5 TEXT,
        車輛6 TEXT,
        車輛7 TEXT,
        車輛8 TEXT,
        車輛9 TEXT,
        車輛10 TEXT,
        車輛11 TEXT,
        車輛12 TEXT,
        分局 TEXT
    )
    '''
    conn.db.cursor().execute(create_table_query)
    data_file = pd.read_excel(store_path)
    data_file.to_sql('traffic_accident', conn.db, if_exists='replace', index=True)
    conn.db.commit()
    ###結束###

def convert_and_store_construction_zone(store_path, conn):
    """
    Process construction zone data from xlsx to list of dictionary
    should be xlsx file
    should return a list of dictionary with data we need
    data name : "112 年 1-10 月及 113 年 1-2 月施工路段資料"的分析資料或驗證資料的任意一份資料當作格式範例
    reference data link : https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E9%81%93%E8%B7%AF%E6%96%BD%E5%B7%A5%E8%B7%AF%E6%AE%B5%E8%B3%87%E6%96%99.xlsx
    data source page link : https://freeway2024.tw/links#links
    """
    print("\tProcess construction zone data from file: ", store_path)
    ###開始###
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS construction_zone (
        incStepIncidentId INTEGER,
        incStepNum INTEGER,
        incStepTime TEXT,
        incStepEndTime TEXT,
        incStepFreewayId TEXT,
        incStepDirection TEXT,
        incStepStartMileage REAL,
        incStepEndMileage REAL,
        incStepBlockagePattern TEXT
    )
    '''
    conn.db.cursor().execute(create_table_query)
    data_file = pd.read_excel(store_path)
    data_file.to_sql('construction_zone', conn.db, if_exists='replace', index=True)
    conn.db.commit()
    ###結束###

def holiday_and_date_sine_processor(start_time):
    '''
    Given a time in 2023, return whether it is holiday or weekend, and its value after sine transformation

    input:
        time: a string in format 'YYYY-MM-DDTHH:MM:SS...', 24-hours based time
              e.g. 2023-01-01T23:45:00+08:00
    
    output:
        is_weekend: a boolean
        is_holiday: a boolean
        holiday:    a float represent its value in holiday after sine transformation (resolution is hour), 
                    if the time does not have to do sine transformation, return 0
        month:      a float represent its value in a year after sine transformation (resolution is month)
        day:        a float represent its value in a year after sine transformation (resolution is day)
        minute:     a float represent its value in a day after sine transformation (resolution is 5 minute)
    '''
    is_holiday = False
    is_weekend = False
    _holiday = 0.0

    date_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date
    
    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.sin(2 * np.pi * (date_time.month / 12))
    day = np.sin(2 * np.pi * (date / 365))
    minute = np.sin(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute
    
    _holiday = np.sin(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute

def holiday_and_date_cosine_processor(start_time):
    '''
    Given a time in 2023, return whether it is holiday or weekend, and its value after cosine transformation

    input:
        time: a string in format 'YYYY-MM-DDTHH:MM:SS...', 24-hours based time
              e.g. 2023-01-01T23:45:00+08:00
    
    output:
        is_weekend: a boolean
        is_holiday: a boolean
        holiday:    a float represent its value in holiday after cosine transformation (resolution is hour), 
                    if the time does not have to do cosine transformation, return 1
        month:      a float represent its value in a year after cosine transformation (resolution is month)
        day:        a float represent its value in a year after cosine transformation (resolution is day)
        minute:     a float represent its value in a day after cosine transformation (resolution is 5 minute)
    '''
    is_holiday = False
    is_weekend = False
    _holiday = 1.0

    date_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date

    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.cos(2 * np.pi * (date_time.month / 12))
    day = np.cos(2 * np.pi * (date / 365))
    minute = np.cos(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute
    
    _holiday = np.cos(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute

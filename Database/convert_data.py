import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import re 

"""
Purpose of this file:
    Define all custom functions for "get_data.py", to load internet data to database.
    Should refer to "data_list.md" for more information of each original data structure.
"""

"""
    "convert_and_store_ETagPairLive" : 
        Process ETagPairLive data from xml to separated data and store in database
    data file : xml
    processed type(not return) : store in database (default name : row.db)
    data information:
        data name : "eTag 配對路徑動態資訊(v2.0)" 的任意一份資料當作格式範例
        reference data link : https://tisvcloud.freeway.gov.tw/history/motc20/ETag/20240302/ETagPairLive_2330.xml.gz
        data source page link : https://tisvcloud.freeway.gov.tw/history/motc20/ETag/
    procedure : 
        1. parse xml file as web data
        2. create ETagPairLive table
        3. extract first level data from "etag_pair_live" xml
            * drop uneeded row_data (both start and end status should be normal)
        4. extract second level data from "flow" xml
            * drop uneeded vehicle type data
            * process ETagPairID to 
                * highway as one digit
                * start_mileage as km
                * end_mileage as km
                * direction as char
            * process time to year, month, day, five_minute
        
"""
def convert_and_store_ETagPairLive(store_path, conn, car_code_needed, segment_id_needed_list):
    def parse_ETagPairID(input_str):
        input_str = str(input_str)
        match = re.match(r'(\d{2})([A-Z])(\d+)([A-Z])-(\d{2})([A-Z])(\d+)([A-Z])', input_str)
        if match:
            highway = match.group(1) 
            start_mileage = match.group(3) 
            end_mileage = match.group(7) 
            direction = match.group(4) 
            return int(highway), int(start_mileage)/10, int(end_mileage)/10, direction
        else:
            return None, None, None, None
        # end of parse_ETagPairID function 

    if car_code_needed is None or len(car_code_needed) == 0:
        raise ValueError("car_code_needed is not defined or empty")
    if segment_id_needed_list is None or len(segment_id_needed_list) == 0:
        raise ValueError("segment_id_needed is not defined or empty")
    # set up 
    xml_data = open(store_path, 'rb').read() # read xml file as web data
    root = ET.fromstring(xml_data)
    c = conn.db.cursor()

    # create table
    c.execute('''
        CREATE TABLE IF NOT EXISTS ETagPairLive (
            ID              INTEGER PRIMARY KEY AUTOINCREMENT,
            ETagPairID      TEXT,
            Highway         INTEGER,
            StartMileage    REAL,
            EndMileage      REAL,
            Direction       CHAR(1),
            Year            INTEGER,
            Month           INTEGER,
            Day             INTEGER,
            FiveMinute      INTEGER,
            VehicleType     INTEGER,
            SpaceMeanSpeed  INTEGER,
            VehicleCount    INTEGER
        )
    ''')

    for etag_pair_live in root.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairLive'):
        """ 
        xml EtagPair structure :
            ETagPairID TEXT,            store
            StartETagStatus INTEGER,    logic, and discard later
            EndETagStatus INTEGER,      logic, and discard later
            StartTime TEXT,             store
            EndTime TEXT,               don't need, always StartTime + 5 min
            DataCollectTime TEXT        don't need, the same as EndTime
        """
        # extract first level data 
        etag_pair_id = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}ETagPairID').text
        start_etag_status = int(etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}StartETagStatus').text)
        end_etag_status = int(etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}EndETagStatus').text)
        start_time = etag_pair_live.find('{http://traffic.transportdata.tw/standard/traffic/schema/}StartTime').text

        # drop uneeded invalid data
        if ((start_etag_status != 0) or (end_etag_status != 0)) :
            continue # two status should be normal 

        # get segment id 
        segment_ids = []
        for segment_id_needed in segment_id_needed_list:
            segment_ids.append(segment_id_needed["ID"])

        for flow in etag_pair_live.findall('.//{http://traffic.transportdata.tw/standard/traffic/schema/}Flow'):
            """ 
            xml Flow structure :
                ETagPairID TEXT,            store
                VehicleType INTEGER,        logic and store
                TravelTime INTEGER,         don't need 
                StandardDeviation INTEGER,  don't need 
                SpaceMeanSpeed INTEGER,     store
                VehicleCount INTEGER,       store 
            """
            # extract second level data
            vehicle_type = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}VehicleType').text)
            space_mean_speed = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}SpaceMeanSpeed').text)
            vehicle_count = int(flow.find('{http://traffic.transportdata.tw/standard/traffic/schema/}VehicleCount').text)

            # drop uneeded vehicle type data and segment id data
            if (vehicle_type not in car_code_needed) :
                continue
            if (etag_pair_id not in segment_ids) :
                continue
            # process ETagPairID to highway, start_mileage, end_mileage, direction
            highway, start_mileage, end_mileage, direction = parse_ETagPairID(etag_pair_id)
            # process time to year, month, day, five_minute
            dt = datetime.fromisoformat(start_time)
            year = dt.year
            month = dt.month
            day = dt.day
            start_dt_of_day = dt
            start_dt_of_day = start_dt_of_day.replace(hour=0, minute=0, second=0, microsecond=0)
            five_minute = int((dt - start_dt_of_day).seconds / 300) # 5 min = 300 sec
            # store data to database as ETagPairLive schema
            c.execute('''
                INSERT INTO ETagPairLive (ETagPairID, Highway, StartMileage, EndMileage, Direction, Year, Month, Day, FiveMinute, VehicleType, SpaceMeanSpeed, VehicleCount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (etag_pair_id, highway, start_mileage, end_mileage, direction, year, month, day, five_minute, vehicle_type, space_mean_speed, vehicle_count)
            )
    conn.db.commit()
    # end of convert_and_store_ETagPairLive function


"""
    "convert_and_store_traffic_accident" : 
        Process 交通事故簡訊通報狀況資料 data from xlsx to pandas dataframe and store in database
    data file : xlsx
    processed type(not return) : store in database (default name : row.db)
    data information:
        data name : "112年1-10月及113年1-2月交通事故簡訊通報狀況資料" 的分析資料或驗證資料的任意一份資料當作格式範例
        reference data link : https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E7%B0%A1%E8%A8%8A%E9%80%9A%E5%A0%B1%E8%B3%87%E6%96%99.xlsx
        data source page link : https://freeway2024.tw/links#links
    procedure : 
        1. create traffic_accident table
        2. parse xlsx to pandas dataframe
        3. process data
            * extract highway number
            * convert direction to char
            * one hot encoding for lane data
            * drop those data with missing value (NaN or None)
            * no need to precess time, already in correct format
        4. change data type
        5. pick up certain columns to database
"""
def convert_and_store_traffic_accident(store_path, conn):
    """ xlsx traffic accident structure
        年 INTEGER,             store
        月 INTEGER,             store
        日 INTEGER,             store
        時 INTEGER,             store
        分 INTEGER,             store
        國道名稱 TEXT,           change to integer and store
        方向 TEXT,              change to char and store
        里程 REAL,              store
        事件發生 TEXT,           not need
        交控中心接獲通報 TEXT,    not need
        CCTV監看現場 TEXT,       not need  
        CMS發布資訊 TEXT,        not need
        交控中心通報工務段 TEXT,   not need
        事故處理小組出發 TEXT,    not need
        事故處理小組抵達 TEXT,    not need
        事故處理小組完成 TEXT,    not need
        事件排除 TEXT,           not need
        處理分鐘 INTEGER,        store
        事故類型 TEXT,           not need
        死亡 INTEGER,           not need
        受傷 INTEGER,           not need
        內路肩 INTEGER,         store
        內車道 INTEGER,         store
        中內車道 INTEGER,       store
        中車道 INTEGER,         store
        中外車道 INTEGER,        store
        外車道 INTEGER,          store
        外路肩 INTEGER,          store
        匝道 INTEGER,          store
        簡訊內容 TEXT,           not need
        翻覆事故註記 INTEGER,    not need
        施工事故註記 INTEGER,    not need
        危險物品車輛註記 INTEGER, not need
        車輛起火註記 INTEGER,    not need
        冒煙車事故註記 INTEGER,  not need
        主線中斷註記 INTEGER,    not need
        肇事車輛 TEXT,          not need
        車輛1 TEXT,             not need
        車輛2 TEXT,             not need
        車輛3 TEXT,             not need
        車輛4 TEXT,             not need
        車輛5 TEXT,             not need
        車輛6 TEXT,             not need
        車輛7 TEXT,             not need
        車輛8 TEXT,             not need
        車輛9 TEXT,             not need
        車輛10 TEXT,            not need
        車輛11 TEXT,            not need
        車輛12 TEXT,            not need
        分局 TEXT               not need
    """
    def extract_highway_number (highway_name) :
        highway_name = str(highway_name)
        match = re.search(r'(\d+)', highway_name)
        if match:
            return match.group(1)
        else : 
            return None
        # end of extract_highway_number function
    def convert_direction (direction) :
        direction = str(direction)
        if pd.isnull(direction):
            return None
        elif "南" in direction:
            return 'S'
        elif "北" in direction:
            return 'N'
        elif "東" in direction:
            return 'E'
        elif "西" in direction:
            return 'W'
        else :
            return None
        # end of convert_direction function
    def one_hot (x) :
        if x == 1:
            return True
        else:
            return False
        # end of one_hot function
    
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS traffic_accident (
        ID              INTEGER PRIMARY KEY AUTOINCREMENT,
        Highway         INTEGER,
        Direction       CHAR(1),
        Mileage         REAL,
        Year            INTEGER,
        Month           INTEGER,
        Day             INTEGER,
        RecoveryMinute  INTEGER,
        FiveMinuteStart INTEGER,
        FiveMinuteEnd   INTEGER,
        內路肩          BOOLEAN,
        內車道          BOOLEAN,
        中內車道        BOOLEAN,
        中車道          BOOLEAN,
        中外車道        BOOLEAN,
        外車道          BOOLEAN,
        外路肩          BOOLEAN,
        匝道            BOOLEAN
    )
    '''
    # create table
    conn.db.cursor().execute(create_table_query)
    conn.db.commit()
    # pick up data from xlsx
    pd.options.mode.copy_on_write = True
    data_file = pd.read_excel(store_path)
    selected_columns = [
        '年', '月', '日', '時', '分', '國道名稱', '方向', '里程', 
        '處理分鐘', '內路肩', '內車道', '中內車道', '中車道', 
        '中外車道', '外車道', '外路肩', '匝道'
    ]
    data_subset = data_file[selected_columns]
    
    # process data
    data_subset['Highway'] = data_subset['國道名稱'].apply(extract_highway_number)
    data_subset['Direction'] = data_subset['方向'].apply(convert_direction)
    data_subset['FiveMinuteStart'] = (data_subset['時'] * 60 + data_subset['分']) // 5
    data_subset['FiveMinuteEnd'] =  (data_subset['時'] * 60 + data_subset['分'] + data_subset['處理分鐘']) // 5
    data_subset['內路肩'] = data_subset['內路肩'].apply(one_hot)
    data_subset['內車道'] = data_subset['內車道'].apply(one_hot)
    data_subset['中內車道'] = data_subset['中內車道'].apply(one_hot)
    data_subset['中車道'] = data_subset['中車道'].apply(one_hot)
    data_subset['中外車道'] = data_subset['中外車道'].apply(one_hot)
    data_subset['外車道'] = data_subset['外車道'].apply(one_hot)
    data_subset['外路肩'] = data_subset['外路肩'].apply(one_hot)
    data_subset['匝道'] = data_subset['匝道'].apply(one_hot)

    # change data type
    data_subset = data_subset.dropna()
    data_need_to_be_int = ['Highway', 'FiveMinuteStart', 'FiveMinuteEnd']
    data_subset['Year'] = data_subset['年'].astype(int)
    data_subset['Month'] = data_subset['月'].astype(int)
    data_subset['Day'] = data_subset['日'].astype(int)
    data_subset['RecoveryMinute'] = data_subset['處理分鐘'].astype(int)
    data_subset['Mileage'] = data_subset['里程']
    for data in data_need_to_be_int:
        data_subset[data] = data_subset[data].astype(int)
    data_subset['Direction'] = data_subset['Direction'].astype(str).str[0]
    data_need_to_be_bool = ['內路肩', '內車道', '中內車道', '中車道', '中外車道', '外車道', '外路肩', '匝道']
    data_subset[data_need_to_be_bool].astype(bool)

    # store data to database
    final_columns = ['Highway', 'Direction', 'Mileage', 'Year', 'Month', 'Day', 'FiveMinuteStart', 'RecoveryMinute', 'FiveMinuteEnd', '內路肩', '內車道', '中內車道', '中車道', '中外車道', '外車道', '外路肩', '匝道']
    # print(data_subset[final_columns].head())
    # print("columns name: ", data_subset.columns.to_list())
    data_subset[final_columns].to_sql('traffic_accident', conn.db, if_exists='replace', index=True)

    conn.db.commit()
    # end of convert_and_store_traffic_accident function


"""
    "convert_and_store_construction_zone" : 
        Process 施工路段資料 data from xlsx to pandas dataframe and store in database
    data file : xlsx
    processed type(not return) : store in database (default name : row.db)
    data information:
        data name : "112 年 1-10 月及 113 年 1-2 月施工路段資料"的分析資料或驗證資料的任意一份資料當作格式範例
        reference data link : https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E9%81%93%E8%B7%AF%E6%96%BD%E5%B7%A5%E8%B7%AF%E6%AE%B5%E8%B3%87%E6%96%99.xlsx
        data source page link : https://freeway2024.tw/links#links
    procedure : 
        1. create construction_zone table
        2. parse xlsx to pandas dataframe
        3. process data
            * extract highway number
            * convert direction to char
            * process time to "start and end" of "year, month, day, five_minute"
            * one hot encoding for lane data
            * handle mileage data (set unit as km)
            * drop those data with missing value (NaN or None)
        4. change data type
        5. pick up certain columns to database
"""
def convert_and_store_construction_zone(store_path, conn):
    """ xlsx construction zone structure
        incStepIncidentId INTEGER,      not need
        incStepNum INTEGER,             not need
        incStepTime TEXT,               process to year, month, day, start_time, end_time
        incStepEndTime TEXT,            process to year, month, day, start_time, end_time
        incStepFreewayId TEXT,          store
        incStepDirection TEXT,          store
        incStepStartMileage REAL,       store
        incStepEndMileage REAL,         store
        incStepBlockagePattern TEXT     process to boolean
    """
    def extract_highway_number (highway_number) :
        if highway_number == 10010 or highway_number == 10019 :
            return int(1)
        elif highway_number == 10020 :
            return int(2)
        elif highway_number == 10030 or highway_number == 10031 :
            return int(3)
        elif highway_number == 10040 :
            return int(4)
        elif highway_number == 10050 :
            return int(5)
        elif highway_number == 10060 :
            return int(6)
        elif highway_number == 10080 :
            return int(8)
        elif highway_number == 10100 :
            return int(10)
        else :
            return None
        # end of extract_highway_number function
    def convert_direction (direction_number) :
        if pd.isnull(direction_number):
            return None
        elif direction_number == 1:
            return 'S'
        elif direction_number == 2:
            return 'N'
        elif direction_number == 3:
            return 'E'
        elif direction_number == 4:
            return 'W'
        else :
            return None
        # end of convert_direction function
    def one_hot (target, control) :
        control = str(control)
        dic = {"內側路肩": 1, "第1車道": 2, "第2車道": 3, "第3車道": 4, "第4車道": 5, "第5車道": 6, "第6車道": 7, "第7車道": 8, "第8車道": 9, "外側路肩": 10, "內邊坡":11, "外邊坡":12}
        if control == "1111111111" or control == "11111111111111111111" :
            return True
        elif control == "2222222222" or control == "22222222222222222222" :
            return None
        elif len(control) != 20 and len(control) != 10:
            return None
        elif target in dic:
            x = lambda control, target : True if control[dic[target]-1] == "1" else False
            return x(control, target)
        else:
            return None
        # end of one_hot function
    def separate_time (target, control) :
        if len(str(control)) < 10 :
            return None
        dt = datetime.strptime(str(control), "%Y-%m-%d %H:%M:%S")
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        minute = dt.minute
        five_minute_of_day = (hour * 60 + minute) // 5
        if "Year" in target:
            return int(year)
        elif "Month" in target:
            return int(month)
        elif "Day" in target:
            return int(day)
        elif "FiveMinute" in target:
            return int(five_minute_of_day)
        else:
            return None
        # end of separate_time function
    def handle_mileage (start_mileage, end_mileage) :
        if pd.isnull(start_mileage) or pd.isnull(end_mileage):
            return None, None
        if start_mileage > end_mileage:
            return None, None
        if start_mileage == 0:
            return end_mileage / 1000, end_mileage / 1000
        if end_mileage == 0:
            return start_mileage / 1000, start_mileage / 1000
        return start_mileage / 1000, end_mileage / 1000
        # end of handle_mileage function
    
    # create table
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS construction_zone (
        ID                 INTEGER PRIMARY KEY AUTOINCREMENT,
        StartYear          INTEGER,
        StartMonth         INTEGER,
        StartDay           INTEGER,
        StartFiveMinute   INTEGER,
        EndYear            INTEGER,
        EndMonth           INTEGER,
        EndDay             INTEGER,
        EndFiveMinute     INTEGER,
        Highway             INTEGER,
        Direction           CHAR(1),
        StartMileage       REAL,
        EndMileage         REAL,
        內側路肩             BOOLEAN,
        第1車道             BOOLEAN,
        第2車道              BOOLEAN,
        第3車道             BOOLEAN,
        第4車道             BOOLEAN,
        第5車道             BOOLEAN,
        第6車道             BOOLEAN,
        第7車道             BOOLEAN,
        第8車道             BOOLEAN,
        外側路肩            BOOLEAN,
        內邊坡              BOOLEAN,
        外邊坡              BOOLEAN
    )
    '''
    conn.db.cursor().execute(create_table_query)
    conn.db.commit()
    # pick up data from xlsx
    pd.options.mode.copy_on_write = True
    data_file = pd.read_excel(store_path)
    selected_columns = [ 'incStepTime', 'incStepEndTime', 'incStepFreewayId', 'incStepDirection', 'incStepStartMileage', 'incStepEndMileage', 'incStepBlockagePattern']
    data_subset = data_file[selected_columns]
    
    # process data
    data_subset['Highway'] = data_subset['incStepFreewayId'].apply(extract_highway_number)
    data_subset['Direction'] = data_subset['incStepDirection'].apply(convert_direction)
    start_time_list = ['StartYear', 'StartMonth', 'StartDay', 'StartFiveMinute']
    end_time_list = ['EndYear', 'EndMonth', 'EndDay', 'EndFiveMinute']
    route_list = ['內側路肩', '第1車道', '第2車道', '第3車道', '第4車道', '第5車道', '第6車道', '第7車道', '第8車道', '外側路肩', '內邊坡', '外邊坡']
    for option in start_time_list :
        data_subset[option] = data_subset.apply(lambda x : separate_time(option, x['incStepTime']), axis=1)
    for option in end_time_list :
        data_subset[option] = data_subset.apply(lambda x : separate_time(option, x['incStepEndTime']), axis=1)
    for route in route_list :
        data_subset[route] = data_subset.apply(lambda x : one_hot(route, x['incStepBlockagePattern']), axis=1)
    data_subset['StartMileage'], data_subset['EndMileage'] = zip(*data_subset.apply(lambda x : handle_mileage(x['incStepStartMileage'], x['incStepEndMileage']), axis=1))
    # 註 : pandas dataframe 這邊用到"多參數用法"與"多回傳值用法"，所以長得特別奇怪

    # change data type
    data_subset = data_subset.dropna()
    data_need_to_be_int = ['StartYear', 'StartMonth', 'StartDay', 'StartFiveMinute', 'EndYear', 'EndMonth', 'EndDay', 'EndFiveMinute', 'Highway']
    data_subset[data_need_to_be_int] = data_subset[data_need_to_be_int].astype(int)
    data_subset['Direction'] = data_subset['Direction'].astype(str).str[0]
    data_need_to_be_bool = route_list
    data_subset[data_need_to_be_bool].astype(bool)
    
    # store data to database
    final_columns = ['StartYear', 'StartMonth', 'StartDay', 'StartFiveMinute', 'EndYear', 'EndMonth', 'EndDay', 'EndFiveMinute', 'Highway', 'Direction', 'StartMileage', 'EndMileage', '內側路肩', '第1車道', '第2車道', '第3車道', '第4車道', '第5車道', '第6車道', '第7車道', '第8車道', '外側路肩', '內邊坡', '外邊坡']
    # print(data_subset[final_columns].head())
    # print("columns name: ", data_subset.columns.to_list())
    data_subset[final_columns].to_sql('construction_zone', conn.db, if_exists='replace', index=True)
    
    conn.db.commit()
    # end of convert_and_store_construction_zone function
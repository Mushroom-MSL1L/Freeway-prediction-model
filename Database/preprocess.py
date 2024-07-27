from .get_data import GetData
from .holiday import modified_holiday_and_date_cosine_processor
import pandas as pd
import datetime

"""
Purpose of the file : 
    Get data from internet. (using GetData class in get_data.py)
    Then preprocess the data and store it in the database.
    Aggregating all data_type as one table. (Preprocess.self.df)

    *** You can get the preprocessed data by just calling get_preprocessed_data() method.
"""

class Preprocess():
    def __init__(self) : 
        # main functions 
        self.get_data = GetData(db_name='row.db')
        self.processed_db = self.get_data.Database
        self.df = pd.DataFrame()
        self.__set_final_table()
        self.car_map = self.__get_car_frequency()
        self.__load_ETagPairLive()
        self.__load_traffic_accident()
        self.__load_construction_zone()
        self.__load_holiday()
        self.store_preprocessed_data()

    def get_preprocessed_data(self) :
        return self.df

    def __set_final_table(self) :
        create_table_query = '''
CREATE TABLE IF NOT EXISTS preprocessed_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction char(1),
    location TEXT,
    car INTEGER,
    speed FLOAT,

    month FLOAT,
    day FLOAT,
    date FLOAT,
    time FLOAT,

    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday FLOAT,

    has_accident BOOLEAN,
    recovery_time INTEGER,
    內路肩 BOOLEAN,
    內車道 BOOLEAN,
    中內車道 BOOLEAN,
    中車道 BOOLEAN,
    中外車道    BOOLEAN,
    外車道  BOOLEAN,
    外路肩  BOOLEAN,
    匝道    BOOLEAN,

    has_construction BOOLEAN,
    construction_time INTEGER,
    第一車道 BOOLEAN,
    第二車道 BOOLEAN,
    第三車道 BOOLEAN,
    第四車道 BOOLEAN,
    第五車道 BOOLEAN,
    第六車道 BOOLEAN,
    第七車道 BOOLEAN,
    第八車道 BOOLEAN,
    外側路肩 BOOLEAN,
    內邊坡 BOOLEAN,
    外邊坡 BOOLEAN
)
'''
        self.processed_db.cursor.execute(create_table_query)
        self.processed_db.db.commit()
        print("\tFinal table created")
        # end of __set_final_table function




    """
    "__get_car_frequency" : 
            Calculate the frequency of certain car type from ETagPairLive of 2023 (train data)
            And store the frequency in the database
            And store the frequency as member dictionary variable of preprocess class
        procedure : 
            1. count the car numbers of each type in database
                * only in 2023 (training data)
                * car type : 31, 32, 41, 42, 5
                * only in ETagPairLive table
            2. calculate the frequency of each car type and store frequency
            3. aggregate the frequency of small car and big car
                * 0 : small car (31, 32)
                * 1 : big car (41, 42)
                * 2 : other (5)
            4. store the frequency in the database
            5. return the frequency as Preprocess.car_map dictionary
    """
    def __get_car_frequency(self) :
        # get the car numbers from the database
        # only in 2023, 102
        count_car_number_query = '''
            SELECT VehicleType, SUM(VehicleCount)
            FROM ETagPairLive
            WHERE Year = 2023
            GROUP BY VehicleType
        '''
        car_code_needed = [31, 32, 41, 42, 5] # 31小客車 32小貨車 41大客車 42大貨車 5聯結車
        car_amount = self.processed_db.cursor.execute(count_car_number_query).fetchall()

        total_car = sum([car[1] for car in car_amount])
        car_type_map = {31: 0, 32: 0, 41: 1, 42: 1, 5: 2} # 0小型車 1:大型車 2:其他
        car_encode_map = {31: 0, 32: 0, 41: 0, 42: 0, 5: 0}
        car_frequency = [0, 0, 0]
        for car in car_amount :
            if car[0] in car_code_needed :
                car_frequency[car_type_map[car[0]]] += car[1] / total_car
        for car in car_amount :
            if car[0] in car_code_needed :
                car_encode_map[car[0]] = round(car_frequency[car_type_map[car[0]]], 3)
        print("\tCar frequency fetched")

        # create the table
        delete_query = '''DROP TABLE IF EXISTS CarFrequency'''
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS CarFrequency (
                CarCode INTEGER,
                Frequency FLOAT
            )
        '''
        self.processed_db.cursor.execute(delete_query)
        self.processed_db.cursor.execute(create_table_query)
        
        for car_code in car_code_needed :
            self.processed_db.cursor.execute('''
                INSERT OR REPLACE INTO CarFrequency (CarCode, Frequency) VALUES (?, ?)''', 
                (car_code, car_encode_map[car_code])
            )
        
        return car_encode_map
        # end of __get_car_frequency function



    """
    "__load_ETagPairLive" :
            Load the ETagPairLive data from the database
            And preprocess the data
            And store the preprocessed data as member variable of preprocess class
        procedure : 
            1. load the ETagPairLive data from the database
            2. preprocess the data
                * change the vehicle type to the frequency
                * aggregate the same vehicle type
                    * sum the vehicle count
                    * average the space mean speed
            3. aggregate the same vehicle type
            4. store the preprocessed data as member variable Preprocess.df
    """
    def __load_ETagPairLive(self) :
        print("\tETagPairLive is preprocessing.")
        load_ETagePair_query = '''
            SELECT *
            FROM ETagPairLive
        '''
        df = pd.read_sql_query(load_ETagePair_query, self.processed_db.db)
        # change vehicle type to the frequency
        df['VehicleType'] = df['VehicleType'].apply(lambda x : self.car_map[x])
        # aggregate same vehicle type
        df = df.groupby([
            'ETagPairID', 'Highway', 'StartMileage', 'EndMileage', 'Direction', 'Year', 'Month', 'Day', 'FiveMinute', 'VehicleType'
        ]).agg({
            'SpaceMeanSpeed': 'mean',
            'VehicleCount': 'sum'
        }).reset_index()

        print("\tETagPairLive is preprocessed and loaded.")
        self.df = df
        # end of __load_ETagPairLive function




    # not sure the correctness
    """
    "__load_traffic_accident" :
            Load the traffic_accident data from the database
            And preprocess the data
            And store the preprocessed data as member variable of preprocess class
        procedure : 
            1. store the ETagPairLive df variable as temporary table
            2. JOIN the ETagPairLive and traffic_accident in db
                * highway, direction should be same
                * accident mileage should be within the ETagPairLive mileage
                * accident time should overlap with the ETagPairLive time
                    * if same day, the accident time should be within the ETagPairLive time
                    * if cross a day, ETag in previous day
                    * if cross a day, ETag in next day
            3. delete the temporary table
            4. fill the NaN value with 0
            5. preprocess the data
                * add the is_accident column
            6. change the columns to boolean type
            7. store the preprocessed data as member variable Preprocess.df
    """
    def __load_traffic_accident(self) :
        print("\tTrafficAccident is preprocessing.")
        def add_is_accident(row) :
            if row['RecoveryMinute'] == 0 :
                return False
            return True
        # traffic_accident direction in middle of ETag
        # ETag time in middle of traffic_accident
        # three cases
        ## same day
        ## cross a day and in the previous day
        ## cross a day and in the next day
        JOIN_query = '''
SELECT 
    ETagPairLive_temp.ETagPairID, 
    ETagPairLive_temp.Highway, 
    ETagPairLive_temp.StartMileage, 
    ETagPairLive_temp.EndMileage, 
    ETagPairLive_temp.Direction, 
    ETagPairLive_temp.Year, 
    ETagPairLive_temp.Month, 
    ETagPairLive_temp.Day, 
    ETagPairLive_temp.FiveMinute, 
    ETagPairLive_temp.VehicleType, 
    ETagPairLive_temp.SpaceMeanSpeed, 
    ETagPairLive_temp.VehicleCount,
    traffic_accident.RecoveryMinute,
    traffic_accident.內路肩,
    traffic_accident.內車道,
    traffic_accident.中內車道, 
    traffic_accident.中車道,
    traffic_accident.中外車道,
    traffic_accident.外車道,
    traffic_accident.外路肩,
    traffic_accident.匝道,

    traffic_accident.Year AS accident_Year,
    traffic_accident.Month AS accident_Month,
    traffic_accident.Day AS accident_Day,
    traffic_accident.FiveMinuteStart,
    traffic_accident.FiveMinuteEnd


FROM ETagPairLive_temp
LEFT JOIN traffic_accident
ON ETagPairLive_temp.Highway = traffic_accident.Highway
AND ETagPairLive_temp.Direction = traffic_accident.Direction
AND traffic_accident.Mileage BETWEEN ETagPairLive_temp.StartMileage AND ETagPairLive_temp.EndMileage
AND ETagPairLive_temp.Year = traffic_accident.Year
AND ETagPairLive_temp.Month = traffic_accident.Month
AND ETagPairLive_temp.Day = traffic_accident.Day
AND (
    (
        ETagPairLive_temp.Year = traffic_accident.Year
        AND ETagPairLive_temp.Month = traffic_accident.Month
        AND ETagPairLive_temp.Day = traffic_accident.Day
        AND ETagPairLive_temp.FiveMinute BETWEEN traffic_accident.FiveMinuteStart AND traffic_accident.FiveMinuteEnd
    )
    OR 
    (
        ETagPairLive_temp.Year = traffic_accident.Year
        AND ETagPairLive_temp.Month = traffic_accident.Month
        AND ETagPairLive_temp.Day = traffic_accident.Day
        AND traffic_accident.RecoveryMinute IS NOT NULL
        AND ETagPairLive_temp.FiveMinute BETWEEN traffic_accident.FiveMinuteStart AND (traffic_accident.FiveMinuteStart + traffic_accident.RecoveryMinute / 5)
    )
    OR
    (
        ETagPairLive_temp.Year = traffic_accident.Year
        AND ETagPairLive_temp.Month = traffic_accident.Month
        AND ETagPairLive_temp.Day = traffic_accident.Day + 1
        AND traffic_accident.RecoveryMinute IS NOT NULL
        AND ETagPairLive_temp.FiveMinute <= (traffic_accident.FiveMinuteStart + traffic_accident.RecoveryMinute / 5 - 288)
    )
)
'''
        # join the ETagPairLive and traffic_accident
        self.df.to_sql('ETagPairLive_temp', self.processed_db.db, if_exists='replace', index=True)
        self.df = pd.read_sql_query(JOIN_query, self.processed_db.db)
        self.processed_db.db.execute(f"DROP TABLE IF EXISTS ETagPairLive_temp")
        pd.set_option("future.no_silent_downcasting", True)
        self.df = self.df.fillna(0)

        # preprocess and store the data
        self.df['is_accident'] = self.df.apply(add_is_accident, axis=1)
        columns_need_to_be_boolean = ['內路肩', '內車道', '中內車道', '中車道', '中外車道', '外車道', '外路肩', '匝道']
        self.df[columns_need_to_be_boolean] = self.df[columns_need_to_be_boolean].astype(bool)
        self.df['RecoveryMinute'] = self.df['RecoveryMinute'].astype(int)

        # print the result
        # pd.set_option('display.max_rows', None)
        # pd.set_option('display.max_columns', None)
        # result = self.df.query('is_accident == True')
        # print("result", result.head(100))
        print("\tTrafficAccident is preprocessed and loaded.")
        # end of __load_traffic_accident function



    # not sure the correctness
    """
    "__load_construction_zone" :
            Load the construction_zone data from the database
            And preprocess the data
            And store the preprocessed data as member variable of preprocess class
        procedure : 
            1. load the construction_zone data from the database
            2. transform the time of "construction" and "ETag" to UTC seconds which is comparable
            3. store the "ETagPairLive df variable" and "construction" as temporary table
            4. JOIN the ETagPairLive and construction_zone in db
                * highway, direction should be same
                * ETag time should be within the construction time
                * ETag location should have overlap to the construction location
            5. delete the temporary table
            6. fill the NaN value with 0
            7. preprocess the data
                * add the is_construction column
            8. store the preprocessed data as member variable Preprocess.df 
            9. print the result
    """
    def __load_construction_zone(self) :
        print("\tConstruction zone is preprocessing.")
        def convert_to_sec(year, month, day, five_minute) :
            if year == 0 and month == 0 and day == 0 and five_minute == 0 :
                return 0
            time = datetime.datetime(int(year), int(month), int(day), int(five_minute) // 12, (int(five_minute) % 12) * 5)
            return time.timestamp()
        def add_is_construction(time) :
            if time == 0 :
                return False
            return True
        JOIN_query = '''
SELECT 
    ETagPairLive_temp.ETagPairID, 
    ETagPairLive_temp.Highway, 
    ETagPairLive_temp.StartMileage, 
    ETagPairLive_temp.EndMileage, 
    ETagPairLive_temp.Direction, 
    ETagPairLive_temp.Year, 
    ETagPairLive_temp.Month, 
    ETagPairLive_temp.Day, 
    ETagPairLive_temp.FiveMinute, 
    ETagPairLive_temp.VehicleType, 
    ETagPairLive_temp.SpaceMeanSpeed, 
    ETagPairLive_temp.VehicleCount,

    ETagPairLive_temp.time,
    construction_zone_temp.StartTime,
    construction_zone_temp.EndTime,

    ETagPairLive_temp.is_accident,
    ETagPairLive_temp.RecoveryMinute,
    ETagPairLive_temp.內路肩 AS traffic_accident_內路肩,
    ETagPairLive_temp.內車道 AS traffic_accident_內車道,
    ETagPairLive_temp.中內車道 AS traffic_accident_中內車道,
    ETagPairLive_temp.中車道 AS traffic_accident_中車道,
    ETagPairLive_temp.中外車道 AS traffic_accident_中外車道,
    ETagPairLive_temp.外車道 AS traffic_accident_外車道,
    ETagPairLive_temp.外路肩 AS traffic_accident_外路肩,
    ETagPairLive_temp.匝道 AS traffic_accident_匝道,

    construction_zone_temp.ConstructionMinute,
    construction_zone_temp.內側路肩 AS construction_內側路肩,
    construction_zone_temp.第1車道, 
    construction_zone_temp.第2車道, 
    construction_zone_temp.第3車道, 
    construction_zone_temp.第4車道, 
    construction_zone_temp.第5車道, 
    construction_zone_temp.第6車道, 
    construction_zone_temp.第7車道, 
    construction_zone_temp.第8車道, 
    construction_zone_temp.外側路肩 AS construction_外側路肩,
    construction_zone_temp.內邊坡 AS construction_內邊坡,
    construction_zone_temp.外邊坡 AS construction_外邊坡
FROM ETagPairLive_temp
LEFT JOIN construction_zone_temp
ON ETagPairLive_temp.Highway = construction_zone_temp.Highway
AND ETagPairLive_temp.Direction = construction_zone_temp.Direction
AND (
    (ETagPairLive_temp.StartMileage BETWEEN construction_zone_temp.StartMileage AND construction_zone_temp.EndMileage)
    OR
    (ETagPairLive_temp.EndMileage BETWEEN construction_zone_temp.StartMileage AND construction_zone_temp.EndMileage)
)
AND ETagPairLive_temp.time BETWEEN construction_zone_temp.StartTime AND construction_zone_temp.EndTime
'''
        
        # process time to comparable format
        construction_df = pd.read_sql_query("SELECT * FROM construction_zone", self.processed_db.db)
        construction_df['StartTime'] = construction_df.apply(lambda row: convert_to_sec(row['StartYear'], row['StartMonth'], row['StartDay'], row['StartFiveMinute']), axis=1)
        construction_df['EndTime'] = construction_df.apply(lambda row: convert_to_sec(row['EndYear'], row['EndMonth'], row['EndDay'], row['EndFiveMinute']), axis=1)
        construction_df['ConstructionMinute'] = construction_df.apply(lambda row: int((row['EndTime'] - row['StartTime'])//60), axis=1)
        self.df['time'] = self.df.apply(lambda row: convert_to_sec(row['Year'], row['Month'], row['Day'], row['FiveMinute']), axis=1)

        # join the ETagPairLive and construction_zone
        self.df.to_sql('ETagPairLive_temp', self.processed_db.db, if_exists='replace', index=True)
        construction_df.to_sql('construction_zone_temp', self.processed_db.db, if_exists='replace', index=True)
        self.df = pd.read_sql_query(JOIN_query, self.processed_db.db) 

        self.processed_db.db.execute(f"DROP TABLE IF EXISTS ETagPairLive_temp")
        self.processed_db.db.execute(f"DROP TABLE IF EXISTS construction_zone_temp")
        pd.set_option("future.no_silent_downcasting", True)
        self.df = self.df.fillna(0)

        # preprocess and store the data
        self.df['is_construction'] = self.df['ConstructionMinute'].apply(add_is_construction)

        # print the result
        # pd.set_option('display.max_rows', None)
        # pd.set_option('display.max_columns', None)
        # result = self.df.query('is_construction == True')
        # print("result", result.head(100))
        print("\tConstruction zone is preprocessed and loaded.")
        # end of __load_construction_zone function

    def __load_holiday(self) :
        print("\tHoliday is preprocessing.")
        # print("columns : ", self.df.columns.to_list())
        self.df['is_weekend'], self.df['is_holiday'], self.df['Holiday'], self.df['Month'], self.df['Day'], self.df['FiveMinute'] = zip(*self.df.apply(lambda row : modified_holiday_and_date_cosine_processor( row['Year'], row['Month'], row['Day'], row['FiveMinute']), axis=1))
        
        # print(self.df.head(10))
        print("\tHoliday is preprocessed and loaded.")
        # end of __load_holiday function

    def store_preprocessed_data(self) :
        self.df.to_sql('preprocessed_data', self.processed_db.db, if_exists='replace', index=True)
        print("\tPreprocessed data stored.")
        # end of store_preprocessed_data function
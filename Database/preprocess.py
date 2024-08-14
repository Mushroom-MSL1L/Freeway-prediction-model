from .get_data import GetData
from .holiday import modified_holiday_and_date_cosine_processor, modified_holiday_and_date_sine_processor
import modin.pandas as mpd
import pandas as pd
from tqdm import tqdm as tqdm
import dask
dask.config.set({'logging.distributed': 'error'})


"""
Purpose of the file : 
    Get data from internet. (using GetData class in get_data.py)
    Then preprocess the data and store it in the database.
    Aggregating all data_type as one table. (Preprocess.self.df)

    *** You can get the preprocessed data by just calling get_preprocessed_data() method.
"""

class Preprocess():
    def __init__(self, segment_id_needed, car_code_needed, already_fetched=False, already_preprocessed=False) : 
        # main functions 
        self.segment_id_needed = self.set_segment_id_needed(segment_id_needed)
        self.car_code_needed = self.set_car_code_needed(car_code_needed)
        self.db_name = 'row.db'
        self.df = mpd.DataFrame()
        self.get_data = GetData(db_name=self.db_name, car_code_needed=self.car_code_needed, segment_id_needed=self.segment_id_needed, already_fetched=already_fetched)
        self.processed_db = self.get_data.Database
        self.car_map = self.__get_car_frequency(already_preprocessed)
        self.__preprocess_all_data(already_preprocessed)

    def get_preprocessed_data(self) :
        query = '''
            SELECT *
            FROM preprocessed_data
        '''
        self.df = self.query_in_batches(query, self.processed_db.db)
        return self.df

    def get_car_map(self) :
        return self.car_map

    def batch_to_sql(self, df, table_name, conn, chunksize=10000):
        for start in range(0, len(df), chunksize):
            end = min(start + chunksize, len(df))
            df_chunk = df.iloc[start:end]
            df_chunk = df_chunk._to_pandas()
            df_chunk.to_sql(table_name, conn, if_exists='append', index=False)

    def query_in_batches(self, query, conn, batch_size=10000):
        offset = 0
        batch_dfs = []
        while True:
            batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
            batch_df = pd.read_sql_query(batch_query, conn)
            if batch_df.empty:
                break
            batch_modin_df = mpd.DataFrame(batch_df)
            
            batch_dfs.append(batch_modin_df)
            offset += batch_size
        if batch_dfs:
            full_modin_df = mpd.concat(batch_dfs, ignore_index=True)
        else:
            full_modin_df = mpd.DataFrame() 
        return full_modin_df

    def set_segment_id_needed(self, segment_id_needed) :
        keys = ['ID', 'from', 'to']
        if type(segment_id_needed) != list : 
            raise ValueError("segment_id_needed should be a list of dictionary")
        for segment in segment_id_needed :
            if type(segment) != dict :
                raise ValueError("segment_id_needed element should be a dictionary")
            for key in keys :
                if key not in segment :
                    raise ValueError(f"segment_id_needed should have key : {key}")
        return segment_id_needed

    def set_car_code_needed(self, car_code_needed) :
        if type(car_code_needed) != list :
            raise ValueError("car_code_needed should be a list")
        if len(car_code_needed) <= 0 :
            raise ValueError("car_code_needed should have more than 0 elements")
        return car_code_needed

    def __preprocess_all_data(self, already_preprocessed = False) :
        def get_UTC_range(year) :
            query = '''
                SELECT MIN(UTC) as min, MAX(UTC) as max
                FROM ETagPairLive
                WHERE Year = ?
            '''
            result = self.processed_db.db.execute(query, (year,)).fetchone()
            min_value, max_value = result
            return min_value, max_value
        # check if the preprocessed data already exists
        if already_preprocessed:
            try:
                test_query = '''
                    SELECT COUNT(*)
                    FROM preprocessed_data
                '''
                preprocessed_data_query = '''
                    SELECT *
                    FROM preprocessed_data
                '''
                if self.processed_db.db.execute(test_query).fetchone()[0] == 0 or self.processed_db.db.execute(preprocessed_data_query).fetchone() == None:
                    raise ValueError("Preprocessed data not found")
                all_df = mpd.DataFrame()
                all_df = self.query_in_batches(preprocessed_data_query, self.processed_db.db)
                self.df = all_df
                print("\tPreprocessed data already fetched, skip preprocessing.")
                return
            except:
                raise ValueError("Preprocessed data not found")
        # begin of preprocessing
        show_progress = False
        
        for _, segment in enumerate(tqdm(self.segment_id_needed, desc='Preprocessing all segment data……')) :
            time_width = 86400 * 10 # 每次處理10天的資料
            for year in range(2023, 2024 + 1) :
                min_UTC, max_UTC = get_UTC_range(year)
                if min_UTC == None or max_UTC == None :
                    continue
                run_times = (max_UTC - min_UTC) // time_width + 1
                current_min_UTC = min_UTC
                current_max_UTC = min(min_UTC + time_width, max_UTC)
                for _, _ in enumerate(tqdm(range(run_times), desc=f'Preprocessing segment data {segment["ID"]} in {year} ')) :
                    temp_df = mpd.DataFrame()
                    temp_df = self.__load_ETagPairLive(segment['ID'], show_progress, current_min_UTC, current_max_UTC)
                    temp_df = self.__load_traffic_accident(segment['ID'], temp_df, show_progress)
                    temp_df = self.__load_construction_zone(segment['ID'], temp_df, show_progress)
                    temp_df = self.__load_holiday(segment['ID'], temp_df, show_progress)
                    self.store_preprocessed_data(segment['ID'], temp_df)
                    current_min_UTC = current_max_UTC + 60 * 5 # 加上5分鐘
                    current_max_UTC = min(current_min_UTC + time_width, max_UTC)
        print("\tPreprocessed data stored.")

### warning : here need to specify the car_code_needed and car groups
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
    def __get_car_frequency(self, already_preprocessed = False) :
        if already_preprocessed:
            try: 
                car_encode_map = {}
                car_frequency_query = '''
                    SELECT CarCode, Frequency
                    FROM CarFrequency
                '''
                car_frequency = self.query_in_batches(car_frequency_query, self.processed_db.db)
                for index, row in car_frequency.iterrows():
                    car_encode_map[int(row['CarCode'])] = float(row['Frequency'])
                print("\tCar frequency already fetched, skip fetching from database.")
                return car_encode_map
            except:
                raise ValueError("Car frequency not found")
        
        # get the car numbers from the database
        # only in 2023, 102
        count_car_number_query = '''
            SELECT VehicleType, SUM(VehicleCount) as TotalCar
            FROM ETagPairLive
            WHERE Year = 2023
            GROUP BY VehicleType
        '''

        car_amount = self.query_in_batches(count_car_number_query, self.processed_db.db)
        car_code_needed = self.car_code_needed
        total_car = car_amount['TotalCar'].sum()
        car_type_map = {31: 0, 32: 0, 41: 1, 42: 1, 5: 2}  # 0: 小型車, 1: 大型車, 2: 其他
        car_encode_map = {31: 0, 32: 0, 41: 0, 42: 0, 5: 0}
        car_frequency = [0, 0, 0]

        # Calculate frequency
        for index, row in car_amount.iterrows():
            car_code = row['VehicleType'] 
            car_count = row['TotalCar']
            if car_code in car_code_needed:
                car_frequency[car_type_map[car_code]] += car_count / total_car

        # Encode the frequency
        for index, row in car_amount.iterrows():
            car_code = row['VehicleType']
            if car_code in car_code_needed:
                car_encode_map[int(car_code)] = round(float(car_frequency[car_type_map[car_code]]), 3)
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
    def __load_ETagPairLive(self, segment_id, show_progress=True, min_UTC=0, max_UTC=0) :
        # exception handling
        if show_progress:
            print("\tETagPairLive is preprocessing.")
        if min_UTC == 0 or max_UTC == 0:
            raise ValueError("min_UTC and max_UTC should be specified")
        
        # load the ETagPairLive data
        load_ETagePair_query = f'''
            SELECT *
            FROM ETagPairLive
            WHERE ETagPairID = '{segment_id}'
            AND UTC BETWEEN {min_UTC} AND {max_UTC}
        '''
        def map_vehicle_type(row, car_map):
            return car_map[row['VehicleType']]
        
        df = self.query_in_batches(load_ETagePair_query, self.processed_db.db)
        # change vehicle type to the frequency
        df['VehicleType'] = df.apply(map_vehicle_type, car_map=self.car_map, axis=1)

        # aggregate same vehicle type
        df = df.groupby([
            'ETagPairID', 'Highway', 'StartMileage', 'EndMileage', 'Direction', 'UTC', 'Year', 'Month', 'Day', 'FiveMinute', 'Weekday', 'VehicleType'
        ]).agg({
            'SpaceMeanSpeed': 'mean',
            'VehicleCount': 'sum'
        }).reset_index()

        if show_progress:
            print("\tETagPairLive is preprocessed and loaded.")
        return df
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
    def __load_traffic_accident(self, segment_id, temp_df, show_progress=True) :
        if show_progress:
            print("\tTrafficAccident is preprocessing.")
        JOIN_query = '''
SELECT 
    ETagPairLive_temp.ETagPairID, 
    ETagPairLive_temp.Highway, 
    ETagPairLive_temp.StartMileage, 
    ETagPairLive_temp.EndMileage, 
    ETagPairLive_temp.Direction, 
    ETagPairLive_temp.UTC,
    ETagPairLive_temp.Year, 
    ETagPairLive_temp.Month, 
    ETagPairLive_temp.Day, 
    ETagPairLive_temp.Weekday,
    ETagPairLive_temp.FiveMinute, 
    ETagPairLive_temp.VehicleType, 
    ETagPairLive_temp.SpaceMeanSpeed, 
    ETagPairLive_temp.VehicleCount,

    traffic_accident.StartUTC AS accident_StartUTC,
    traffic_accident.EndUTC AS accident_EndUTC,
    traffic_accident.Year AS accident_Year,
    traffic_accident.Month AS accident_Month,
    traffic_accident.Day AS accident_Day,
    traffic_accident.Highway AS accident_Highway,
    traffic_accident.Direction AS accident_Direction,
    traffic_accident.Mileage AS accident_Mileage,

    traffic_accident.RecoveryMinute,
    traffic_accident.內路肩,
    traffic_accident.內車道,
    traffic_accident.中內車道, 
    traffic_accident.中車道,
    traffic_accident.中外車道,
    traffic_accident.外車道,
    traffic_accident.外路肩,
    traffic_accident.匝道

FROM ETagPairLive_temp
LEFT JOIN traffic_accident
ON ETagPairLive_temp.Highway = traffic_accident.Highway
AND ETagPairLive_temp.Direction = traffic_accident.Direction
AND (
    (traffic_accident.Mileage BETWEEN ETagPairLive_temp.StartMileage AND ETagPairLive_temp.EndMileage)
    OR
    (traffic_accident.Mileage BETWEEN ETagPairLive_temp.EndMileage AND ETagPairLive_temp.StartMileage)
)
AND ETagPairLive_temp.Year = traffic_accident.Year
AND ETagPairLive_temp.UTC BETWEEN traffic_accident.StartUTC AND traffic_accident.EndUTC
'''
        # join the ETagPairLive and traffic_accident
        self.batch_to_sql(temp_df, 'ETagPairLive_temp', self.processed_db.db)
        temp_df = self.query_in_batches(JOIN_query, self.processed_db.db)
        self.processed_db.db.execute(f"DROP TABLE IF EXISTS ETagPairLive_temp") 
        mpd.set_option("future.no_silent_downcasting", True)
        temp_df = temp_df.fillna(0)

        # preprocess and store the data
        temp_df['is_accident'] = temp_df['RecoveryMinute'] != 0
        if show_progress:
            print("\tTrafficAccident is preprocessed and loaded.")
        return temp_df
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
    def __load_construction_zone(self, segment_id, temp_df, show_progress=True) :
        if show_progress:
            print("\tConstruction zone is preprocessing.")
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
    ETagPairLive_temp.UTC,
    ETagPairLive_temp.Year, 
    ETagPairLive_temp.Month, 
    ETagPairLive_temp.Day, 
    ETagPairLive_temp.Weekday,
    ETagPairLive_temp.FiveMinute, 
    ETagPairLive_temp.VehicleType, 
    ETagPairLive_temp.SpaceMeanSpeed, 
    ETagPairLive_temp.VehicleCount,

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

    construction_zone.ConstructionMinute,
    construction_zone.內側路肩 AS construction_內側路肩,
    construction_zone.第1車道, 
    construction_zone.第2車道, 
    construction_zone.第3車道, 
    construction_zone.第4車道, 
    construction_zone.第5車道, 
    construction_zone.第6車道, 
    construction_zone.第7車道, 
    construction_zone.第8車道, 
    construction_zone.外側路肩 AS construction_外側路肩,
    construction_zone.內邊坡 AS construction_內邊坡,
    construction_zone.外邊坡 AS construction_外邊坡
FROM ETagPairLive_temp
LEFT JOIN construction_zone
ON ETagPairLive_temp.Highway = construction_zone.Highway
AND ETagPairLive_temp.Direction = construction_zone.Direction
AND ETagPairLive_temp.Year = construction_zone.StartYear
AND ETagPairLive_temp.UTC BETWEEN construction_zone.StartUTC AND construction_zone.EndUTC
AND (
    NOT (
        MAX(ETagPairLive_temp.StartMileage, ETagPairLive_temp.EndMileage) < MIN(construction_zone.StartMileage, construction_zone.EndMileage) 
        OR 
        MAX(construction_zone.StartMileage, construction_zone.EndMileage) < MIN(ETagPairLive_temp.StartMileage, ETagPairLive_temp.EndMileage)
    )
)
'''
        # join the ETagPairLive and construction_zone
        self.batch_to_sql(temp_df, 'ETagPairLive_temp', self.processed_db.db)
        temp_df = self.query_in_batches(JOIN_query, self.processed_db.db)

        self.processed_db.db.execute(f"DROP TABLE IF EXISTS ETagPairLive_temp")
        mpd.set_option("future.no_silent_downcasting", True)
        temp_df = temp_df.fillna(0)

        # preprocess and store the data
        temp_df['is_construction'] = temp_df['ConstructionMinute'].apply(add_is_construction)

        if show_progress:
            print("\tConstruction zone is preprocessed and loaded.")
        return temp_df
        # end of __load_construction_zone function


    def __load_holiday(self, segment_id, temp_df, show_progress=True) :
        if show_progress:
            print("\tHoliday is preprocessing.")
        temp_df['is_weekend'], temp_df['is_holiday'], temp_df['holiday_cos'], temp_df['month_cos'], temp_df['day_cos'], temp_df['five_minute_cos'], temp_df['weekday_cos'] \
            = zip(*temp_df.apply(lambda row : modified_holiday_and_date_cosine_processor( row['Year'], row['Month'], row['Day'], row['FiveMinute'], row['Weekday']), axis=1))
        temp_df['is_weekend'], temp_df['is_holiday'], temp_df['holiday_sin'], temp_df['month_sin'], temp_df['day_sin'], temp_df['five_minute_sin'], temp_df['weekday_sin'] \
            = zip(*temp_df.apply(lambda row : modified_holiday_and_date_sine_processor( row['Year'], row['Month'], row['Day'], row['FiveMinute'], row['Weekday']), axis=1))
        
        if show_progress:
            print("\tHoliday is preprocessed and loaded.")
        return temp_df
        # end of __load_holiday function

    def store_preprocessed_data(self, segment_id, temp_df) :
        create_table_query = '''
CREATE TABLE IF NOT EXISTS preprocessed_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ETagPairID TEXT,
    direction char(1),
    highway INTEGER,
    start_mileage FLOAT,
    end_mileage FLOAT,
    year INTEGER,

    car INTEGER,
    speed FLOAT,

    utc INTEGER,

    month_sin FLOAT,
    month_cos FLOAT,
    day_sin FLOAT,
    day_cos FLOAT,
    five_minute_sin FLOAT,
    five_minute_cos FLOAT,
    weekday_sin FLOAT,
    weekday_cos FLOAT,

    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_sin FLOAT,
    holiday_cos FLOAT,

    has_accident BOOLEAN,
    recovery_time INTEGER,
    traffic_accident_內路肩 BOOLEAN,
    traffic_accident_內車道 BOOLEAN,
    traffic_accident_中內車道 BOOLEAN,
    traffic_accident_中車道 BOOLEAN,
    traffic_accident_中外車道 BOOLEAN,
    traffic_accident_外車道 BOOLEAN,
    traffic_accident_外路肩 BOOLEAN,
    traffic_accident_匝道 BOOLEAN,

    has_construction BOOLEAN,
    construction_time INTEGER,
    construction_第一車道 BOOLEAN,
    construction_第二車道 BOOLEAN,
    construction_第三車道 BOOLEAN,
    construction_第四車道 BOOLEAN,
    construction_第五車道 BOOLEAN,
    construction_第六車道 BOOLEAN,
    construction_第七車道 BOOLEAN,
    construction_第八車道 BOOLEAN,
    construction_外側路肩 BOOLEAN,
    construction_內邊坡 BOOLEAN,
    construction_外邊坡 BOOLEAN
)
'''
        self.processed_db.cursor.execute(create_table_query)
        self.processed_db.db.commit()

        column_needed = [ 'UTC',
            'ETagPairID', 'Direction', 'Highway', 'StartMileage', 'EndMileage', 'VehicleType', 'SpaceMeanSpeed', 
            'Year', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'five_minute_sin', 'five_minute_cos', 'weekday_sin', 'weekday_cos',
            'is_weekend', 'is_holiday', 'holiday_sin', 'holiday_cos',
            'is_accident', 'RecoveryMinute', 'traffic_accident_內路肩', 'traffic_accident_內車道', 'traffic_accident_中內車道', 'traffic_accident_中車道', 'traffic_accident_中外車道', 'traffic_accident_外車道', 'traffic_accident_外路肩', 'traffic_accident_匝道',
            'is_construction', 'ConstructionMinute', '第1車道', '第2車道', '第3車道', '第4車道', '第5車道', '第6車道', '第7車道', '第8車道', 'construction_外側路肩', 'construction_內邊坡', 'construction_外邊坡'
        ]
        rename_dict = {
            'Direction': 'direction',
            'Highway': 'highway',
            'StartMileage': 'start_mileage',
            'EndMileage': 'end_mileage',

            'Year': 'year',
            'VehicleType': 'car',
            'SpaceMeanSpeed': 'speed',

            'is_accident': 'has_accident',
            'RecoveryMinute': 'recovery_time',

            'is_construction': 'has_construction',
            'ConstructionMinute': 'construction_time',
            '第1車道': 'construction_第一車道',
            '第2車道': 'construction_第二車道',
            '第3車道': 'construction_第三車道',
            '第4車道': 'construction_第四車道',
            '第5車道': 'construction_第五車道',
            '第6車道': 'construction_第六車道',
            '第7車道': 'construction_第七車道',
            '第8車道': 'construction_第八車道',
        }
        temp_df = temp_df[column_needed]
        temp_df = temp_df.rename(columns=rename_dict)
        self.batch_to_sql(temp_df, 'preprocessed_data', self.processed_db.db)
        # end of store_preprocessed_data function
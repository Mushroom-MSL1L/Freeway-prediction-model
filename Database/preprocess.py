from .get_data import GetData
from .db import database

"""
Get data from internet.
Then preprocess the data aand store it in the database.
"""

class Preprocess():
    def __init__(self) : 
        self.get_data = GetData(db_name='row.db')
        # self.processed_db = database(file_name='preprocessed.db')
        self.processed_db = self.get_data.Database
        self.__set_final_table()
        self.car_map = self.__get_car_frequency()
        # self.__get_work_day()
        # self.__load_ETagPairLive()
        # self.__load_traffic_accident()
        # self.__load_construction_zone()

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

        # attach the databases 
        # row_db_name = self.get_data.get_db_name()
        # attach_db_query = f"ATTACH DATABASE '{row_db_name}' AS db2"
        # self.processed_db.cursor.execute(attach_db_query)

        self.processed_db.db.commit()
        print("\tTable created")

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
        return car_encode_map
        
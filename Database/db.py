import sqlite3
import os

# https://docs.python.org/zh-tw/3/library/sqlite3.html
class database:
    def __init__(self, file_name):
        self.file_name = file_name
        self.db = self.__connect()
        self.cursor = self.db.cursor()
        self.__setup_table()

    def __connect(self):
        try:
            connection = sqlite3.connect(self.__get_path('assets/' + self.file_name))
        except:
            raise EnvironmentError("Failed to connect to the database")
        print("\tConnected to the database")
        return connection
    
    def __setup_table(self):
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
        self.cursor.execute(create_table_query)
        self.db.commit()
        print("\tTable created")

    def disconnect(self):
        self.db.close()
        print("Disconnected to the database")
    
    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path
import sqlite3
import os

# https://docs.python.org/zh-tw/3/library/sqlite3.html

"""
Basic database class for sqlite3
"""
class database:
    def __init__(self, file_name):
        self.file_name = file_name
        self.db = self.__connect()
        self.cursor = self.db.cursor()

    def get_db_name(self):
        return self.file_name

    def __connect(self):
        try:
            connection = sqlite3.connect(self.__get_path('assets/' + self.file_name))
        except:
            raise EnvironmentError("Failed to connect to the database")
        print("\tConnected to the database")
        return connection

    def disconnect(self):
        self.db.close()
        print("Disconnected to the database")
    
    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path
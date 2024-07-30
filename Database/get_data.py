import requests
import gzip
import shutil
import os
import pathlib
import time 
from urllib.parse import urlparse
from io import BytesIO
from tqdm.auto import tqdm
import datetime 
from .convert_data import convert_and_store_ETagPairLive, convert_and_store_traffic_accident, convert_and_store_construction_zone
from .db import database

"""
Purpose of the file :
    Getting all data needed from internet, and save unpreprocessed data in db (default name is row.db).
    Need to write custom function in "convert_data.py" and some specs in here for new data source.
    Specs for new data source:
        * __init__ : define data_type
        * __fetch_all_data  :define url, file_name, data_type
        * __process_and_store_data : call functions from convert_data.py for certain data_type
    Core sub-function : __fetch_data
"""

class GetData:
    def __init__(self, db_name='row.db', car_code_needed=[], segment_id_needed=[], already_fetched = False):
        os.makedirs(self.__get_path ('assets/'), exist_ok=True)
        self.__data_types = ["ETagPairLive", "traffic_accident", "construction_zone"]
        self.car_code_needed = car_code_needed
        self.segment_id_needed = segment_id_needed
        self.Database = database(file_name = db_name)
        self.__fetch_all_data(already_fetched)

    def get_db_name(self):
        return self.Database.get_db_name()
    
    # warning comments
    def __fetch_all_data(self, already_fetched):
        """
        define all type of url and file name
        and call fetch_data() to get data
        """
        if already_fetched:
            print("\tAll data already fetched, skip fetching data from internet.")
            return
        data_sources = [
            {
                "file_name": "112年1-10月交通事故簡訊通報狀況資料之分析資料.xlsx", 
                "data_type": "traffic_accident" , 
                "url": "https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E7%B0%A1%E8%A8%8A%E9%80%9A%E5%A0%B1%E8%B3%87%E6%96%99.xlsx"
            }, {
                "file_name": "113年1-2月交通事故簡訊通報狀況資料之驗證資料.xlsx", 
                "data_type": "traffic_accident" , 
                "url": "https://freeway2024.tw/113%E5%B9%B41-2%E6%9C%88%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E7%B0%A1%E8%A8%8A%E9%80%9A%E5%A0%B1%E8%B3%87%E6%96%99.xlsx"
            }, {
                "file_name": "112年1-10月施工路段資料之分析資料.xlsx", 
                "data_type": "construction_zone" , 
                "url": "https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E9%81%93%E8%B7%AF%E6%96%BD%E5%B7%A5%E8%B7%AF%E6%AE%B5%E8%B3%87%E6%96%99.xlsx"
            }, {
                "file_name": "113年1-2月施工路段資料之驗證資料.xlsx", 
                "data_type": "construction_zone" , 
                "url": "https://freeway2024.tw/113%E5%B9%B41-2%E6%9C%88%E6%96%BD%E5%B7%A5%E8%B7%AF%E6%AE%B5%E8%B3%87%E6%96%99.xlsx"
            }
        ]
        for _, ds in enumerate(tqdm(data_sources, desc='fetch all data(except ETagPairLive)')):
            self.__fetch_data(ds['url'], ds['file_name'], ds['data_type'], skip_exist=True, delete_file=False)

        if self.car_code_needed == []:
            raise ValueError("car_code_needed is empty, should be a valid car code list")
        self.__fetch_all_ETagPairLive(skip_exist=True, delete_file=True, show_delete=False)
        print("All data successfully inserted into the SQLite3 database.")
        # end of fetch_all_data function

    # main sub-function
    def __fetch_data(self, url, file_name, data_type, addition_path="", skip_exist=True, delete_file=True, show_delete=True):
        """
        get one data and process it from url
        store in Database/assets folder
        skip_exist : if file exist in asset, skip download
        delete_file : if file exist in asset, delete it after process
        """
        # exception handling
        if not url:
            raise ValueError("url is empty, should be a valid url")
        if not file_name:
            raise ValueError("file_name is empty, should be a valid file name")
        if data_type not in self.__data_types:
            raise ValueError("data_type is not valid, get " + data_type + " ,should be one of " + self.__data_types)

        # check if file exist
        store_path = self.__get_path('assets/'+addition_path+'/'+file_name)
        if skip_exist and self.__check_gotton(store_path, addition_path+'/'+file_name):
            return

        # main function
        folder, url_file_name, extensions = self.__get_url_file_name(url)
        requested_file = self.__request_file(url)
        self.__unzip_and_store_file(requested_file, store_path, extensions)
        self.__process_and_store_data(file_name, store_path, data_type, self.Database)
        if delete_file:
            self.__delete_file(store_path, show_delete)
        self.__enroll_file(addition_path+'/'+file_name)

    def __get_url_file_name(self, url):
        if not url:
            raise ValueError("url is empty, should be a valid url")
        parsed_url = urlparse(url)
        pathes = parsed_url.path.split("/")
        folder = pathes[-2] # 資料夾名稱
        file_name = pathes[-1] # 檔案名稱
        path = pathlib.Path(file_name)
        extension = path.suffixes # 副檔名
        return folder, file_name, extension

    def __request_file(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError("response is not 200, should be a valid url, url is " + url)
        return response.content

    def __unzip_and_store_file(self, requested_file, store_path, extensions):
        if not extensions:
            raise ValueError("request_file_name is empty, should be a valid file name")
        if not requested_file:
            raise ValueError("request_file is empty, should be a valid file")
        if not store_path:
            raise ValueError("store_path is empty, should be a valid file path")
        
        compressed_file = BytesIO(requested_file)
        if extensions[-1] == ".gz":
            with open(store_path, 'wb') as f_out \
            ,gzip.open(compressed_file, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        else:
            with open(store_path, 'wb') as f_out:
                f_out.write(requested_file)

    # can be extend
    def __process_and_store_data(self, file_name, store_path, data_type, Database):
        if not file_name:
            raise ValueError("file_name is empty, should be a valid file name")
        if not store_path:
            raise ValueError("store_path is empty, should be a valid file path")
        if data_type not in self.__data_types:
            raise ValueError("data_type is not valid, get " + data_type + " ,should be one of " + self.__data_types)
        
        # process data here
        try:
            if data_type == "ETagPairLive":
                convert_and_store_ETagPairLive(store_path, Database, self.car_code_needed, self.segment_id_needed)
            elif data_type == "traffic_accident":
                convert_and_store_traffic_accident(store_path, Database)
            elif data_type == "construction_zone":
                convert_and_store_construction_zone(store_path, Database)
            else:
                raise ValueError("data_type is not valid, get " + data_type + " ,should be one of " + self.__data_types)
        except Exception as e:
            print("Error: ", e)
            print("Error occurred during processing the data from file: ", store_path)
            print("Data not inserted into the SQLite3 database.")

    def __delete_file(self, store_path, show_delete = True):
        if not store_path:
            raise ValueError("store_path is empty, should be a valid file path")
        if os.path.exists(store_path):
            os.remove(store_path)
            if show_delete:
                print("file \"" + store_path + "\" deleted")
        else:
            print("The file does not exist, file path is " + store_path)

    # warning comments
    def __fetch_all_ETagPairLive(self, skip_exist=True, delete_file=True, show_delete=False):
        ## 112年1月1號得資料在2023/0101/0025
        ## 112年10月31號得資料在2023/1101/0020
        ## 113年1月1號得資料在2024/0101/0025
        ## 113年2月28號得資料在2024/0301/0020
        # self.__fetch_ETagPairLive(2023, 1, 1, 0, 25, 2023, 11, 1, 0, 20) # 112年主要資料
        # self.__fetch_ETagPairLive(2024, 1, 1, 0, 25, 2024, 3, 1, 0, 20) # 113年主要資料

        ## test data
        self.__fetch_ETagPairLive(2023, 1, 1, 0, 25, 2023, 1, 1, 0, 25, skip_exist, delete_file, show_delete)
        self.__fetch_ETagPairLive(2023, 1, 1, 8, 00, 2023, 1, 1, 9, 00, skip_exist, delete_file, show_delete)
        self.__fetch_ETagPairLive(2023, 11, 1, 0, 20, 2023, 11, 1, 0, 20)
        self.__fetch_ETagPairLive(2024, 1, 1, 0, 25, 2024, 1, 1, 0, 25)
        self.__fetch_ETagPairLive(2024, 3, 1, 0, 20, 2024, 3, 1, 0, 20)

    def __fetch_ETagPairLive(self, begin_year, begin_month, begin_day, begin_hour, begin_min, end_year, end_month, end_day, end_hour, end_min, skip_exist=True, delete_file=True, show_delete=False):
        def get_time_range (start_time, end_time):
            current_time = start_time
            while current_time <= end_time:
                yield current_time
                current_time = current_time + datetime.timedelta(minutes=5)
        data_type = "ETagPairLive"
        addition_path = "ETag" 
        os.makedirs(self.__get_path ('assets/ETag/' + str(begin_year) + '/'), exist_ok=True)
        start_time = datetime.datetime(begin_year, begin_month, begin_day, begin_hour, begin_min)
        end_time = datetime.datetime(end_year, end_month, end_day, end_hour, end_min)
        num_steps = int((end_time - start_time).total_seconds() / 300) + 1  
        if delete_file:
            tqdm_title = "fetch and delete ETagPairLive from " + str(start_time) + " to " + str(end_time)
        else :
            tqdm_title = "fetch ETagPairLive from " + str(start_time) + " to " + str(end_time)
        
        for current_time in tqdm(get_time_range(start_time, end_time), desc=tqdm_title, total=num_steps, position=0, leave=True):
            try : 
                file_name = "ETagPairLive_" + str(begin_year) + '_' + '{:02}'.format(current_time.month) + '{:02}'.format(current_time.day) + '_' + '{:02}'.format(current_time.hour) + '{:02}'.format(current_time.minute) + ".xml"
                url = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/" + str(begin_year) + '{:02}'.format(current_time.month) + '{:02}'.format(current_time.day) + '/' + "ETagPairLive_" + '{:02}'.format(current_time.hour) + '{:02}'.format(current_time.minute) + ".xml.gz"
                self.__fetch_data(url, file_name, data_type, addition_path=addition_path, skip_exist=skip_exist, delete_file=delete_file, show_delete=show_delete)
            except Exception as e:
                print("Error: ", e)
                print("Error occurred during fetching the ETagPairLive data from url: ", url)
                print("Data not inserted into the SQLite3 database.")
                pass
        # end of fetch_ETagPairLive function

    def __check_gotton(self, store_path, file_name):
        if os.path.exists(store_path):
            print("file \"" + file_name + "\" exist in asset, skip file:"+ file_name)
            return 1
        try:
            with open(self.__get_path('assets/file_list.txt'), 'r') as f:
                if file_name in f.read():
                    print("file \"" + file_name + "\" exist in asset, skip file:"+ file_name)
                    return 1 
        except:
            if os.path.exists(self.__get_path('assets/file_list.txt')) == False:
                with open(self.__get_path('assets/file_list.txt'), 'w') as f:
                    f.write('')
                    return 0
        return 0
    
    def __enroll_file(self, file_name):
        with open(self.__get_path('assets/file_list.txt'), 'a') as f:
            f.write(file_name + '\n')

    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path
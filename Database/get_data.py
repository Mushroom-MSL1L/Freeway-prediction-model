import requests
import gzip
import shutil
import os
import pathlib
from urllib.parse import urlparse
from io import BytesIO
import time 
from .process_data import process_ETagPairLive, process_traffic_accident, process_construction_zone
from .db import database

class GetData:
    def __init__(self):
        os.makedirs(self.__get_path ('assets/'), exist_ok=True)
        self.__data_types = ["ETagPairLive", "traffic_accident", "construction_zone"]
        self.Database = database()
        self.__fetch_all_data()

    # not finish yet 
    def __fetch_all_data(self):
        """
        define all type of url and file name
        and call fetch_data() to get data
        """
        ### begin of test ###
        url = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/20240302/ETagPairLive_2320.xml.gz"
        file_name = "ETagPairLive_2320.xml"
        data_type = "ETagPairLive"
        self.__fetch_data(url, file_name, data_type, skip_exist=False, delete_file=False)

        url = "https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E7%B0%A1%E8%A8%8A%E9%80%9A%E5%A0%B1%E8%B3%87%E6%96%99.xlsx"
        file_name = "112年1-10月及113年1-2月交通事故簡訊通報狀況資料之分析資料.xlsx"
        data_type = "traffic_accident"
        self.__fetch_data(url, file_name, data_type, skip_exist=False, delete_file=False)

        url = "https://freeway2024.tw/112%E5%B9%B41-10%E6%9C%88%E9%81%93%E8%B7%AF%E6%96%BD%E5%B7%A5%E8%B7%AF%E6%AE%B5%E8%B3%87%E6%96%99.xlsx"
        file_name = "112年1-10月及113年1-2月施工路段資料之分析資料.xlsx"
        data_type = "construction_zone"
        self.__fetch_data(url, file_name, data_type, skip_exist=False, delete_file=False)
        ### end of test ###

        # self.__fetch_all_ETagPairLive()

    # not finish yet 
    def __fetch_data(self, url, file_name, data_type, addition_path="", skip_exist=True, delete_file=True):
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
        if skip_exist:
            if os.path.exists(store_path):
                print("file \"" + file_name + "\" exist in asset, skip url:"+ url)
                return

        # main function
        folder, url_file_name, extensions = self.__get_url_file_name(url)
        requested_file = self.__request_file(url)
        self.__unzip_and_store_file(requested_file, store_path, extensions)
        self.__process_data(file_name, store_path, data_type)
        # self.__store_data()
        if delete_file:
            self.__delete_file(store_path)

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
    def __process_data(self, file_name, store_path, data_type):
        if not file_name:
            raise ValueError("file_name is empty, should be a valid file name")
        if not store_path:
            raise ValueError("store_path is empty, should be a valid file path")
        if data_type not in self.__data_types:
            raise ValueError("data_type is not valid, get " + data_type + " ,should be one of " + self.__data_types)
        
        # process data here
        if data_type == "ETagPairLive":
            process_ETagPairLive(store_path)
        elif data_type == "traffic_accident":
            process_traffic_accident(store_path)
        elif data_type == "construction_zone":
            process_construction_zone(store_path)

    def __delete_file(self, store_path):
        if not store_path:
            raise ValueError("store_path is empty, should be a valid file path")
        if os.path.exists(store_path):
            os.remove(store_path)
            print("file \"" + store_path + "\" deleted")
        else:
            print("The file does not exist, file path is " + store_path)

    def __fetch_all_ETagPairLive(self):
        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        data_type = "ETagPairLive"
        os.makedirs(self.__get_path ('assets/ETag/2023/'), exist_ok=True)
        for month in range(1, 11) :
            url_month = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/2023" + '{:02}'.format(month)
            os.makedirs(self.__get_path ('assets/ETag/2023/' + '{:02}'.format(month)), exist_ok=True)
            for day in range(1, days[month-1]+1) :
                addition_path = "ETag/2023" + "/" + '{:02}'.format(month) + '/' + '{:02}'.format(day)
                url_day = url_month + '{:02}'.format(day) + "/"
                os.makedirs(self.__get_path ('assets/ETag/2023/' + '{:02}'.format(month) + '/' + '{:02}'.format(day)), exist_ok=True)
                for hour in range(0, 24) :
                    for min in range(0, 60, 5) :
                        file_name = "ETagPairLive_" + '{:02}'.format(hour) + '{:02}'.format(min) + ".xml"
                        url = url_day + file_name + ".gz"
                        self.__fetch_data(url, file_name, data_type, addition_path=addition_path, skip_exist=True)

    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path
from Database import Preprocess
from Model import Model

"""
check before run :
    get_data.py 
        1. __fetch_all_ETagPairLive
        2. __fetch_all_data 的 data_source 
    preprocess.py
        * 各種print 
    main.py
        1. segment_id_needed
        2. Preprocess 的 already_fetched
            * assets 要不要刪掉

other warning:
    * preprocess.py
        1. __get_car_frequency 的車子type mapping 是寫死的
        2. 暫時無法確定改成modin之後是不是執行正確的（只有少數資料的時候）
        3. 沒跑過modin有大量資料的時候能不能跑
    * modin 
        * interrupt時會有一堆warning，是正常的。
"""

if __name__ == "__main__":
    segment_id_needed = [
        {"ID" : "01F0928N-01F0880N", "from" : "新竹(新竹、竹東)-竹北", "to" : "竹北-湖口"},
        {"ID" : "01F0956N-01F0928N", "from" : "新竹(科學工業園區)-新竹(新竹、竹東)", "to" : "新竹(新竹、竹東)-竹北"},
        {"ID" : "01F0979N-01F0956N", "from" : "新竹系統-新竹(科學工業園區)", "to" : "新竹(科學工業園區)-新竹(新竹、竹東)"},
        {"ID" : "01F1045N-01F0979N", "from" : "頭份-新竹系統", "to" : "新竹系統-新竹(科學工業園區)"},

        {"ID" : "01F0880S-01F0928S", "from" : "湖口-竹北", "to" : "竹北-新竹(新竹、竹東)"}, 
        {"ID" : "01F0928S-01F0950S", "from" : "竹北-新竹(新竹、竹東)", "to" : "新竹(新竹、竹東)-新竹(科學工業園區)"},
        {"ID" : "01F0950S-01F0980S", "from" : "新竹(新竹、竹東)-新竹(科學工業園區)", "to" : "新竹(科學工業園區)-新竹系統"},
        {"ID" : "01F0980S-01F1045S", "from" : "新竹(科學工業園區)-新竹系統", "to" : "新竹系統-頭份"},
    ]
    car_code_needed = [31, 32, 41, 42, 5] # 31小客車 32小貨車 41大客車 42大貨車 5聯結車
    preprocess_var = Preprocess(segment_id_needed, car_code_needed, already_fetched=False)
    preprocessed_data = preprocess_var.get_preprocessed_data()
    print(preprocessed_data.head())

    # m = Model()
    # m.test_model_with_diabetes()
    
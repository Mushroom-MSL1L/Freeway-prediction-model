from Database import Preprocess
from Model import Model
from Model import two_model
import sweetviz as sv
import pandas as pd

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
    already_fetched         = True
    already_preprocessed    = True
    
    preprocess_var = Preprocess(segment_id_needed, car_code_needed, already_fetched, already_preprocessed)
    preprocessed_data = preprocess_var.get_preprocessed_data()
    car_map = preprocess_var.get_car_map()
    print(preprocessed_data.head())
    print(car_map)


    # m = Model()
    t = two_model()
    all_columns = [ 'UTC',
            'ETagPairID', 'direction', 'highway', 'start_mileage', 'end_mileage', 'car', 'speed', 
            'year', 'month_sin', 'month_cos', 'day_sin', 'day_cos', 'five_minute_sin', 'five_minute_cos', 
            'is_weekend', 'is_holiday', 'holiday_sin', 'holiday_cos',
            'has_accident', 'recovery_time', 'accident_level', 'traffic_accident_內路肩', 'traffic_accident_內車道', 'traffic_accident_中內車道', 'traffic_accident_中車道', 'traffic_accident_中外車道', 'traffic_accident_外車道', 'traffic_accident_外路肩', 'traffic_accident_匝道',
            'has_construction', 'construction_time', 'construction_第一車道', 'construction_第二車道', 'construction_第三車道', 'construction_第四車道', 'construction_第五車道', 'construction_第六車道', 'construction_第七車道', 'construction_第八車道', 'construction_外側路肩', 'construction_內邊坡', 'construction_外邊坡'
        ]
    column_needed = [
        'car', 
        'speed',
        'month_sin', 'month_cos', 'day_sin', 'day_cos',
        'five_minute_sin', 'five_minute_cos', 
        'is_weekend', 
        'is_holiday', 
        'holiday_sin', 'holiday_cos',
        'has_accident', 
        'recovery_time', 
        'accident_level', 
        'traffic_accident_內路肩', 'traffic_accident_內車道', 'traffic_accident_中內車道', 'traffic_accident_中車道', 'traffic_accident_中外車道', 'traffic_accident_外車道', 'traffic_accident_外路肩', 'traffic_accident_匝道', 
        'has_construction', 
        'construction_time', 
        'construction_第一車道', 'construction_第二車道', 'construction_第三車道', 'construction_第四車道', 'construction_第五車道', 'construction_第六車道', 'construction_第七車道', 'construction_第八車道', 'construction_外側路肩', 'construction_內邊坡', 'construction_外邊坡'
        ]
    first_data = preprocessed_data.query(f"ETagPairID == '01F0928N-01F0880N'")
    first_data = first_data.query(f"car == 0.869")
    # # third_data = second_data.query(f"construction_time > 0")
    # # m.import_freeway(first_data, 'speed', column_needed)
    t.import_freeway(first_data, 'speed', column_needed)

    sweet_report = sv.analyze(pd.read_csv("outliers.csv"))
    sweet_report.show_html('sweet_report.html')

    t.train(save_model=True, file_name="01F0928N_01F0880N.joblib")
    t.test()
    t.predict_all_and_export()

    # params = {
    #     'n_estimators': [100, 200, 300, 400],
    #     'max_depth': list(range(3, 10)),
    #     'learning_rate': [0.1, 0.05, 0.02, 0.01],
    #     'min_child_weight': [1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001]
    # }
    # m.train_XGBoost_random_search(
    #     params=params,
    #     save_model=True, 
    #     file_name="01F0928N_01F0880N_XGBoost_random_search.joblib"
    # )
    # m.test()

    # m.train(
    #     _n_estimators=100, 
    #     _max_features=None, 
    #     _max_depth=None, 
    #     _min_samples_leaf=1, 
    #     save_model=True, 
    #     file_name="01F0928N_01F0880N.joblib"
    # )
    # m.train_halving_random(
    #     save_model=True, 
    #     file_name="01F0928N_01F0880N_halving_random.joblib"
    # )
    # m.train_grid_search(
    #     save_model=True, 
    #     file_name="01F0928N_01F0880N_grid_search.joblib"
    # )
    # m.train_halving_random(
    #     save_model=True, 
    #     file_name="01F0928N_01F0880N_halving_random.joblib"
    # )
    # m.test()

    # m.import_model("01F0928N_01F0880N_halving_random_8.joblib")
    # car_code = car_map[31]
    # query = f"car == {car_code}"
    # m.predict(query=query, n=10, type="query_random")
    
    # column_needed = [
    #     'speed',
    #     'car', 
    #     'month_sin', 'month_cos', 'day_sin', 'day_cos',
    #     'five_minute_sin', 'five_minute_cos',
    #     'is_weekend', 
    #     'weekday_sin', 'weekday_cos', 
    #     'is_holiday', 
    #     'holiday_sin', 'holiday_cos',
    #     'has_accident', 
    #     'recovery_time', 
    #     'traffic_accident_內路肩', 'traffic_accident_內車道', 'traffic_accident_中內車道', 'traffic_accident_中車道', 'traffic_accident_中外車道', 'traffic_accident_外車道', 'traffic_accident_外路肩', 'traffic_accident_匝道', 
    #     'has_construction', 
    #     'construction_time', 
    #     'construction_第一車道', 'construction_第二車道', 'construction_第三車道', 'construction_第四車道', 'construction_第五車道', 'construction_第六車道', 'construction_第七車道', 'construction_第八車道', 'construction_外側路肩', 'construction_內邊坡', 'construction_外邊坡'
    #     ]
    # first_data = preprocessed_data.query(f"ETagPairID == '01F0928N-01F0880N'")
    # m.import_freeway(first_data, 'speed', column_needed)
    # m.import_model("01F0928N_01F0880N_halving_random.joblib")
    # m.predict_all_and_export()

import numpy as np
import pandas as pd
import joblib
import os
import modin.pandas as mpd
from scipy.stats import randint
from datetime import datetime, timezone, timedelta

import xgboost as xgb

from sklearn.inspection import permutation_importance
from sklearn.datasets import load_diabetes
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.experimental import enable_halving_search_cv  # noqa
from sklearn.model_selection import RandomizedSearchCV, train_test_split, HalvingRandomSearchCV, HalvingGridSearchCV


"""
How to use:
    1. call import_freeway() or import_diabetes() to save train and test dataset as a member variable in this class
    2. call train() to train model
    3. call test() to get result
"""
class Model:
    my_model = None
    model_path = ""
    x_train = pd.DataFrame()
    x_test = pd.DataFrame()
    y_train = pd.DataFrame()
    y_test = pd.DataFrame()
    x_original_test = pd.DataFrame()

    def __init__(self):
        pass

    def train(self, _n_estimators, _max_features, _max_depth, _min_samples_leaf, save_model=True, file_name=""):
        """
        train the model

        notice:
            model is imported and saved by 'path' variable, change it to load/save model correctly!
        
        input:
            model parameters: 
                _n_estimator:      an integer indicate the number of decision tree in random forest
                _max_features:     {'sqrt', 'log2', None}, an integer or a float, indicate the number of maximal features in a tree
                _max_depth:        an integer indicate the maximal depth of each decision tree in random forest
                _min_samples_leaf: an integer indicate the minimal number of sample in a leaf node
                *see https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html for detail
                
            import_model: a boolean indicate whether model is from 'path' variable, or train a new model 
            save_model:   a boolean indicate whether model is saved to 'path' variable 
        """
        os.makedirs(self.__get_path ('models/'), exist_ok=True)
        name, _ = os.path.splitext(file_name)
        path = self.__get_path ('models/' + name + '/' + file_name)
        
        path = self.__rename_and_create_folder(path)
        self.my_model = RandomForestRegressor(n_estimators=_n_estimators, max_features=_max_features, max_depth=_max_depth, min_samples_leaf=_min_samples_leaf)
        self.my_model = self.my_model.fit(self.x_train, self.y_train)

        if save_model:
            joblib.dump(self.my_model, path)
            self.model_path = path
            columns = self.x_train.columns.tolist()
            content = "RandomForestRegressor\n"
            content += "n_estimators: " + str(_n_estimators) + "\n"
            content += "max_features: " + str(_max_features) + "\n"
            content += "max_depth: " + str(_max_depth) + "\n"
            content += "min_samples_leaf: " + str(_min_samples_leaf) + "\n"
            content += "features: \n"
            for column in columns:
                content += "  " + column + "\n"
            self.__record(content)
            print("model saved, at ", path)

    def test(self):
        y_pred_train = self.my_model.predict(self.x_train)
        y_pred_test = self.my_model.predict(self.x_test)
        content = ""

        content += self.model_path + "\n"
        content += "------------------Results------------------\n"
        content += "For train dataset:\n"
        content += "  Mean Squared Error: " + str(mean_squared_error(self.y_train, y_pred_train)) + "\n"
        content += "  Mean Absolute Error:" + str(mean_absolute_error(self.y_train, y_pred_train)) + "\n"
        content += "  R^2 Score:          " + str(r2_score(self.y_train, y_pred_train)) + "\n"
        content += "For test dataset:\n"
        content += "  Mean Squared Error: " + str(mean_squared_error(self.y_test, y_pred_test)) + "\n"
        content += "  Mean Absolute Error:" + str(mean_absolute_error(self.y_test, y_pred_test)) + "\n"
        content += "  R^2 Score:          " + str(r2_score(self.y_test, y_pred_test)) + "\n"

        content += "------------Feature importances------------\n"
        performance = permutation_importance(self.my_model, self.x_test, self.y_test, n_repeats=10, random_state=0)
        for i in performance.importances_mean.argsort()[::-1]:
            if performance.importances_mean[i] - 2 * performance.importances_std[i] > 0:
                content += f"{self.x_train.columns[i]:<21}" \
                            f"{performance.importances_mean[i]:.3f}" \
                            f" +/- {performance.importances_std[i]:.3f}\n"
        content += "--------------------end--------------------\n"
        print(content)
        self.__record(content)

    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path

    def __rename_and_create_folder(self, file_path):
        directory, original_filename = os.path.split(file_path)
        parent_directory = os.path.dirname(directory)
        name, ext = os.path.splitext(original_filename)
        
        new_filename = original_filename
        new_folder = name
        counter = 0
        
        while os.path.exists(os.path.join(parent_directory, new_folder, new_filename)):
            counter += 1
            new_filename = f"{name}_{counter}{ext}"
            new_folder = f"{name}_{counter}"
        
        new_file_path = os.path.join(parent_directory, new_folder, new_filename)
        os.makedirs(os.path.join(parent_directory, new_folder), exist_ok=True)
        return new_file_path

    def __record(self, content):
        directory, original_filename = os.path.split(self.model_path)
        path = self.__get_path (os.path.join(directory, "record.txt"))
        with open(path, 'a') as f:
            f.write(content + '\n')

    def train_XGBoost(self, _n_estimators, _max_depth, _learning_rate, _min_child_weight, save_model=True, file_name=""):
        os.makedirs(self.__get_path('models/'), exist_ok=True)
        name, _ = os.path.splitext(file_name)
        path = self.__get_path('models/' + name + '/' + file_name)
        
        path = self.__rename_and_create_folder(path)

        self.my_model = xgb.XGBRegressor(n_estimators=_n_estimators, 
                                        max_depth=_max_depth, 
                                        learning_rate=_learning_rate, 
                                        min_child_weight=_min_child_weight)
        
        self.my_model = self.my_model.fit(self.x_train, self.y_train)

        if save_model:
            joblib.dump(self.my_model, path)
            self.model_path = path
            columns = self.x_train.columns.tolist()

            content = "XGBRegressor\n"
            content += "n_estimators: " + str(_n_estimators) + "\n"
            content += "max_depth: " + str(_max_depth) + "\n"
            content += "learning_rate: " + str(_learning_rate) + "\n"
            content += "min_child_weight: " + str(_min_child_weight) + "\n"
            content += "features: \n"
            for column in columns:
                content += "  " + column + "\n"
            self.__record(content)
            print("Model saved at", path)

    def train_XGBoost_random_search(self, params, save_model=True, file_name=""):
        xgb_model = xgb.XGBRegressor()

        random_search = RandomizedSearchCV(xgb_model, 
                                        param_distributions=params, 
                                        n_iter=50, # sample 100 times
                                        cv=2, 
                                        verbose=1, 
                                        n_jobs=-1) # all processors
        random_search.fit(self.x_train, self.y_train)

        best_params = random_search.best_params_

        self.train_XGBoost(
            _n_estimators=best_params['n_estimators'], 
            _max_depth=best_params['max_depth'], 
            _learning_rate=best_params['learning_rate'], 
            _min_child_weight=best_params['min_child_weight'],
            save_model=save_model, 
            file_name=file_name
        )


# include constant hyperparameters
    def train_grid_search(self, save_model=True, file_name=""):
        os.makedirs(self.__get_path ('models/'), exist_ok=True)
        name, _ = os.path.splitext(file_name)
        path = self.__get_path ('models/' + name + '/' + file_name)
        path = self.__rename_and_create_folder(path)
        
        clf = RandomForestRegressor(random_state=0)
        param_grid = {
            "n_estimators": [300, 400, 500],
            "max_features": ["sqrt", "log2", None],
            "max_depth": [3, None],
            "min_samples_leaf": [2, 4, 8, 16]
        }
        search = HalvingGridSearchCV(clf, param_grid, random_state=0)

        self.my_model = search.fit(self.x_train, self.y_train)
        self.model_path = ""

        if save_model:
            joblib.dump(self.my_model, path)
            self.model_path = path
            columns = self.x_train.columns.tolist()
            params = search.best_params_  
            content = "RandomForestRegressor with Grid Search\n"
            for key in params.keys():
                content += key + ": " + str(params[key]) + "\n"
            content += "features: \n"
            for column in columns:
                content += "  " + column + "\n"
            self.__record(content)
            print("Model saved, at ", path)

# include constant hyperparameters
    def train_halving_random (self, save_model=True, file_name=""):
        os.makedirs(self.__get_path ('models/'), exist_ok=True)
        name, _ = os.path.splitext(file_name)
        path = self.__get_path ('models/' + name + '/' + file_name)
        path = self.__rename_and_create_folder(path)
        
        clf = RandomForestRegressor(random_state=0)
        param_grid = {
            "max_features": ["sqrt", "log2", None],
            "max_depth": [int(x) for x in np.linspace(start=20, stop=100, num=10)],
            "min_samples_leaf": [int(x) for x in np.linspace(start=2, stop=1024, num=10)]
        }
        search = HalvingRandomSearchCV(
            clf, 
            param_grid, 
            resource='n_estimators', 
            min_resources=50,
            max_resources=500, 
            random_state=0
        )

        self.my_model = search.fit(self.x_train, self.y_train)
        self.model_path = ""

        if save_model:
            joblib.dump(self.my_model, path)
            self.model_path = path
            columns = self.x_train.columns.tolist()
            params = search.best_params_  
            content = "RandomForestRegressor with Halving Random Forest\n"
            for key in params.keys():
                content += key + ": " + str(params[key]) + "\n"
            content += "features: \n"
            for column in columns:
                content += "  " + column + "\n"
            self.__record(content)
            print("Model saved, at ", path)

    def predict(self, query, n = 1, type='random'):
        if n < 1:
            raise ValueError("n must be greater than 0")
        if type == 'random':
            x = self.x_test.sample(n)
        elif type == 'query_const':
            x = self.x_test.query(query).head(n)
            if x.shape[0] < n:
                raise ValueError("query result is less than n")
        elif type == 'query_random':
            x = self.x_test.query(query)
            if x.shape[0] < n:
                raise ValueError("query result is less than n")
            x = x.sample(n)
        elif type == 'all':
            x = self.x_test
        else:
            raise ValueError("type must be 'random' or 'query'")
        
        pred_y = self.my_model.predict(x)
        real_y = self.y_test[x.index]
        print("features: ", x.columns.tolist())

        results = x.copy()
        results['Predicted'] = pred_y
        results['Real'] = real_y.values
        results.to_csv('prediction_results.csv', index=False)

        for i in range(min(n, x.shape[0])):
            print("Data ", i)
            print("Features: ", x.iloc[i])
            print("Predicted: ", pred_y[i], " Real: ", real_y.iloc[i])
        return x, pred_y, real_y

    def predict_all_and_export(self):
        def get_run_time (speed, mileage_a, mileage_b) :
            if speed == 0:
                return 0
            return abs(mileage_a - mileage_b) / speed
        pred_y = self.my_model.predict(self.x_test)
        results = self.x_original_test.copy()
        base_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        results['utc'] = results['utc'].apply(lambda x: base_time + timedelta(seconds=x))
        results['Predicted_speed'] = pred_y
        results['Real_speed'] = self.y_test.values
        results['Prediected_run_time'] = results.apply(lambda x: get_run_time(x['Predicted_speed'], x['start_mileage'], x['end_mileage']), axis=1)
        results['Real_run_time'] = results.apply(lambda x: get_run_time(x['Real_speed'], x['start_mileage'], x['end_mileage']), axis=1)
        results = results.sort_values(by=['utc'], ascending=True)
        results.to_csv('prediction_results.csv', index=False)
        print("prediction results exported")
        filtered_results = results[results['Real_run_time'] != 0]
        print("mape: ", np.mean(np.abs((filtered_results['Prediected_run_time'] - filtered_results['Real_run_time']) / filtered_results['Real_run_time']))*100, "%")


    def import_model(self, file_name):
        name, _ = os.path.splitext(file_name)
        path = self.__get_path ('models/' + name + '/' + file_name)
        try :
            self.my_model = joblib.load(path)
            self.model_path = path
            print("model imported from ", file_name)
        except:
            raise ValueError("model not found")

    def import_diabetes(self):
        """
        import example dataset 'diabetes'
        """
        path = "random_forest.joblib"
        diabete = load_diabetes()
        x = pd.DataFrame(diabete.data, columns=diabete.feature_names)
        y = diabete.target

        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(x, y, test_size=0.2)
    
    def import_freeway(self, mdf, target_column, column_needed):
        """
        import freeway dataset

        input:
            mdf: a modin dataframe with data
        """
        train_data_mdf = mpd.DataFrame()
        test_data_mdf = mpd.DataFrame()

        mdf = mdf[mdf[target_column] > 0]
        
        train_data_mdf = mdf.query('year == 2023')
        test_data_mdf = mdf.query('year == 2024')

        self.x_original_test = test_data_mdf.copy()

        train_data = train_data_mdf[column_needed]._to_pandas()
        test_data = test_data_mdf[column_needed]._to_pandas()

        self.y_train = train_data[target_column]
        self.y_test = test_data[target_column]

        self.x_train = train_data.drop(columns=[target_column])
        self.x_test = test_data.drop(columns=[target_column])
        print("freeway imported")

    def get_model(self):
        return self.my_model

    def get_model_path(self):
        return self.model_path
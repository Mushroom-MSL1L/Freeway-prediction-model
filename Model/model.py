import numpy as np
import pandas as pd
import joblib
import os

from sklearn.inspection import permutation_importance
from sklearn.datasets import load_diabetes
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

class Model:
    my_model=None
    x_train=None
    x_test=None
    y_train=None
    y_test=None

    def __init__(self):
        pass

    def train(self, _n_estimators, _max_features, _max_depth, _min_samples_leaf, import_model=False, save_model=True, path=""):
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
        path = self.__get_path ('models/' + path)
        
        if import_model:
            try :
                self.my_model = joblib.load(path)
                print("model imported")
            except:
                raise ValueError("model not found")
        else:
            self.my_model = RandomForestRegressor(n_estimators=_n_estimators, max_features=_max_features, max_depth=_max_depth, min_samples_leaf=_min_samples_leaf)
            self.my_model.fit(self.x_train, self.y_train)

        if save_model:
            joblib.dump(self.my_model, path)
            print("model saved, at ", path)

    def test(self):
        y_pred = self.my_model.predict(self.x_test)
        print("------------------Results------------------")
        print("Mean Squared Error: ", mean_squared_error(self.y_test, y_pred))
        print("Mean Absolute Error:", mean_absolute_error(self.y_test, y_pred))
        print("R^2 Score:          ", r2_score(self.y_test, y_pred))
        
        print("------------Feature importances------------")
        performance = permutation_importance(self.my_model, self.x_test, self.y_test, n_repeats=10, random_state=0)
        for i in performance.importances_mean.argsort()[::-1]:
            if performance.importances_mean[i] - 2 * performance.importances_std[i] > 0:
                print(f"{self.x_train.columns[i]:<21}"
                    f"{performance.importances_mean[i]:.3f}"
                    f" +/- {performance.importances_std[i]:.3f}")
        print("--------------------end--------------------\n")

    def __get_path (self, file_name):
        current_file_path = os.path.realpath(__file__)
        current_dir_path = os.path.dirname(current_file_path)
        file_path = os.path.join(current_dir_path, file_name)
        return file_path

    def test_model_with_diabetes(self):
        """
        test model training and testing function with example dataset 'diabetes'
        """
        path = "random_forest.joblib"
        diabete = load_diabetes()
        x = pd.DataFrame(diabete.data, columns=diabete.feature_names)
        y = diabete.target

        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(x, y, test_size=0.2)

        print("testing training model...")
        self.train(_n_estimator=100, _max_features=None, _max_depth=None, _min_samples_leaf=1, import_model=False, save_model=True, path=path)
        self.test()

        print("testing importing model...")
        self.train(_n_estimator=100, _max_features=None, _max_depth=None, _min_samples_leaf=1, import_model=True, save_model=False, path=path)
        self.test()
    
    def test_model_with_freeway(self, df, __n_estimators=100, __max_features=None, __max_depth=None, __min_samples_leaf=1, _import_model=False, _save_model=True):
        """
        train and test model with freeway dataset
        """
        train_data=df.query("year=2023")
        test_data=df.query("year=2024")

        self.x_train=train_data.loc[:, df.columns != 'speed']
        self.x_test=test_data.loc[:, df.columns != 'speed']
        self.y_train=train_data['speed']
        self.y_test=test_data['speed']

        print("training model...")
        self.train( _n_estimators=__n_estimators, _max_features=__max_features, _max_depth=__max_depth, _min_samples_leaf=__min_samples_leaf, import_model=_import_model, save_model=_save_model, path="random_forest.joblib")
        self.test()

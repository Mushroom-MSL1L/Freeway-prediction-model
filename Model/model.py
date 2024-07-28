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
    def __init__(self):
        pass

    def train(self, x, y, _test_size=0.2, _random_split=True, _n_estimators=100, _max_features=None, _max_depth=None, _min_samples_leaf=1, import_model=False, save_model=True, path=""):
        """
        train the model

        notice:
            model is imported and saved by 'path' variable, change it to load/save model correctly!
        
        input:
            x: data used to predict outcome in panda dataframe's format
            y: outcome

            dataset parameters:
                _test_size:    a float between 0 and 1 to indicate the ratio of test dataset
                _random_split: a boolean indicate whether dataset is split randomly into train and test dataset every time function is called
            
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
        
        if _random_split:
            X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=_test_size)
        else:
            X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=_test_size, random_state=1)

        if import_model:
            try :
                my_model = joblib.load(path)
                print("model imported")
            except:
                raise ValueError("model not found")
        else:
            my_model = RandomForestRegressor(n_estimators=_n_estimators, max_features=_max_features, max_depth=_max_depth, min_samples_leaf=_min_samples_leaf)
            my_model.fit(X_train, y_train)

        if save_model:
            joblib.dump(my_model, path)
            print("model saved, at ", path)
        
        y_pred = my_model.predict(X_test)
        print("------------------Results------------------")
        print("Mean Squared Error: ", mean_squared_error(y_test, y_pred))
        print("Mean Absolute Error:", mean_absolute_error(y_test, y_pred))
        print("R^2 Score:          ", r2_score(y_test, y_pred))
        
        print("------------Feature importances------------")
        performance = permutation_importance(my_model, X_test, y_test, n_repeats=10, random_state=0)
        for i in performance.importances_mean.argsort()[::-1]:
            if performance.importances_mean[i] - 2 * performance.importances_std[i] > 0:
                print(f"{x.columns[i]:<21}"
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
        run "python model.py" to test model training function with example dataset
        """
        path = "random_forest.joblib"
        diabete = load_diabetes()
        x = pd.DataFrame(diabete.data, columns=diabete.feature_names)
        y = diabete.target

        print("testing training model...")
        self.train(x, y, _random_split=False, import_model=False, save_model=True, path=path)
        print("testing importing model...")
        self.train(x, y, _random_split=False, import_model=True, save_model=False, path=path)
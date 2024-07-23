import numpy as np
import pandas as pd
import joblib

from sklearn.inspection import permutation_importance
from sklearn.datasets import load_diabetes
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

def train(x, y, _test_size=0.2, _random_split=True, _n_estimators=100, _max_depth=None, _min_samples_leaf=1, import_model=False, save_model=True):
    """
    train the model

    notice:
        model is imported and saved by 'path' variable, change it to load/save model correctly!
    
    input:
        x: data used to predict outcome in panda dataframe's format
        y: outcome

        dataset parameters:
            _test_size:    a float between 0 and 1 to indicate the ratio of test dataset
            _random_split: a boolean indicate whether dataset is split
        
        model parameters:
            _n_estimator:      an integer indicate the number of decision tree in random forest
            _max_depth:        an integer indicate the maximal depth of each decision tree in random forest
            _min_samples_leaf: an integer indicate the minimal number of sample in a leaf node
        
        import_model: a boolean indicate whether model is from 'path' variable, or train a new model 
        save_model:   a boolean indicate whether model is saved to 'path' variable 
    """
    path = "./random_forest.joblib"
    
    if _random_split:
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=_test_size)
    else:
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=_test_size, random_state=1)

    if import_model:
        my_model = joblib.load(path)
        print("model imported")
    else:
        my_model = RandomForestRegressor(n_estimators=_n_estimators, max_depth=_max_depth, min_samples_leaf=_min_samples_leaf)
        my_model.fit(X_train, y_train)

    if save_model:
        joblib.dump(my_model, path)
        print("model saved")
    
    y_pred = my_model.predict(X_test)
    print("------------------Results------------------")
    print("Mean Squared Error: ", mean_squared_error(y_test, y_pred))
    print("Mean Absolute Error:", mean_absolute_error(y_test, y_pred))
    print("R^2 Score:          ", r2_score(y_test, y_pred))
    
    print("------------Feature importances------------")
    performance = permutation_importance(my_model, X_test, y_test, n_repeats=10, random_state=0)
    for i in performance.importances_mean.argsort()[::-1]:
        if performance.importances_mean[i] - 2 * performance.importances_std[i] > 0:
            print(f"{x.columns[i]:<20}"
                  f"{performance.importances_mean[i]:.3f}"
                  f" +/- {performance.importances_std[i]:.3f}")
    print("--------------------end--------------------\n")


if __name__ == "__main__":
    """
    run "python model.py" to test model training function with example dataset
    """
    diabete = load_diabetes()
    x = pd.DataFrame(diabete.data, columns=diabete.feature_names)
    y = diabete.target

    print("testing training model...")
    train(x, y, _random_split=False)
    print("testing importing model...")
    train(x, y, _random_split=False, import_model=True, save_model=False)
import numpy as np
import pandas as pd
import eli5
from eli5.sklearn import PermutationImportance
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

def train(x, y, test_dataset_size=0.2, num_of_estimators=100, random_split=False, import_model=False, save_model=True):
    """
    train the model

    notice:
        model is imported and saved by 'path' variable, change it to load/save model correctly!
    
    input:
        x: data used to predict outcome
        y: outcome
        test_data_size: a float between 0 and 1 to indicate the ratio of test dataset
        num_of_estimator: an integer indicate the number of decision tree in random forest
        random_split: a boolean indicate whether test and train dataset should be randomly split every time
        import_model: a boolean indicate whether model is from 'path' variable, or train a new model 
        save_model: a boolean indicate whether model is saved to 'path' variable 
    """
    state = 42
    path = "./random_forest.joblib"
    
    """
    from sklearn.datasets import load_iris
    iris = load_iris()
    x = iris.data
    y = iris.target
    """

    if random_split:
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=test_dataset_size)
    else:
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=test_dataset_size, random_state=state)

    if import_model:
        my_model = joblib.load(path)
    else:
        my_model = RandomForestRegressor(n_estimators=num_of_estimators, random_state=state)
        my_model.fit(X_train, y_train)

    if save_model:
        my_model = joblib.dump(my_model, path)
    
    y_pred = my_model.predict(X_test)

    print("Mean Squared Error:", mean_squared_error(y_test, y_pred))
    print("Mean Absolute Error:", mean_absolute_error(y_test, y_pred))
    print("R^2 Score:", r2_score(y_test, y_pred))

    performance = PermutationImportance(my_model, random_state=1).fit(X_test, y_test)
    eli5.show_weights(performance, feature_names=X_test.columns.tolist())
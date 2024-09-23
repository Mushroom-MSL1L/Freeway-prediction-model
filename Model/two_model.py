from .model import Model
import numpy as np
import modin.pandas as mpd
import pandas as pd
import os
import joblib
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, IsolationForest
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.inspection import permutation_importance
from sklearn.neighbors import LocalOutlierFactor
from sklearn.experimental import enable_halving_search_cv  # noqa
from sklearn.model_selection import HalvingRandomSearchCV

class two_model(Model) :
    y_train_outlier = pd.DataFrame()
    y_pred_train = pd.DataFrame()
    y_pred_test = pd.DataFrame()
    classifier = None
    normal_regressors = None
    outlier_regressors = None
    def __init__(self) :
        super().__init__()

    def train(self, save_model=False, file_name="two_model.joblib"): 
        self.classifier = RandomForestClassifier()
        self.classifier.fit(self.x_train, self.y_train_outlier)
        normal_index = self.y_train_outlier[self.y_train_outlier == False].index
        outlier_index = self.y_train_outlier[self.y_train_outlier == True].index

        self.normal_regressors = RandomForestRegressor(min_samples_leaf=43, n_estimators = 270, max_depth = 57, max_features = "sqrt", random_state=0) 
        self.normal_regressors.fit(self.x_train.loc[normal_index], self.y_train.loc[normal_index])

        self.outlier_regressors = RandomForestRegressor(min_samples_leaf=43, n_estimators = 270, max_depth = 57, max_features = "sqrt", random_state=0) 
        self.outlier_regressors.fit(self.x_train.loc[outlier_index], self.y_train.loc[outlier_index])

        os.makedirs(self._get_path ('models/'), exist_ok=True)
        name, _ = os.path.splitext(file_name)
        path = self._get_path ('models/' + name + '/' + file_name)
        path = self._rename_and_create_folder(path)
        directory, filename = os.path.split(path)
        name, _ = os.path.splitext(filename)

        if save_model:
            joblib.dump(self.classifier, directory + "/" + name + "_classifier.joblib")
            joblib.dump(self.normal_regressors, directory + "/" + name + "_nor_reg.joblib")
            joblib.dump(self.outlier_regressors, directory + "/" + name + "_out_reg.joblib")
            self.model_path = path
            columns = self.x_train.columns.tolist()
            content = "RandomForestRegressor and RandomForestClassifier\n"
            content += "features: \n"
            for column in columns:
                content += "  " + column + "\n"
            self._record(content)
            print("model saved, at ", path)

    def test(self):
        y_pred_test_class = self.classifier.predict(self.x_test)
        normal_index = np.where(y_pred_test_class == False)[0] 
        outlier_index = np.where(y_pred_test_class == True)[0] 
        
        self.y_pred_test = np.zeros(len(self.y_test))
        self.y_pred_test[normal_index] = self.normal_regressors.predict(self.x_test.iloc[normal_index])
        self.y_pred_test[outlier_index] = self.outlier_regressors.predict(self.x_test.iloc[outlier_index])

        y_pred_train_class = self.classifier.predict(self.x_train)
        normal_index = np.where(y_pred_train_class == False)[0]  
        outlier_index = np.where(y_pred_train_class == True)[0]  
        
        self.y_pred_train = np.zeros(len(self.y_train))
        self.y_pred_train[normal_index] = self.normal_regressors.predict(self.x_train.iloc[normal_index])
        self.y_pred_train[outlier_index] = self.outlier_regressors.predict(self.x_train.iloc[outlier_index])


        content = ""

        content += self.model_path + "\n"
        content += "------------------Results------------------\n"
        content += "For train dataset:\n"
        content += "  Mean Squared Error: " + str(mean_squared_error(self.y_train, self.y_pred_train)) + "\n"
        content += "  Mean Absolute Error:" + str(mean_absolute_error(self.y_train, self.y_pred_train)) + "\n"
        content += "  R^2 Score:          " + str(r2_score(self.y_train, self.y_pred_train)) + "\n"
        content += "For test dataset:\n"
        content += "  Mean Squared Error: " + str(mean_squared_error(self.y_test, self.y_pred_test)) + "\n"
        content += "  Mean Absolute Error:" + str(mean_absolute_error(self.y_test, self.y_pred_test)) + "\n"
        content += "  R^2 Score:          " + str(r2_score(self.y_test, self.y_pred_test)) + "\n"

        # content += "------------Feature importances------------\n"
        # performance = permutation_importance(self.my_model, self.x_test, self.y_test, n_repeats=10, random_state=0)
        # for i in performance.importances_mean.argsort()[::-1]:
        #     if performance.importances_mean[i] - 2 * performance.importances_std[i] > 0:
        #         content += f"{self.x_train.columns[i]:<21}" \
        #                     f"{performance.importances_mean[i]:.3f}" \
        #                     f" +/- {performance.importances_std[i]:.3f}\n"
        content += "--------------------end--------------------\n"
        print(content)
        self._record(content)

    def _rename_and_create_folder(self, file_path):
        directory, original_filename = os.path.split(file_path)
        parent_directory = os.path.dirname(directory)
        name, ext = os.path.splitext(original_filename)
        
        new_filename = original_filename
        new_folder = name
        counter = 0
        
        while os.path.exists(os.path.join(parent_directory, new_folder)):
            counter += 1
            new_filename = f"{name}_{counter}{ext}"
            new_folder = f"{name}_{counter}"
        
        new_file_path = os.path.join(parent_directory, new_folder, new_filename)
        os.makedirs(os.path.join(parent_directory, new_folder), exist_ok=True)
        return new_file_path


    def zScore_outlier_detection(self, y, target_column):
        temp = mpd.DataFrame()
        temp[target_column] = y[target_column]
        mean = temp[target_column].mean()
        std = temp[target_column].std()
        threshold = 3
        temp['z_score'] = (temp[target_column] - mean) / std
        temp['is_outlier'] = temp['z_score'].apply(lambda x: x < -threshold or x > threshold)
        return temp["is_outlier"]
    def local_outlier_factor_detection(self, y, target_column) : 
        temp = mpd.DataFrame()
        temp[target_column] = y[target_column]
        clf = LocalOutlierFactor(n_neighbors=50, contamination="auto")
        temp['is_outlier'] = clf.fit_predict(temp)
        temp['is_outlier'] = temp['is_outlier'].apply(lambda x: x == -1)
        return temp["is_outlier"]
    def isolation_forest_detection(self, y, target_column) :
        temp = mpd.DataFrame()
        temp[target_column] = y[target_column]
        clf = IsolationForest(random_state=0)
        temp['is_outlier'] = clf.fit_predict(temp)
        temp['is_outlier'] = temp['is_outlier'].apply(lambda x: x == -1)
        return temp["is_outlier"]

    def import_freeway(self, mdf, target_column, column_needed):
        train_data_mdf = mpd.DataFrame()
        test_data_mdf = mpd.DataFrame()

        mdf = mdf[mdf[target_column] > 0]
        
        train_data_mdf = mdf.query('year == 2023')
        test_data_mdf = mdf.query('year == 2024')

        self.x_original_test = test_data_mdf.copy()
        self.y_train_outlier = self.local_outlier_factor_detection(train_data_mdf, target_column)._to_pandas()

        self.export_outliers_to_csv(train_data_mdf, self.y_train_outlier, target_column, column_needed, "outliers.csv")

        train_data = train_data_mdf[column_needed]._to_pandas()
        test_data = test_data_mdf[column_needed]._to_pandas()

        self.y_train = train_data[target_column]
        self.y_test = test_data[target_column]

        self.x_train = train_data.drop(columns=[target_column])
        self.x_test = test_data.drop(columns=[target_column])
        print("freeway imported")

    def predict_all_and_export(self):
        def get_run_time (speed, mileage_a, mileage_b) :
            if speed == 0:
                return 0
            return abs(mileage_a - mileage_b) / speed
        def plot_figure(results):
            results.plot(x='utc', y=['Predicted_speed', 'Real_speed'], title='Real vs Predicted Speed in 2024/01 And 2024/02')
            plt.ylabel("speed(km/h)")
            plt.show()
            results.plot(x='utc', y=['Prediected_run_time', 'Real_run_time'], title='Real vs Predicted time in 2024/01 And 2024/02')
            plt.ylabel("travel time(minute)")
            plt.show()
        pred_y = self.y_pred_test
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
        plot_figure(results)
        self.residual_graph()

    def export_outliers_to_csv(self, mdf, outliers, target_column, column_needed, file_name="outliers.csv"):        
        mdf = mdf[mdf[target_column] > 0]
        outliers_data = mdf.loc[outliers.values]
        outliers_data = outliers_data[column_needed]
        outliers_data.to_csv(file_name, index=False)
        print(f"Outliers exported to {file_name}")
        plt.plot(mdf.index, mdf[target_column], label=f'{target_column} line')
        outliers_indices = outliers[outliers == 1].index
        plt.scatter(outliers_indices, mdf.loc[outliers_indices, target_column], color='red', label='Outliers', edgecolors='black', zorder=5)
        plt.xlabel('Index')
        plt.ylabel(target_column)
        plt.title(f'{target_column} with Outliers Marked')
        plt.legend()
        plt.show()

    def residual_graph (self):
        residual = self.y_test - self.y_pred_test
        plt.scatter(self.y_pred_test, residual)
        plt.axhline(y=0, color='r', linestyle='-')
        plt.xlabel('Predicted values')
        plt.ylabel('Residuals')
        plt.title('Residuals vs Predicted values')
        plt.show()
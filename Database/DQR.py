# https://www.kaggle.com/code/ankit5294/quick-data-profiling-data-quality-report-eda
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import matplotlib
import modin.pandas as mpd

class DQR :
    def __init__(self, df):
        self.set_up()
        self.report = self.data_quality_report(df)
    
    def set_up(self):
        plt.style.use('ggplot') # specify the plot style
        plt.rcParams['font.sans-serif'] = ['STFangsong'] # specify the font which can display Chinese 
        plt.rcParams.update({'font.size': 11}) # specify the font size, or it will be too big to display
        pd.set_option('display.width', 1000) # display more char in a line
        
    def get_report (self):
        return self.report
        
    def missing_values(self, df):
        figure, axis = plt.subplots(1, 1, figure=(16, 5))
        axis_1 = axis.pcolormesh(df.isnull().T, cmap = 'rainbow') # change to color map
        axis.set_yticks([x + 0.5 for x in range(0, len(df.columns))]) # in the middle of the cell
        axis.set_yticklabels([x + " - " + str(round(sum(df[x].isnull())/df.shape[0]*100,2)) + "%" for x in df.columns])
        axis.set_title('Missing Values')
        plt.show()
        
    def correlation_matrix(self, df) : 
        figure, axis = plt.subplots(1, 1, figure=(16, 5))
        correlation_matrix = df.corr(method='pearson')
        axis_1 = axis.pcolormesh(correlation_matrix, cmap='viridis') # change to color map
        axis.set_xticks([x + 0.5 for x in range(0, len(correlation_matrix.columns))])
        axis.set_xticklabels(correlation_matrix.columns, rotation='vertical')
        axis.set_yticks([x + 0.5 for x in range(0, len(correlation_matrix.columns))])
        axis.set_yticklabels(correlation_matrix.columns)
        axis.set_title('Pearson\'s Correlation Matrix')
        plt.colorbar(axis_1, ax=axis)
        plt.show()
        
    def data_quality_report(self, df):
        if type(df) == mpd.dataframe.DataFrame:
            df = df._to_pandas()
        df = pd.DataFrame(df)
        rows = df.shape[0] # include miss
        columns = df.shape[1]
        DQR = df.describe(include='all').T # T for transpose
        
        DQR['miss %'] =    DQR['count'].apply(lambda x: (rows - x) / rows * 100)
        DQR['card.'] =     [len(df[x].value_counts()) for x in DQR.index]
        
        DQR['mode'] =      [df[x].value_counts().index[0] for x in DQR.index]
        DQR['mode freq'] = [df[x].value_counts().iloc[0] for x in DQR.index]
        DQR['mode %'] =    DQR['mode freq'] / rows * 100
        
        DQR['2nd mode'] =  [df[x].value_counts().index[1] if len(df[x].value_counts()) > 1 else np.nan for x in DQR.index]
        DQR['2nd mode freq'] = [df[x].value_counts().iloc[1] if len(df[x].value_counts()) > 1 else np.nan for x in DQR.index]
        DQR['2nd mode %'] = DQR['2nd mode freq'] / rows * 100 if DQR['2nd mode'] is not np.nan else np.nan
        
        print('Data Quality Report')
        print('Total columns:', str(columns))
        print('Total rows:', str(rows))
        
        print(df.dtypes)
        print(df.dtypes.value_counts())
        print(DQR)
        
        DQR_rows = DQR.shape[0] 
        df_features = df.columns
        print('histogram and line plot for each feature')
        if (DQR_rows > 5) :
            df.hist(bins=100, 
                    figsize=(20,(DQR_rows//5+1)*4),
                    layout=((DQR_rows//5+1), 5)
            )
            df[df_features].plot(
                kind='line', 
                subplots=True, 
                layout=(DQR_rows, 1), 
                figsize=(20, 4*DQR_rows)
            )
            plt.show()
        else :
            df.hist(bins=100, 
                    figsize=(20, 5),
                    layout=(1, DQR_rows)
            )
            df[df_features].plot(
                kind='line', 
                subplots=True, 
                layout=(1, DQR_rows), 
                figsize=(20, 5)
            )
            plt.show()
        self.missing_values(df)
        self.correlation_matrix(df)
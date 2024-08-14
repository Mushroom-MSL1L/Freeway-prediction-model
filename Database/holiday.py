import numpy as np
import time
import sklearn
from datetime import datetime

"""
Purpose of this file :
    For preprocess_data.py, to process the holiday and date information in the dataset.
    Only focus on 2023 1-12 month, and  2024 1-3 month
"""

# a list for convenice in calculating which day it is in a year
days = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
"""
2023 calendar, starts from Sunday, 2023/01/01, 1-indexed
    first row:
        0: workday
        1: holiday
        2: weekend
    second row:
        a value for convenice in calculating which hour it is in a holiday
    third row:
        a value represents how long the period is (in hour), 0 represent that it is not in a holiday period
"""
holiday = np.zeros((3, 426))
holiday[0] = [0,
              1, 1, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              1, 0, 0,
                       0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 1,
              1, 1, 1, 
                       0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 0,
                                1,
              1, 1, 1, 1, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2, 
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2,
                 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 
                          0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 1, 1, 1,
              1, 0, 0, 0, 0, 0, 
                                2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0,
                    0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 
                             0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 0,
              2, 0, 0, 0, 0, 1, 1,
              
              1, 0, 0, 0, 0, 0, 1,
              1, 1, 1, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0,
                       0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 
                             0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 1,
              1, 

                 1, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 0, 
                          0, 0, 2,
              2, 0, 0, 0, 1, 1, 1,
              1, 1, 1, 1, 0, 0, 0,
              2, 0, 0, 0, 0, 0, 2,
              2, 0, 0, 1, 0]

holiday[1] = [  0,
               60,  84, 108, 132,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,  24,  48,  72,  96, 120, 144,
              168, 192, 216, 240, 264, 288, 312,
              336, 360, 384,
                             408, 432, 456,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,  24,  48,
               72,  96, 120, 
                             144, 168,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, -12,  12,
                                             36,
               60,  84, 108, 132, 156, 180, 204,
              228,   0,   0,   0,   0,   0,   0, 
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,
                     0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,
                                    0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,  24,  48,  72,  96, 
              120, 144, 168,   0,   0,   0, 
                                              0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,
                          0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0, -12,  12,  36,  60,
              
               84, 108, 132,   0,   0,  24,  48,
               72,  96, 120, 144, 168,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,
                               0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, -12,  12,  36,
               60,
                
                    84, 108, 132,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   
                                    0,   0,   0,
              -12,  12,  36,  60,  84, 108, 132,
              156, 180, 204, 228, 252, 276, 300,
              324,   0,   0,   0,   0,   0,   0,
                0,   0, -12,  12,  36]

holiday[2] = [  0,
              144, 144, 144, 144,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
              480, 480, 480, 480, 480, 480, 480,
              480, 480, 480, 480, 480, 480, 480,
              480, 480, 480,
                             480, 480, 480,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 192, 192, 192,
              192, 192, 192, 
                             192, 192,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 240, 240,
                                            240,
              240, 240, 240, 240, 240, 240, 240,
              240,   0,   0,   0,   0,   0,   0, 
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,
                     0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,
                                    0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0, 192, 192, 192, 192, 192, 
              192, 192, 192,   0,   0,   0, 
                                              0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,
                          0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0, 144, 144, 144, 144,
              
              144, 144, 144,   0, 192, 192, 192,
              192, 192, 192, 192, 192,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,
                               0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,
                                         0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0, 144, 144, 144,
              144,
                  
                   144, 144, 144,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   0,   0,   0,
                0,   0,   0,   0,   
                                    0,   0,   0,
              336, 336, 336, 336, 336, 336, 336,
              336, 336, 336, 336, 336, 336, 336,
              336,   0,   0,   0,   0,   0,   0, 
                0,   0,  48,  48,  48]

# ETagLive : 2023-01-01T00:00:00+08:00
# accident : 2023	2	9	19	21
# Construction : 2023/1/1 7:45
# function format : YYYY-MM-DDTHH:MM:SS...

def modified_holiday_and_date_cosine_processor(year, month, day, five_minute, weekday):
    '''
    Given a valid time, return whether it is holiday or weekend, and its value after cosine transformation

    input:
        year, month, day, five_minute
    
    output:
        is_weekend: a boolean
        is_holiday: a boolean
        holiday:    a float represent its value in holiday after sine transformation (resolution is hour), 
                    if the time does not have to do sine transformation, return 1
        month:      a float represent its value in a year after sine transformation (resolution is month)
        day:        a float represent its value in a year after sine transformation (resolution is day)
        minute:     a float represent its value in a day after sine transformation (resolution is 5 minute)
        weekday:    a float represent its value in a week after sine transformation (resolution is day)
    '''

    is_holiday = False
    is_weekend = False
    _holiday = 1.0

    date_time = datetime(year, month, day, five_minute // 12, (five_minute % 12) * 5)
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date

    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.cos(2 * np.pi * (date_time.month / 12))
    day = np.cos(2 * np.pi * (date / 365))
    minute = np.cos(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))
    weekday = np.cos(2 * np.pi * (weekday / 7))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute, weekday
    
    _holiday = np.cos(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute, weekday

def modified_holiday_and_date_sine_processor(year, month, day, five_minute, weekday):
    is_holiday = False
    is_weekend = False
    _holiday = 0.0

    date_time = datetime(year, month, day, five_minute // 12, (five_minute % 12) * 5)
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date

    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.sin(2 * np.pi * (date_time.month / 12))
    day = np.sin(2 * np.pi * (date / 365))
    minute = np.sin(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))
    weekday = np.sin(2 * np.pi * (weekday / 7))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute, weekday
    
    _holiday = np.sin(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute, weekday

def holiday_and_date_sine_processor(start_time):
    '''
    Given a time in 2023, return whether it is holiday or weekend, and its value after sine transformation

    input:
        time: a string in format 'YYYY-MM-DDTHH:MM:SS...', 24-hours based time
              e.g. 2023-01-01T23:45:00+08:00
    
    output:
        is_weekend: a boolean
        is_holiday: a boolean
        holiday:    a float represent its value in holiday after sine transformation (resolution is hour), 
                    if the time does not have to do sine transformation, return 0
        month:      a float represent its value in a year after sine transformation (resolution is month)
        day:        a float represent its value in a year after sine transformation (resolution is day)
        minute:     a float represent its value in a day after sine transformation (resolution is 5 minute)
    '''
    is_holiday = False
    is_weekend = False
    _holiday = 0.0

    date_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date
    
    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.sin(2 * np.pi * (date_time.month / 12))
    day = np.sin(2 * np.pi * (date / 365))
    minute = np.sin(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute
    
    _holiday = np.sin(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute

def holiday_and_date_cosine_processor(start_time):
    '''
    Given a time in 2023, return whether it is holiday or weekend, and its value after cosine transformation

    input:
        time: a string in format 'YYYY-MM-DDTHH:MM:SS...', 24-hours based time
              e.g. 2023-01-01T23:45:00+08:00
    
    output:
        is_weekend: a boolean
        is_holiday: a boolean
        holiday:    a float represent its value in holiday after cosine transformation (resolution is hour), 
                    if the time does not have to do cosine transformation, return 1
        month:      a float represent its value in a year after cosine transformation (resolution is month)
        day:        a float represent its value in a year after cosine transformation (resolution is day)
        minute:     a float represent its value in a day after cosine transformation (resolution is 5 minute)
    '''
    is_holiday = False
    is_weekend = False
    _holiday = 1.0

    date_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z')
    date = days[date_time.month] + date_time.day
    index = (date_time.year - 2023) * 365 + date

    if holiday[0][index] == 1:
        is_holiday = True
    elif holiday[0][index] == 2:
        is_weekend = True
    
    month = np.cos(2 * np.pi * (date_time.month / 12))
    day = np.cos(2 * np.pi * (date / 365))
    minute = np.cos(2 * np.pi * ((date_time.hour * 12 + date_time.minute / 5) / 288))

    # deal with 'no sine transformation value' scenario
    if holiday[2][index] == 0 or holiday[1][index] + date_time.hour <= 0 or holiday[1][index] + date_time.hour >= holiday[2][index]:
        return is_weekend, is_holiday, _holiday, month, day, minute
    
    _holiday = np.cos(2 * np.pi * ((holiday[1][index] + date_time.hour) / holiday[2][index]))
    return is_weekend, is_holiday, _holiday, month, day, minute

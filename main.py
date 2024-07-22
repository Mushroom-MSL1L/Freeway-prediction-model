from Database import Preprocess
import os

if __name__ == "__main__":
    preprocess_var = Preprocess()
    preprocessed_data = preprocess_var.get_preprocessed_data()

    print(preprocessed_data.head())
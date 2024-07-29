from Database import Preprocess
from Model import Model

if __name__ == "__main__":
    # test_model_with_diabetes()
    m = Model()
    preprocess_var = Preprocess()
    preprocessed_data = preprocess_var.get_preprocessed_data()
    print(preprocessed_data.head())
    
#saving the model
from pyspark.ml.classification import LogisticRegressionModel  
model_path = "/path/to/save/model"  
model.save(model_path) 

#loading the model
from pyspark.ml.classification import LogisticRegressionModel  
model_path = "/path/to/save/model"  
loaded_model = LogisticRegressionModel.load(model_path) 

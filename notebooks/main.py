import pandas as pd
import os
output_folder = '../output'
file_path = os.path.join(output_folder, 'customer.csv')  
data = pd.read_csv(file_path)
import data_fetch 
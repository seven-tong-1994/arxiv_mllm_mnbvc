
import pyarrow.parquet as pq
import pandas as pd
 
# 打开Parquet文件
parquet_file = pd.read_parquet('output_dir/2004.14974.parquet')
 
print(parquet_file.head(1))
print(parquet_file.shape[0])
for i in range(parquet_file.shape[0]):
    print(parquet_file['文本'][i])
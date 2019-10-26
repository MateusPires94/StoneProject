import platform
import pandas as pd
import auxtools
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'

df_test = pd.DataFrame([{'a':1,'b':2}])
s3 = auxtools.S3Aux('stone-project')
local_filename = TMP +'test.csv'
remote_filename = TMP+'logs/test.csv'
df_test.to_csv(local_filename,index=False)
s3.upload(local_filename,remote_filename)
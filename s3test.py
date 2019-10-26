import platform
import pandas as pd
import auxtools
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'


engine = auxtools.MySQLAux('MOVIE').engine()
df = pd.DataFrame([{'a':1,'b':2}])
print(df)
df.to_sql('teste', engine, if_exists='replace', index=False)

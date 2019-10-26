import platform
import pandas as pd
import auxtools
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'

credenciais = auxtools.MySQLAux('MOVIE')
print(credenciais.user,credenciais.host,credenciais.password,credenciais.database)
cnx = auxtools.MySQLAux('MOVIE').connect()
query = 'SELECT DISTINCT id_movie from movies_detail'
df_movies_id = pd.read_sql(query,cnx)
cnx.close()
print(df_movies_id)
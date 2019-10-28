import platform
import pandas as pd
import auxtools
import datetime
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'


# engine = auxtools.MySQLAux('MOVIE').engine()
# df = pd.DataFrame([{'a':1,'b':2}])
# print(df)
# df.to_sql('teste', engine, if_exists='replace', index=False)


# print(__file__.split('\\')[-1])
# print(datetime.datetime(2099,1,1,0,0,0))

mail = auxtools.MailAux()
mail.send_mail('Teste','Teste MailAux','Teste','mateusricardo94@gmail.com','mateus.ricardo@mobly.com.br')
import platform
import pandas as pd
import auxtools
import datetime
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'


engine = auxtools.MySQLAux('MOVIE').engine()
df = pd.DataFrame([{'id_execution':0,
	'start':datetime.datetime.now(),
	'finish':datetime.datetime.now(),
	's3_link':'NA',
	'status':'SUCCESS'}])
df = df[['id_execution','start','finish','s3_link','status']]
df.to_sql('control_table', engine,
              if_exists='replace', index=False)

# print(df)
# local_file = TMP+'teste_public.csv'
# remote_file = 'logs/teste_public.csv'
# df.to_csv(local_file,index=False)
# s3 = auxtools.S3Aux('stone-project')
# s3.upload(local_file,remote_file,public=True)

# min_date = str(datetime.date.today() - datetime.timedelta(days=20*365))
# print('2019-01-08'>min_date)
# print('1019-01-08'>min_date)


# print(__file__.split('\\')[-1])
# print(datetime.datetime(2099,1,1,0,0,0))

# mail = auxtools.MailAux()
# mail.send_mail('Teste','Teste MailAux','Teste','mateusricardo94@gmail.com','mateus.ricardo@mobly.com.br')
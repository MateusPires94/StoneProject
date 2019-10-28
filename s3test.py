import platform
import pandas as pd
import auxtools
import datetime
if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'


df = pd.DataFrame([{'a':1,'b':2}])
print(df)
local_file = TMP+'teste_public.csv'
remote_file = 'logs/teste_public.csv'
df.to_csv(local_file,index=False)
s3 = auxtools.S3Aux('stone-project')
s3.upload(local_file,remote_file,public=True)




# print(__file__.split('\\')[-1])
# print(datetime.datetime(2099,1,1,0,0,0))

# mail = auxtools.MailAux()
# mail.send_mail('Teste','Teste MailAux','Teste','mateusricardo94@gmail.com','mateus.ricardo@mobly.com.br')
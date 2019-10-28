import pandas as pd
import requests as r
import json
import auxtools
import argparse
import platform

if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'

movie_api_file = 'movie_key.json'
api_key = auxtools.fetch_movie_api(movie_api_file)['api_key']

def args_setup():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--use", help="use controller", default=0)

    args = parser.parse_args()

    return args


args = args_setup()
use_controller = args.use

Controller = auxtools.ExecutionController('MOVIE',TMP=TMP,use_controller=use_controller)

def main():
    genres_url = 'https://api.themoviedb.org/3/genre/movie/list?api_key={}&language=en-US'.format(api_key)
    genre_table_name = 'genres_options'
    results = json.loads(r.get(genres_url).text)
    genres_list = []
    for genre in results['genres']:
        genres_list.append(genre)
    df_genres = pd.DataFrame(genres_list)
    df_genres = df_genres[['id','name']]
    df_genres.columns = ['id_genres','name']
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_genres.to_sql(genre_table_name, engine,
                  if_exists='replace', index=False)
if __name__ == '__main__':
    script_name = __file__.split('\\')[-1]
    Controller.write_to_log('starting script {}'.format(script_name))
    if Controller.last_status != 'FAILED':    
        try:
            main()
            Controller.write_to_log('finishing script {}'.format(script_name))
        except Exception as e:
            Controller.set_to_fail()
            Controller.write_to_log('Error trying to run script {}'.format(script_name))
            Controller.write_to_log(e)
            mail = auxtools.MailAux()
            mail.send_mail('STONE-PROJECT-ERROR','Baixe o log aqui: {}'.format(Controller.s3_link),'MOVIES','mateusricardo94@gmail.com','mateus.ricardo@mobly.com.br')
    else:
        Controller.write_to_log('skipping script {} due previous error'.format(script_name))
    Controller.send_log_to_s3()
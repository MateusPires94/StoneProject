import pandas as pd
import requests as r
import json
import auxtools
import numbers
import time
import argparse
import platform

if platform.system() == 'Linux':
    TMP = '/tmp/'
else:
    TMP = 'C:/Users/mateus.ricardo/Desktop/tmp/'

movie_api_file = 'movie_key.json'
api_key = auxtools.fetch_movie_api(movie_api_file)['api_key']

def generate_auxiliar_bridges(x,bridge_field,aux_list):
    for element in x[bridge_field]:
        aux_dict = {}
        aux_dict['fk_movie'] = x['id_movie']
        aux_dict['fk_'+bridge_field] = element
        aux_list.append(aux_dict)

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
    # --- PARTE 1 : Carregando IDs dos Filmes nas APIS de now-playing e upcoming --- #
    get_crew_and_cast_url = 'https://api.themoviedb.org/3/movie/{}/credits?api_key={}'
    cast_table_name = 'current_cast_credits'
    crew_table_name = 'current_crew_credits'
    get_movie_url = 'https://api.themoviedb.org/3/movie/{}?api_key={}&language=en-US'
    get_all_movies_dict = {'now_playing':'https://api.themoviedb.org/3/movie/now_playing?api_key={}&language=en-US&page={}&region=US',
                  'upcoming':'https://api.themoviedb.org/3/movie/upcoming?api_key={}&language=en-US&page={}&region=US'}

    id_list = []
    for k,v in get_all_movies_dict.items():
        page = '1'
        url = v.format(api_key,page)
        results = json.loads(r.get(url).text)
        print(results['total_results'])
        pages = int(results['total_pages'])
        for page in range(1,pages+1):
            url = v.format(api_key,page)
            results = json.loads(r.get(url).text)
            for movie in results['results']:
                id_dict = {}
                id_dict['id_movie'] = movie['id']
                id_dict['type'] = k
                id_list.append(id_dict)
    df_movies_id = pd.DataFrame(id_list)
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_movies_id.to_sql('aux_movie_types', engine,
                  if_exists='replace', index=False)

    # --- PARTE 1 : Carregando IDs dos Filmes nas APIS de now-playing e upcoming --- #

    # --- PARTE 2 : Iterando IDs dos Filmes para puxar cast e crew da API  --- #
    crew_list = []
    cast_list = []
    for i,movie_id in enumerate(df_movies_id['id_movie'].unique()):
        print('{}/{}'.format(i+1,len(df_movies_id)))
        url = get_crew_and_cast_url.format(movie_id,api_key)
        results = r.get(url)
        code = results.status_code
        while code == 429:
            time.sleep(2)
            results = r.get(url)
            code = results.status_code
            print(code)
        results = json.loads(results.text)
        for cast in results['cast']:
            cast['fk_movie'] = movie_id
            cast_list.append(cast)
        for crew in results['crew']:
            crew['fk_movie'] = movie_id
            crew_list.append(crew)

    df_cast = pd.DataFrame(cast_list).sort_values('cast_id')
    df_crew = pd.DataFrame(crew_list).sort_values('credit_id')

    df_cast = df_cast[['credit_id','cast_id','fk_movie','character','order','id']]
    df_cast.columns = ['id_credit','id_cast','fk_movie','character','order','fk_person']
    df_crew = df_crew[['credit_id','fk_movie','job','department','id']]
    df_crew.columns = ['id_credit','fk_movie','job','department','fk_person']
    # --- PARTE 2 : Iterando IDs dos Filmes para puxar cast e crew da API  --- #

    # --- PARTE 3 : Carregando dados nas tabelas  --- #
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_cast.to_sql(cast_table_name, engine,
                  if_exists='replace', index=False)
    df_crew.to_sql(crew_table_name, engine,
                  if_exists='replace', index=False)
    # --- PARTE 3 : Carregando dados nas tabelas  --- #

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
            Controller.send_mail()
    else:
        Controller.write_to_log('skipping script {} due previous error'.format(script_name))
    Controller.send_log_to_s3()
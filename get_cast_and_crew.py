import pandas as pd
import requests as r
import json
import auxtools
import time
import argparse
import datetime
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
    years = 20
    get_crew_and_cast_url = 'https://api.themoviedb.org/3/person/{}/movie_credits?api_key={}&language=en-US'
    cast_table_name = 'historical_cast_credits'
    crew_table_name = 'historical_crew_credits'

    # --- PARTE 1 : Consultando IDs das Pessoas  --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = 'SELECT DISTINCT fk_person from current_cast_credits UNION ALL SELECT DISTINCT fk_person from current_crew_credits'
    df_person_id = pd.read_sql(query,cnx)
    cnx.close()
    # --- PARTE 1 : Consultando IDs das Pessoas  --- #

    # --- PARTE 2 : Iterando IDs das pessoas para puxar cast e crew da API  --- #
    crew_list = []
    cast_list = []
    min_date = str(datetime.date.today() - datetime.timedelta(days=years*365))
    for i,id_person in enumerate(df_person_id['fk_person'].unique()):
        print('{}/{} - {}'.format(i+1,len(df_person_id),id_person))
        url = get_crew_and_cast_url.format(id_person,api_key)
        results = r.get(url)
        code = results.status_code
        while code == 429:
            time.sleep(2)
            results = r.get(url)
            code = results.status_code
            print(code)
        results = json.loads(results.text)
        for cast in results['cast']:
            cast['fk_person'] = id_person
            if 'release_date' in cast:
                if cast['release_date']>min_date:
                    cast_list.append(cast)
        for crew in results['crew']:
            crew['fk_person'] = id_person
            if 'release_date' in crew:
                if crew['release_date']>min_date:
                    crew_list.append(crew)

    df_cast = pd.DataFrame(cast_list).sort_values('credit_id')
    df_crew = pd.DataFrame(crew_list).sort_values('credit_id')

    df_cast = df_cast[['credit_id','id','character','fk_person']]
    df_cast.columns = ['id_credit','fk_movie','character','fk_person']
    df_crew = df_crew[['credit_id','id','job','department','fk_person']]
    df_crew.columns = ['id_credit','fk_movie','job','department','fk_person']
    # --- PARTE 2 : Iterando IDs das pessoas para puxar cast e crew da API  --- #


    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = '''SELECT DISTINCT fk_person from current_crew_credits where job='Director' '''
    director_list = list(pd.read_sql(query,cnx)['fk_person'].unique())
    cnx.close()
    df_cast['currently_directing'] = df_cast['fk_person'].apply(lambda x: 1 if x in director_list else 0)
    df_crew['currently_directing'] = df_crew['fk_person'].apply(lambda x: 1 if x in director_list else 0)
    # --- PARTE 4 : Carregando dados nas tabelas  --- #
    index_list = ['fk_movie','fk_person']
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_cast.to_sql(cast_table_name, engine,
                  if_exists='replace', index=False)
    auxtools.MySQLAux("MOVIE").create_indexes(cast_table_name,index_list)
    df_crew.to_sql(crew_table_name, engine,
                  if_exists='replace', index=False)
    auxtools.MySQLAux("MOVIE").create_indexes(crew_table_name,index_list)
    # --- PARTE 4 : Carregando dados nas tabelas  --- #


if __name__ == '__main__':
    script_name = __file__.split('\\')[-1]
    Controller.write_to_log('starting script {}'.format(script_name))
    if Controller.last_status != 'FAILED':    
        try:
            main()
            Controller.write_to_log('finishing script {}'.format(script_name))
        except Exception as e:
            print(e)
            Controller.set_to_fail()
            Controller.write_to_log('Error trying to run script {}'.format(script_name))
            Controller.write_to_log(e)
            Controller.send_mail()
    else:
        Controller.write_to_log('skipping script {} due previous error'.format(script_name))
    Controller.send_log_to_s3()
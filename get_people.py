import pandas as pd
import requests as r
import json
import auxtools
import time
import argparse

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

Controler = auxtools.ExecutionController('MOVIE',use_controller=use_controller)

def main():
    people_table_name = 'current_people_info'
    get_person_url = 'https://api.themoviedb.org/3/person/{}?api_key={}&language=en-US'

    # --- PARTE 1 : Consultando todas as pessoas existentes na tabela de cast e de crew  --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = 'SELECT DISTINCT fk_person from current_cast_credits UNION ALL SELECT DISTINCT fk_person from current_crew_credits'
    df_person_id = pd.read_sql(query,cnx)
    cnx.close()
    all_people = list(df_person_id['fk_person'].unique())
    # --- PARTE 1 : Consultando todas as pessoas existentes na tabela de cast e de crew  --- #

    # --- PARTE 2 : Iterando pessoas e puxando dados da API  --- #
    people_list = []
    for i,person_id in enumerate(all_people):
        url = get_person_url.format(person_id,api_key)
        print('{}/{}'.format(i+1,len(all_people)))
        results = r.get(url)
        code = results.status_code
        while code == 429:
            time.sleep(2)
            results = r.get(url)
            code = results.status_code
            print(code)
        results = json.loads(results.text)
        people_list.append(results)
    df_people = pd.DataFrame(people_list)
    # --- PARTE 2 : Iterando pessoas e puxando dados da API  --- #

    # --- PARTE 3 : Guardando dados de pessoas em tabela do banco  --- #
    df_people['id_person'] = df_people['id']
    df_people = df_people[['id_person']+[z for z in df_people.columns if z not in ['id_person','id']]]
    df_people['also_known_as'] = df_people['also_known_as'].apply(str)
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_people.to_sql(people_table_name, engine,
                  if_exists='replace', index=False)
    # --- PARTE 3 : Guardando dados de pessoas em tabela do banco  --- #
if __name__ == '__main__':
    main()
import pandas as pd
import requests as r
import json
import auxtools
import numbers
import time
import argparse

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

Controler = auxtools.ExecutionController('MOVIE',use_controller=use_controller)

def main(): 

    # --- PARTE 1 : Trazendo Filmes históricos de todo o elenco presente nos filmes atuais --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = 'SELECT DISTINCT fk_movie from historical_cast_credits UNION ALL SELECT DISTINCT fk_movie from historical_cast_credits'
    df_movies_id = pd.read_sql(query,cnx)
    cnx.close()
    # --- PARTE 1 : Trazendo Filmes históricos de todo o elenco presente nos filmes atuais --- #

    # --- PARTE 2 : Iterando na API de detalhes dos filmes e armazenando os dados em um DataFrame --- #
    movie_list = []
    for i,movie_id in enumerate(df_movies_id['fk_movie'].unique()):
        print('{}/{}'.format(i+1,len(df_movies_id)))
        url = get_movie_url.format(movie_id,api_key)
        results = r.get(url)
        code = results.status_code
        while code == 429:
            time.sleep(2)
            results = r.get(url)
            code = results.status_code
            print(code)
        results = json.loads(results.text)
        results['type'] = id_dict_aux[movie_id]
        movie_list.append(results)
    df_movies = pd.DataFrame(movie_list)
    # --- PARTE 2 : Iterando na API de detalhes dos filmes e armazenando os dados em um DataFrame --- #

    # --- PARTE 3 : Pequeno tratamento dos dados --- #
    df_movies['belongs_to_collection'] = df_movies['belongs_to_collection'].apply(lambda x: int(x['id']) if not pd.isnull(x) else x)
    df_movies['genres'] = df_movies['genres'].apply(lambda x: [int(y['id']) for y in x] if not isinstance(x, numbers.Number) else [])
    df_movies['production_companies'] = df_movies['production_companies'].apply(lambda x: [int(y['id']) for y in x] if not isinstance(x, numbers.Number) else [])
    df_movies['production_countries'] = df_movies['production_countries'].apply(lambda x: [y['name'] for y in x] if not isinstance(x, numbers.Number) else [])
    df_movies['spoken_languages'] = df_movies['spoken_languages'].apply(lambda x: [y['name'] for y in x] if not isinstance(x, numbers.Number) else [])
    df_movies['production_companies_count'] = df_movies['production_companies'].apply(lambda x:len(x))
    df_movies['profit'] = df_movies['revenue'] - df_movies['budget']  
    df_movies['revenue_per_company'] = df_movies.apply(lambda x:x['revenue']/x['production_companies_count'] if x['production_companies_count']>1 else x['revenue'],axis=1)
    df_movies['budget_per_company'] = df_movies.apply(lambda x:x['budget']/x['production_companies_count'] if x['production_companies_count']>1 else x['budget'],axis=1)
    df_movies['profit_per_company'] = df_movies.apply(lambda x:x['profit']/x['production_companies_count'] if x['production_companies_count']>1 else x['profit'],axis=1)
    df_movies['id_movie'] = df_movies['id']
    df_movies = df_movies[['id_movie']+[z for z in df_movies.columns if z not in ['id_movie','id']]]
    # --- PARTE 3 : Pequeno tratamento dos dados --- #

    # --- PARTE 4 : Aplicando flag de tipo do filme('now_playing','upcoming','old') --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = '''SELECT * FROM aux_movie_types '''
    df_movie_types = pd.read_sql(query,cnx)
    cnx.close()
    df_movies = pd.merge(df_movies,df_movie_types,how='left',on='id_movie')
    df_movies['type'] = df_movies['type'].fillna('old')
    # --- PARTE 4 : Aplicando flag de tipo do filme('now_playing','upcoming','old') --- # 

    # --- PARTE 5 : Criando tabelas no banco de links entre filmes e atributos de gênero, empresas produtoras e países produtores --- #

    engine = auxtools.MySQLAux("MOVIE").engine()
    bridge_fields = ['genres','production_countries','production_companies']
    for bridge_field in bridge_fields:
        aux_list = []
        table_name = '{}_link_{}'.format(movie_table_name,bridge_field)
        print(table_name)
        df_movies.apply(lambda x:generate_auxiliar_bridges(x,bridge_field,aux_list),axis=1)
        df_bridge = pd.DataFrame(aux_list)
        df_bridge.to_sql(table_name, engine, if_exists='replace', index=False)

    # --- PARTE 5 : Criando tabelas de links entre filmes e atributos de gênero, empresas produtoras e países produtores --- #

    # --- PARTE 6 : Armazenando dados dos filmes no banco --- #
    df_movies = df_movies[[ for x in df_movies.columns if z not in bridge_field]]
    stringfy_columns = ['belongs_to_collection','spoken_languages']
    for column in stringfy_columns:
        df_movies[column] = df_movies[column].apply(str)

    df_movies.to_sql(movie_table_name, engine, if_exists='replace', index=False)
    # --- PARTE 6 : Armazenando dados dos filmes no banco --- #
if __name__ == '__main__':
    main()
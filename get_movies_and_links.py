import pandas as pd
import requests as r
import json
import auxtools

movie_api_file = 'movie_key.json'
api_key = auxtools.fetch_movie_api(movie_api_file)['api_key']

def generate_auxiliar_bridges(x,bridge_field,aux_list):
    for element in x[bridge_field]:
        aux_dict = {}
        aux_dict['fk_movie'] = x['id_movie']
        aux_dict['fk_'+bridge_field] = element
        aux_list.append(aux_dict)

def main():
    # --- PARTE 1 : Carregando IDs dos Filmes nas APIS de now-playing e upcoming --- #
    movie_table_name = 'movies_detail'
    get_movie_url = 'https://api.themoviedb.org/3/movie/{}?api_key={}&language=en-US'
    get_all_movies_dict = {'now_playing':'https://api.themoviedb.org/3/movie/now_playing?api_key={}&language=en-US&page={}&region=US',
                  'upcoming':'https://api.themoviedb.org/3/movie/upcoming?api_key={}&language=en-US&page={}&region=US'}

    id_list = []
    id_dict_aux={}
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
                id_dict['id'] = movie['id']
                id_dict['type'] = k
                id_list.append(id_dict)
                id_dict_aux[movie['id']] = k
    df_movies_id = pd.DataFrame(id_list)
    # --- PARTE 1 : Carregando IDs dos Filmes nas APIS de now-playing e upcoming --- #

    # --- PARTE 2 : Iterando na API de detalhes dos filmes e armazenando os dados em um DataFrame --- #
    movie_list = []
    for i,movie_id in enumerate(df_movies_id['id'].unique()):
        print('{}/{}'.format(i+1,len(df_movies_id)))
        url = get_movie_url.format(movie_id,api_key)
        results = json.loads(r.get(url).text)
        results['type'] = id_dict_aux[movie_id]
        movie_list.append(results)
    df_movies = pd.DataFrame(movie_list)
    # --- PARTE 2 : Iterando na API de detalhes dos filmes e armazenando os dados em um DataFrame --- #

    # --- PARTE 3 : Pequeno tratamento dos dados --- #
    df_movies['belongs_to_collection'] = df_movies['belongs_to_collection'].apply(lambda x: int(x['id']) if not pd.isnull(x) else x)
    df_movies['genres'] = df_movies['genres'].apply(lambda x: [int(y['id']) for y in x] if not pd.isnull(x) else x)
    df_movies['production_companies'] = df_movies['production_companies'].apply(lambda x: [int(y['id']) for y in x] if not pd.isnull(x) else x)
    df_movies['production_countries'] = df_movies['production_countries'].apply(lambda x: [y['name'] for y in x] if not pd.isnull(x) else x)
    df_movies['spoken_languages'] = df_movies['spoken_languages'].apply(lambda x: [y['name'] for y in x] if not pd.isnull(x) else x)

    df_movies['id_movie'] = df_movies['id']
    df_movies = df_movies[['id_movie']+[z for z in df_movies.columns if z not in ['id_movie','id']]]
    # --- PARTE 3 : Pequeno tratamento dos dados --- #

    # --- PARTE 4 : Criando tabelas no banco de links entre filmes e atributos de gênero, empresas produtoras e países produtores --- #

    engine = auxtools.MySQLAux("MOVIE").engine()
    bridge_fields = ['genres','production_countries','production_companies']
    for bridge_field in bridge_fields:
        aux_list = []
        table_name = '{}_link_{}'.format(movie_table_name,bridge_field)
        print(table_name)
        df_movies.apply(lambda x:generate_auxiliar_bridges(x,bridge_field,aux_list),axis=1)
        df_bridge = pd.DataFrame(aux_list)
        df_bridge.to_sql(table_name, engine,
                      if_exists='replace', index=False)

    # --- PARTE 4 : Criando tabelas de links entre filmes e atributos de gênero, empresas produtoras e países produtores --- #

    # --- PARTE 5 : Armazenando dados dos filmes no banco --- #
    stringfy_columns = ['belongs_to_collection','genres','production_companies','production_countries','spoken_languages']
    for column in stringfy_columns:
        df_movies[column] = df_movies[column].apply(str)

    df_movies.to_sql(movie_table_name, engine,
                  if_exists='replace', index=False)
    # --- PARTE 5 : Armazenando dados dos filmes no banco --- #
if __name__ == '__main__':
    main()
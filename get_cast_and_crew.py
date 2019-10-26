import pandas as pd
import requests as r
import json
import auxtools

movie_api_file = 'movie_key.json'
api_key = auxtools.fetch_movie_api(movie_api_file)['api_key']

def main():
    get_crew_and_cast_url = 'https://api.themoviedb.org/3/movie/{}/credits?api_key={}'
    cast_table_name = 'cast_credits'
    crew_table_name = 'crew_credits'

    # --- PARTE 1 : Consultando IDs dos Filmes  --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = 'SELECT DISTINCT id_movie from movies_detail'
    df_movies_id = pd.read_sql(query,cnx)
    cnx.close()
    # --- PARTE 1 : Consultando IDs dos Filmes  --- #

    # --- PARTE 2 : Iterando IDs dos Filmes para puxar cast e crew da API  --- #
    crew_list = []
    cast_list = []
    for i,movie_id in enumerate(df_movies_id['id_movie'].unique()):
        print('{}/{}'.format(i+1,len(df_movies_id)))
        url = get_crew_and_cast_url.format(movie_id,api_key)
        results = json.loads(r.get(url).text)
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
    main()
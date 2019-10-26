import pandas as pd
import requests as r
import json
import auxtools

movie_api_file = 'movie_key.json'
api_key = auxtools.fetch_movie_api(movie_api_file)['api_key']

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
    main()
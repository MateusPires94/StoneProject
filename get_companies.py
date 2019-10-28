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
    production_companies_table_name = 'production_companies_options'
    get_companies_url = 'https://api.themoviedb.org/3/company/{}?api_key={}'

    # --- PARTE 1 : Consultando empresas existentes  --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = 'SELECT DISTINCT fk_production_companies from movies_detail_link_production_companies'
    df_production_companies_id = pd.read_sql(query,cnx)
    cnx.close()
    # --- PARTE 1 : Consultando empresas existentes  --- #

    # --- PARTE 2 : Iterando empresas e puxando dados  --- #
    all_companies = list(df_production_companies_id['fk_production_companies'].unique())
    companies_list = []
    for i,comp_id in enumerate(all_companies):
        url = get_companies_url.format(comp_id,api_key)
        print('{}/{}'.format(i+1,len(all_companies)))
        results = r.get(url)
        code = results.status_code
        while code == 429:
            time.sleep(2)
            results = r.get(url)
            code = results.status_code
            print(code)
        results = json.loads(results.text)
        companies_list.append(results)
    df_companies = pd.DataFrame(companies_list)
    # --- PARTE 2 : Iterando empresas e puxando dados  --- #

    # --- PARTE 3 : Carregando dados em tabela do banco  --- #
    df_companies['id_production_companies'] = df_companies['id']
    df_companies = df_companies[['id_production_companies']+[z for z in df_companies.columns if z not in ['id_production_companies','id']]]
    engine = auxtools.MySQLAux("MOVIE").engine()
    df_companies.to_sql(production_companies_table_name, engine,
                  if_exists='replace', index=False)
    # --- PARTE 3 : Carregando dados em tabela do banco  --- #
if __name__ == '__main__':
    main()
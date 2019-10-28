import pandas as pd
import requests as r
import json
import auxtools
import time
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
    production_companies_table_name = 'current_production_companies_options'
    get_companies_url = 'https://api.themoviedb.org/3/company/{}?api_key={}'

    # --- PARTE 1 : Consultando empresas existentes e presentes apenas em filmes em cartas ou em filmes que serão lançados em breve   --- #
    cnx = auxtools.MySQLAux('MOVIE').connect()
    query = '''SELECT DISTINCT fk_production_companies from movies_detail_link_production_companies pc
                JOIN aux_movie_types mt on mt.id_movie = pc.fk_movie
    '''
    df_production_companies_id = pd.read_sql(query,cnx)
    cnx.close()
    # --- PARTE 1 : Consultando empresas existentes e presentes apenas em filmes em cartas ou em filmes que serão lançados em breve   --- #

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
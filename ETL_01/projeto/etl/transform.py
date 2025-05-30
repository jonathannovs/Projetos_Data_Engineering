import pandas as pd
import datetime as dt
import re
from sqlalchemy import create_engine

class Etl:
    Base_path = '/opt/airflow/data'

    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.path = f'{self.Base_path}/{csv_file}'

    def extract(self):
        try:
            return pd.read_csv(self.path)
        except Exception as e:
            print(f'[extract] Erro ao ler CSV {self.path}: {e}')
            return None

    def transform_clientes(self):
        df = self.extract()
        if df is not None:
            try:
                df['cpf'] = df['cpf'].str.replace('.', '', regex=True).str.replace('-', '', regex=True).str.strip().str.zfill(11)
                remover = ['Dr.', 'Sr.', 'Dra.', 'Srta.','Sra.']
                for r in remover:
                    df['nome'] = df['nome'].str.replace(r, '', regex=False)
                df['nome'] =  df['nome'].str.upper().str.strip()
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_clientes] Erro ao transformar: {e}')
        return None

    def transform_empresas(self):
        df = self.extract()
        if df is not None:
            try:
                df['cnpj'] = df['cnpj'].astype(str).str.replace(r'\D', '', regex=True).str.strip().str.zfill(14)
                df['nome_fantasia'] =  df['nome_fantasia'].str.upper().str.strip()
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_empresas] Erro ao transformar: {e}')
        return None
    
    def transform_despesas(self):
        df = self.extract()
        if df is not None:
            try:
                df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_despesas] Erro ao transformar: {e}')
        return None
    
    def transform_receitas(self):
        df = self.extract()
        if df is not None:
            try:
                df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_receitas] Erro ao transformar: {e}')
        return None

    def transform_orcamentos(self):
        df = self.extract()
        if df is not None:
            try:
                df['valor_estimado'] = pd.to_numeric(df['valor_estimado'], errors='coerce').fillna(0).astype('float')
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_orcamentos] Erro ao transformar: {e}')
        return None



    def transform_transferencias(self):
        df = self.extract()
        if df is not None:
            try:
                df['data'] = pd.to_datetime(df['data'], format='%Y-%m-%d')
                df['dt_ingest'] = dt.datetime.now().strftime('%Y-%m-%d')
                return df
            except Exception as e:
                print(f'[transform_transferencias] Erro ao transformar: {e}')
        return None

    def run_transform(self):
        try:
            file = self.csv_file.replace('.csv', '')
            func_file = f'transform_{file}'
            if hasattr(self, func_file):
                return getattr(self, func_file)()
            return self.extract()
        except Exception as e:
            print(f'[run_transform] Erro ao executar transformação: {e}')
            return None


class Load:
    def __init__(self, etl: Etl, table_name: str, db: str, user: str, password: str):
        self.etl = etl
        self.table_name = table_name
        self.db = db
        self.user = user
        self.password = password

    def load_to_db(self):
        df = self.etl.run_transform()
        if df is None:
            print(f'[load_to_db] Nenhum dado a carregar para a tabela {self.table_name}. Verifique o arquivo CSV.')
            return
        try:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@postgres:5432/{self.db}')
            print(f'[load_to_db] DataFrame shape: {df.shape}')
            df.to_sql(self.table_name, engine, if_exists='append', index=False, schema='transacional')
            print(f'[load_to_db] Dados carregados com sucesso na tabela {self.table_name}')
        except Exception as e:
            print(f'[load_to_db] Erro ao carregar dados no banco: {e}')

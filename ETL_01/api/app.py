# pip install fastapi uvicorn sqlalchemy psycopg2-binary pydantic

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, select, text
from datetime import date
import uvicorn


DB_HOST = "localhost"  
DB_PORT = "5432"
DB_NAME = "RZK"
DB_USER = "JONANOV"
DB_PASSWORD = "teste123"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
metadata = MetaData()


empresas = Table(
    "empresas",
    metadata,
    Column("id_empresa", Integer, primary_key=True),
    Column("nome_fantasia", String),
    Column("cnpj", String),
    Column("data_fundacao", Date),
    Column("dt_ingest", Date),
    schema="relacional"
)

class Empresa(BaseModel):
    id_empresa: int
    nome_fantasia: Optional[str]
    cnpj: Optional[str]
    data_fundacao: Optional[date] 
    dt_ingest: Optional[date]

    class Config:
        orm_mode = True  


app = FastAPI()

@app.get("/")
def raiz():
    return {"mensagem": "API Empresas ativa"}

@app.get("/empresas", response_model=List[Empresa])
def get_all_empresas():
    with engine.connect() as conn:
        query = select(empresas)
        result = conn.execute(query)
       
        return [dict(row._mapping) for row in result]

@app.get("/empresas/{id_empresa}", response_model=Empresa)
def get_empresa_by_id(id_empresa: int):
    with engine.connect() as conn:
        query = select(empresas).where(empresas.c.id_empresa == id_empresa)
        result = conn.execute(query).first()
        if result is None:
            raise HTTPException(status_code=404, detail="Empresa não encontrada")
        return dict(result._mapping)

def consulta_view(view_path: str):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {view_path}"))
        return [dict(row._mapping) for row in result]

@app.get("/analitico/evolucao_receita")
def get_evolucao_receita():
    return consulta_view("analitico.view_evolucao_receita")

@app.get("/analitico/gastos_salarios")
def get_gastos_salarios():
    return consulta_view("analitico.view_gastos_salarios")

@app.get("/analitico/lucro_mes")
def get_lucro_mes():
    return consulta_view("analitico.view_lucro_mes")

@app.get("/analitico/maiores_despesas")
def get_maiores_despesas():
    return consulta_view("analitico.view_maiores_despesas")

@app.get("/analitico/melhores_clientes")
def get_melhores_clientes():
    return consulta_view("analitico.view_melhores_clientes")

@app.get("/analitico/orcamento_consolidado")
def get_orcamento_consolidado():
    return consulta_view("analitico.view_orcamento_consolidado")

@app.get("/analitico/transferencias_mensal")
def get_transferencias_mensal():
    return consulta_view("analitico.view_transferencias_mensal")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

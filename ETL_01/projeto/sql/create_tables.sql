CREATE SCHEMA IF NOT EXISTS transacional;

CREATE TABLE IF NOT EXISTS transacional.Clientes (
  id_cliente SERIAL NOT NULL PRIMARY KEY,
  nome VARCHAR(100),
  email VARCHAR(100),
  cpf VARCHAR(11),
  dt_ingest DATE
);

CREATE TABLE IF NOT EXISTS transacional.Empresas (
  id_empresa SERIAL NOT NULL PRIMARY KEY,
  nome_fantasia VARCHAR(100),
  cnpj VARCHAR(14),
  data_fundacao DATE,
  dt_ingest DATE
);

CREATE TABLE IF NOT EXISTS transacional.Despesas (
  id_despesa SERIAL NOT NULL PRIMARY KEY,
  id_empresa INT,
  id_cliente INT,
  categoria VARCHAR(50),
  valor NUMERIC(10, 2),
  data DATE,
  descricao TEXT,
  dt_ingest DATE,
  FOREIGN KEY (id_cliente) REFERENCES transacional.Clientes(id_cliente),
  FOREIGN KEY (id_empresa) REFERENCES transacional.Empresas(id_empresa)
);

CREATE TABLE IF NOT EXISTS transacional.Receitas (
  id_receita SERIAL NOT NULL PRIMARY KEY,
  id_empresa INT,
  id_cliente INT,
  categoria VARCHAR(50),
  valor NUMERIC(10, 2),
  data DATE,
  descricao TEXT,
  dt_ingest DATE,
  FOREIGN KEY (id_cliente) REFERENCES transacional.Clientes(id_cliente),
  FOREIGN KEY (id_empresa) REFERENCES transacional.Empresas(id_empresa)
);

CREATE TABLE IF NOT EXISTS transacional.Orcamentos (
  id_orcamento SERIAL NOT NULL PRIMARY KEY,
  id_empresa INT,
  ano INT,
  mes INT,
  tipo VARCHAR(50),
  valor_estimado NUMERIC(10, 2),
  dt_ingest DATE,
  FOREIGN KEY (id_empresa) REFERENCES transacional.Empresas(id_empresa)
);

CREATE TABLE IF NOT EXISTS transacional.Transferencias (
  id_transferencia SERIAL NOT NULL PRIMARY KEY,
  id_empresa_origem INT,
  id_empresa_destino INT,
  tipo VARCHAR(50),
  valor NUMERIC(10, 2),
  data DATE,
  descricao TEXT,
  dt_ingest DATE,
  FOREIGN KEY (id_empresa_origem) REFERENCES transacional.Empresas(id_empresa),
  FOREIGN KEY (id_empresa_destino) REFERENCES transacional.Empresas(id_empresa)
);

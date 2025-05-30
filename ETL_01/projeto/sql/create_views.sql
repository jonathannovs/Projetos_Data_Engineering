
CREATE SCHEMA IF NOT EXISTS analitico;
CREATE OR REPLACE VIEW analitico.view_lucro_mes AS
-- 1 - ######### lucro
WITH receitas AS (
SELECT
    id_empresa,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano,
    SUM(valor) AS total_receita
FROM transacional.receitas
GROUP BY id_empresa, EXTRACT(MONTH FROM data), EXTRACT(YEAR FROM data)
),
despesas AS (
SELECT
    id_empresa,
    EXTRACT(MONTH FROM data) AS mes,
    EXTRACT(YEAR FROM data) AS ano,
    SUM(valor) AS total_despesa
FROM transacional.despesas
GROUP BY id_empresa, EXTRACT(MONTH FROM data), EXTRACT(YEAR FROM data)
),
lucro_mes AS (
SELECT
    r.id_empresa,
    r.mes,
    r.ano,
    r.total_receita,
    d.total_despesa,
    r.total_receita - d.total_despesa AS lucro
FROM receitas r
INNER JOIN despesas d
    ON r.id_empresa = d.id_empresa AND r.mes = d.mes AND r.ano = d.ano
)
select
	lm.id_empresa,
	e.cnpj,
	e.nome_fantasia,
	lm.mes,
	lm.ano,
	lm.total_receita,
	lm.total_despesa,
	lm.lucro
from lucro_mes lm
inner join transacional.empresas e
on lm.id_empresa = e.id_empresa;

-- 2 - Melhores Clientes
CREATE OR REPLACE VIEW analitico.view_melhores_clientes AS
WITH base_top_clientes AS (
    SELECT 
        e.nome_fantasia,
        e.cnpj,
        e.id_empresa,
        r.id_receita,
        r.id_cliente AS id_cliente_r,
        r.categoria AS categoria_receita,
        r.valor AS valor_receita,
        r.data AS data_receita,
        c.id_cliente,
        c.nome,
        c.cpf
    FROM transacional.empresas e
    LEFT JOIN transacional.receitas r ON e.id_empresa = r.id_empresa
    LEFT JOIN transacional.clientes c ON r.id_cliente = c.id_cliente
    WHERE r.categoria = 'Venda Produto'
),
receita_cliente AS (
    SELECT
        id_empresa,
        id_cliente_r,
        MAX(cnpj) AS cnpj,
        MAX(nome_fantasia) AS nome_fantasia,
        MAX(nome) AS nome_cliente,
        MAX(cpf) AS cpf,
        SUM(valor_receita) AS receita_total,
        MAX(valor_receita) AS compra_maxima,
        ROUND(AVG(valor_receita), 2) AS ticket_medio,
        COUNT(valor_receita) AS qtd_compras
    FROM base_top_clientes 
    GROUP BY id_empresa, id_cliente_r
),
ranking_receitas AS (
    SELECT *,
        DENSE_RANK() OVER(PARTITION BY id_empresa ORDER BY receita_total DESC) AS rank_cliente_receita 
    FROM receita_cliente
)
SELECT * FROM ranking_receitas;


-- 3 - Maiores despesas
CREATE OR REPLACE VIEW analitico.view_maiores_despesas AS

with base_despesas as(
select 
	e.nome_fantasia,
	e.cnpj,
	e.id_empresa,
	d.id_despesa,
	d.categoria,
	d.valor as valor_despesa,
	d.data as data_despesa,
	c.id_cliente,
	c.nome,
	c.cpf
	from transacional.empresas e
inner join transacional.despesas d on e.id_empresa = d.id_empresa
inner join  transacional.clientes c on d.id_cliente = c.id_cliente
),
maiores_despesas as (
select
	id_empresa,
    id_cliente,
    categoria,
    MAX(cnpj) AS cnpj,
    MAX(nome_fantasia) AS nome_fantasia,
    MAX(nome) AS nome_cliente,
    MAX(cpf) AS cpf,
    SUM(valor_despesa) AS despesa_total,
    EXTRACT(MONTH FROM data_despesa) AS mes,
    EXTRACT(YEAR FROM data_despesa) AS ano
from base_despesas
GROUP by id_empresa, id_cliente, categoria, mes, ano
)
SELECT * FROM maiores_despesas
ORDER BY id_empresa, despesa_total DESC;


-- 4 -  maiores Salários
CREATE OR REPLACE VIEW analitico.view_gastos_salarios AS
with base_despesas as(
select 
	e.nome_fantasia,
	e.cnpj,
	e.id_empresa,
	d.id_despesa,
	d.categoria,
	d.valor as valor_despesa,
	d.data as data_despesa,
	c.id_cliente,
	c.nome,
	c.cpf
	from transacional.empresas e
inner join transacional.despesas d on e.id_empresa = d.id_empresa
inner join  transacional.clientes c on d.id_cliente = c.id_cliente
where d.categoria = 'Salário'
),
maiores_salarios as (
select
	id_empresa,
    id_cliente,
    MAX(cnpj) AS cnpj,
    MAX(nome_fantasia) AS nome_fantasia,
    MAX(nome) AS nome_funcionario,
    MAX(cpf) AS cpf,
    SUM(valor_despesa) AS total_salario,
    TO_CHAR(DATE_TRUNC('month', data_despesa),'TMMonth YYYY') as mes
from base_despesas
GROUP by id_empresa, id_cliente, mes
)
SELECT* from maiores_salarios
order by id_empresa, mes;

-- #######  5 - Evolução Receita
CREATE OR REPLACE VIEW analitico.view_evolucao_receita AS
with base_receitas as (
select 
	e.nome_fantasia,
	e.cnpj,
	e.id_empresa,
	r.id_receita,
	r.id_cliente as id_cliente_r,
	r.categoria as categoria_receita,
	r.valor as valor_receita,
	r.data as data_receita,
	c.id_cliente,
	c.nome,
	c.cpf
	from transacional.empresas e
inner join transacional.receitas r on e.id_empresa = r.id_empresa
inner join  transacional.clientes c on r.id_cliente = c.id_cliente
),
receita_mensal as (
SELECT
	id_empresa,
	max(nome_fantasia) as nome_fantasia,
	Extract(year from data_receita) as ano,
	Extract(month from data_receita) as mes,
	SUM(valor_receita) AS receita_total
from base_receitas
group by id_empresa, Extract(year from data_receita), Extract(month from data_receita)
),
evolucao as(
select
	id_empresa,
	nome_fantasia,
	ano,
	mes,
	receita_total,
	LAG(receita_total) OVER (PARTITION BY id_empresa ORDER BY ano, mes) AS receita_mes_anterior,
    receita_total - LAG(receita_total) OVER (PARTITION BY id_empresa ORDER BY ano, mes) AS evolucao_receita,
    ROUND(AVG(receita_total) over(partition by id_empresa order by ano, mes ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) as MEDIA_MOVEL_3M
from receita_mensal
)
select* from evolucao
order by id_empresa, ano, mes;

-- #########  6 - Transferências
CREATE OR REPLACE VIEW analitico.view_transferencias_mensal AS
WITH transf_empresas AS (
    SELECT 
        t.*, 
        e_origem.nome_fantasia AS nome_empresa_origem,
        e_destino.nome_fantasia AS nome_empresa_destino
    FROM transacional.transferencias t
    INNER JOIN transacional.empresas e_origem ON t.id_empresa_origem = e_origem.id_empresa
    INNER JOIN transacional.empresas e_destino ON t.id_empresa_destino = e_destino.id_empresa
),
saidas AS (
    SELECT 
        id_empresa_origem AS id_empresa,
        MAX(nome_empresa_origem) AS empresa,
        SUM(valor) AS total_transferido,
        COUNT(valor) AS qtd_transf_realizadas,
        EXTRACT(MONTH FROM data) AS mes,
        EXTRACT(YEAR FROM data) AS ano
    FROM transf_empresas
    GROUP BY id_empresa_origem, EXTRACT(MONTH FROM data), EXTRACT(YEAR FROM data)
),
entradas AS (
    SELECT 
        id_empresa_destino AS id_empresa,
        MAX(nome_empresa_destino) AS empresa,
        SUM(valor) AS total_recebido,
        COUNT(valor) AS qtd_transf_recebidas,
        EXTRACT(MONTH FROM data) AS mes,
        EXTRACT(YEAR FROM data) AS ano
    FROM transf_empresas
    GROUP BY id_empresa_destino, EXTRACT(MONTH FROM data), EXTRACT(YEAR FROM data)
)
SELECT 
    COALESCE(e.id_empresa, s.id_empresa) AS id_empresa,
    COALESCE(e.empresa, s.empresa) AS empresa,
    COALESCE(s.total_transferido, 0) AS total_transferido,
    COALESCE(s.qtd_transf_realizadas, 0) AS qtd_transf_realizadas,
    COALESCE(e.total_recebido, 0) AS total_recebido,
    COALESCE(e.qtd_transf_recebidas, 0) AS qtd_transf_recebidas,
    COALESCE(e.mes, s.mes) AS mes,
    COALESCE(e.ano, s.ano) AS ano
FROM entradas e
FULL OUTER JOIN saidas s
    ON e.id_empresa = s.id_empresa AND e.mes = s.mes AND e.ano = s.ano
ORDER BY empresa, ano, mes;

--# 7 - orcamento
CREATE OR REPLACE VIEW analitico.view_orcamento_consolidado AS
WITH orcamento_agrupado AS (
    SELECT 
        o.id_empresa,
        o.ano,
        o.mes,
        SUM(CASE WHEN o.tipo = 'Receita' THEN o.valor_estimado ELSE 0 END) AS receita_prevista,
        SUM(CASE WHEN o.tipo = 'Despesa' THEN o.valor_estimado ELSE 0 END) AS despesa_prevista
    FROM transacional.orcamentos o
    GROUP BY o.id_empresa, o.ano, o.mes
),
realizado_receitas AS (
    SELECT 
        r.id_empresa,
        EXTRACT(YEAR FROM r.data) AS ano,
        EXTRACT(MONTH FROM r.data) AS mes,
        SUM(r.valor) AS receita_realizada
    FROM transacional.receitas r
    GROUP BY r.id_empresa, EXTRACT(YEAR FROM r.data), EXTRACT(MONTH FROM r.data)
),
realizado_despesas AS (
    SELECT 
        d.id_empresa,
        EXTRACT(YEAR FROM d.data) AS ano,
        EXTRACT(MONTH FROM d.data) AS mes,
        SUM(d.valor) AS despesa_realizada
    FROM transacional.despesas d
    GROUP BY d.id_empresa, EXTRACT(YEAR FROM d.data), EXTRACT(MONTH FROM d.data)
),
empresas AS (
    SELECT id_empresa, nome_fantasia, cnpj
    FROM transacional.empresas
),
consolidado AS (
    SELECT 
        COALESCE(o.id_empresa, r.id_empresa, d.id_empresa) AS id_empresa,
        COALESCE(o.ano, r.ano, d.ano) AS ano,
        COALESCE(o.mes, r.mes, d.mes) AS mes,
        COALESCE(o.receita_prevista, 0) AS receita_prevista,
        COALESCE(o.despesa_prevista, 0) AS despesa_prevista,
        COALESCE(r.receita_realizada, 0) AS receita_realizada,
        COALESCE(d.despesa_realizada, 0) AS despesa_realizada
    FROM orcamento_agrupado o
    FULL OUTER JOIN realizado_receitas r 
        ON o.id_empresa = r.id_empresa AND o.ano = r.ano AND o.mes = r.mes
    FULL OUTER JOIN realizado_despesas d 
        ON COALESCE(o.id_empresa, r.id_empresa) = d.id_empresa 
        AND COALESCE(o.ano, r.ano) = d.ano 
        AND COALESCE(o.mes, r.mes) = d.mes
)
SELECT 
    e.nome_fantasia,
    e.cnpj,
    c.ano,
    c.mes,
    c.receita_prevista,
    c.receita_realizada,
    c.despesa_prevista,
    c.despesa_realizada,
    (c.receita_prevista - c.despesa_prevista) AS saldo_previsto,
    (c.receita_realizada - c.despesa_realizada) AS saldo_realizado
FROM consolidado c
INNER JOIN empresas e ON c.id_empresa = e.id_empresa
where  (c.receita_realizada - c.despesa_realizada) <> 0 
ORDER BY e.nome_fantasia, c.ano, c.mes;

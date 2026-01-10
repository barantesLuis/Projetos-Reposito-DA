import pandas as pd

# Caminho onde foi salvo o arquivo em parquet
caminho_parquet = r"C:\Git\Repositórios\Portfolio-DA\Projeto1_Cohort_Empresas_BR\dados_projeto_1\empresas_tratado.parquet"

# Leitura (instantânea)
df_empresas = pd.read_parquet(caminho_parquet)

# Verificando
print(df_empresas.head())
print(f"Linhas carregadas: {len(df_empresas):,}")


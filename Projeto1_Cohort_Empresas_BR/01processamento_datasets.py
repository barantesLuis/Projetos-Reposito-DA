import os
import pandas as pd
from tqdm import tqdm

# ==============================
# CONFIGURAÇÕES
# ==============================
# Ajuste o caminho se necessário, mas parece correto baseado no seu print
BASE_DIR = r"C:\Git\Repositórios\Portfolio-DA\Projeto1_Cohort_Empresas_BR\dados_projeto_1"
EXTRACT_DIR = os.path.join(BASE_DIR, "extracted")

# ==============================
# FUNÇÃO DE LEITURA
# ==============================
def ler_dados(tipo):
    # Dicionário robusto: chaves no singular para facilitar
    TIPOS_ARQUIVOS = {
        "empresa": "EMPRECSV",
        "estabelecimento": "ESTABELE",
        "socio": "SOCIOCSV",
        "cnae": "CNAECSV",
        "natureza": "NATJUCSV",
        "municipio": "MUNICCSV"
    }

    # Lógica inteligente: converte para minúsculo e remove o 's' final se houver
    # Assim funciona tanto "EMPRESA" quanto "empresas"
    chave_busca = tipo.lower().rstrip('s')
    
    prefixo = TIPOS_ARQUIVOS.get(chave_busca)

    if not prefixo:
        raise ValueError(f"Tipo inválido: {tipo} (Tente usar: empresa, socio, estabelecimento...)")

    # Verifica se a pasta existe antes de listar
    if not os.path.exists(EXTRACT_DIR):
        raise FileNotFoundError(f"A pasta não foi encontrada: {EXTRACT_DIR}")

    arquivos = [
        os.path.join(EXTRACT_DIR, f)
        for f in os.listdir(EXTRACT_DIR)
        if prefixo in f.upper()
    ]

    if not arquivos:
        raise ValueError(f"Nenhum arquivo com '{prefixo}' encontrado na pasta.")

    dfs = []
    for arquivo in tqdm(arquivos, desc=f"📊 Lendo {tipo}"):
        try:
            df = pd.read_csv(
                arquivo,
                sep=";",
                encoding="latin1",
                low_memory=False,
                header=None, # IMPORTANTE: Arquivos da Receita NÃO têm cabeçalho
                dtype=str    # IMPORTANTE: Lê tudo como texto para evitar erros
            )
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {os.path.basename(arquivo)}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)

# ==============================
# EXECUÇÃO
# ==============================
if __name__ == "__main__":
    # Definição dos nomes das colunas (Layout Receita Federal - EMPRESA)
    COLUNAS_EMPRESAS = [
        "cnpj_basico", "razao_social", "natureza_juridica", 
        "qualificacao_responsavel", "capital_social", "porte_empresa", "ente_federativo"
    ]

    try:
        print("--- Iniciando Processamento ---")
        # Agora vai aceitar "EMPRESA" sem dar erro
        df_empresas = ler_dados("EMPRESA")
        
        # Renomeia as colunas
        if not df_empresas.empty:
            df_empresas.columns = COLUNAS_EMPRESAS
            print("\n✅ Sucesso! Primeiras 5 linhas:")
            print(df_empresas.head())
            print(f"\nTotal de registros carregados: {len(df_empresas):,}")
        else:
            print("Nenhum dado foi carregado.")
            
    except Exception as e:
        print(f"\n❌ Ocorreu um erro: {e}")
        

print(len(df_empresas.columns))
print(f"No total são: {len(df_empresas.columns)}, colunas")
print(f"E as colunas são: {df_empresas.columns}")

# ==============================
# ARMAZENAMENTO EM PARQUET
# ==============================

caminho_parquet = os.path.join(BASE_DIR, "empresas_tratado.parquet")

print("💾 Salvando em Parquet...")
df_empresas.to_parquet(caminho_parquet, index=False, compression='snappy')
print(f"✅ Arquivo salvo com sucesso em: {caminho_parquet}")
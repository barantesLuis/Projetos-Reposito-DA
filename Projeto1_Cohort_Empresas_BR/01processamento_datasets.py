import os
import pandas as pd
from tqdm import tqdm

# =====================================================
# CONFIGURAÇÕES DE DIRETÓRIO
# =====================================================
BASE_DIR = r"C:\Git\Projetos-Reposito-DA\Projeto1_Cohort_Empresas_BR\dados_projeto_1"
EXTRACT_DIR = os.path.join(BASE_DIR, "extracted")

# =====================================================
# MAPEAMENTO DOS TIPOS DE ARQUIVO
# =====================================================
TIPOS_ARQUIVOS = {
    "empresa": "EMPRECSV",
    "estabelecimento": "ESTABELE",
    "socio": "SOCIOCSV",
    "cnae": "CNAECSV",
    "natureza": "NATJUCSV",
    "municipio": "MUNICCSV",
}

# =====================================================
# LAYOUTS (COLUNAS) - RECEITA FEDERAL
# =====================================================
LAYOUTS = {
    "empresa": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo",
    ],
    "estabelecimento": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao",
        "motivo_situacao",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_principal",
        "cnae_secundarios",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd1",
        "telefone1",
        "ddd2",
        "telefone2",
        "ddd_fax",
        "fax",
        "email",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "socio": [
        "cnpj_basico",
        "tipo_socio",
        "nome_socio",
        "cpf_cnpj_socio",
        "qualificacao_socio",
        "data_entrada",
        "pais",
        "cpf_representante",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ],
    "cnae": ["codigo_cnae", "descricao_cnae"],
    "natureza": ["codigo_natureza", "descricao_natureza"],
    "municipio": ["codigo_municipio", "nome_municipio"],
}


# =====================================================
# FUNÇÃO DE LEITURA DOS CSVs
# =====================================================
def ler_dados(tipo: str) -> pd.DataFrame:
    chave = tipo.lower().rstrip("s")

    if chave not in TIPOS_ARQUIVOS:
        raise ValueError(f"Tipo inválido: {tipo}")

    prefixo = TIPOS_ARQUIVOS[chave]

    if not os.path.exists(EXTRACT_DIR):
        raise FileNotFoundError(f"Pasta não encontrada: {EXTRACT_DIR}")

    arquivos = [
        os.path.join(EXTRACT_DIR, f)
        for f in os.listdir(EXTRACT_DIR)
        if prefixo in f.upper()
    ]

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {tipo}")

    dfs = []

    for arquivo in tqdm(arquivos, desc=f"📥 Lendo {tipo.upper()}"):
        try:
            df = pd.read_csv(
                arquivo,
                sep=";",
                encoding="latin1",
                header=None,
                dtype=str,
                low_memory=False,
            )
            dfs.append(df)
        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")

    return pd.concat(dfs, ignore_index=True)


# =====================================================
# EXECUÇÃO PRINCIPAL
# =====================================================
if __name__ == "__main__":
    print("\n🚀 INICIANDO PIPELINE DE DADOS\n")

    dfs = {}

    for tipo in TIPOS_ARQUIVOS.keys():
        try:
            print(f"\n🔄 Processando {tipo.upper()}")

            df = ler_dados(tipo)

            if df.empty:
                print(f"⚠️ Nenhum dado para {tipo}")
                continue

            if len(df.columns) == len(LAYOUTS[tipo]):
                df.columns = LAYOUTS[tipo]
            else:
                print(
                    f"⚠️ Layout não aplicado em {tipo} | "
                    f"Esperado: {len(LAYOUTS[tipo])} | "
                    f"Encontrado: {len(df.columns)}"
                )

            dfs[tipo] = df

            print(f"✅ {tipo}: {len(df):,} registros carregados")

        except Exception as e:
            print(f"❌ Erro no processamento de {tipo}: {e}")

    # =================================================
    # SALVAMENTO EM PARQUET
    # =================================================
    print("\n💾 SALVANDO ARQUIVOS PARQUET\n")

    for tipo, df in dfs.items():
        caminho = os.path.join(BASE_DIR, f"{tipo}_tratado.parquet")
        df.to_parquet(caminho, index=False, compression="snappy")
        print(f"✅ {tipo} salvo em: {caminho}")

    print("\n🏁 PIPELINE FINALIZADO COM SUCESSO\n")

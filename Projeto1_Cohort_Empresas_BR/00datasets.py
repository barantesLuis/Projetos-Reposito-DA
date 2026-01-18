import os
import zipfile
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ==============================
# CONFIGURAÇÕES
# ==============================
BASE_URL = (
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-12/"
)

BASE_DIR = r"C:\Git\Projetos-Reposito-DA\Projeto1_Cohort_Empresas_BR\dados_projeto_1"
ZIP_DIR = os.path.join(BASE_DIR, "zip")
EXTRACT_DIR = os.path.join(BASE_DIR, "extracted")

os.makedirs(ZIP_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)


# ==============================
# 1. LISTAR ARQUIVOS ZIP
# ==============================
def listar_arquivos_zip():
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    return [
        BASE_URL + link.get("href")
        for link in soup.find_all("a")
        if link.get("href", "").endswith(".zip")
    ]


# ==============================
# 2. BAIXAR ARQUIVOS
# ==============================
def baixar_arquivos(urls):
    for url in tqdm(urls, desc="⬇️ Baixando arquivos"):
        nome_arquivo = os.path.join(ZIP_DIR, url.split("/")[-1])

        if os.path.exists(nome_arquivo):
            continue

        r = requests.get(url, stream=True)
        with open(nome_arquivo, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


# ==============================
# 3. EXTRAIR ARQUIVOS
# ==============================
def extrair_arquivos():
    for arquivo in os.listdir(ZIP_DIR):
        if arquivo.endswith(".zip"):
            caminho_zip = os.path.join(ZIP_DIR, arquivo)
            with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
                zip_ref.extractall(EXTRACT_DIR)


# ==============================
# EXECUÇÃO
# ==============================
if __name__ == "__main__":
    print("🔎 Listando arquivos...")
    urls = listar_arquivos_zip()

    baixar_arquivos(urls)
    extrair_arquivos()

    print("✅ Ingestão concluída")

import os
import sqlite3
import requests
import zipfile
import pandas as pd
from tqdm import tqdm

# Cria pastas para receber os arquivos zipados e descompactados
paths = [
    "data/csv/empresas",
    "data/csv/estabelecimentos",
    "data/downloads/empresas",
    "data/downloads/estabelecimentos",
    "data/auxiliares"
]

for path in paths:
    os.makedirs(path, exist_ok=True)

# Configurações
BASE_DIR = "data"
URLS_FILES = {
    "empresas": os.path.join(BASE_DIR, "empresas.txt"),
    "estabelecimentos": os.path.join(BASE_DIR, "estabelecimentos.txt"),
}
DOWNLOAD_DIR = {
    "empresas": os.path.join(BASE_DIR, "downloads", "empresas"),
    "estabelecimentos": os.path.join(BASE_DIR, "downloads", "estabelecimentos"),
}
EXTRACT_DIR = {
    "empresas": os.path.join(BASE_DIR, "csv", "empresas"),
    "estabelecimentos": os.path.join(BASE_DIR, "csv", "estabelecimentos"),
}
DB_PATH = os.path.join(BASE_DIR, "BancoCNPJAgosto.db")
CHUNK_SIZE = 25000

# Colunas
COLUNAS_EMPRESAS = [
    "cnpj_basico", "razao_social", "natureza_juridica",
    "qualificacao_responsavel", "capital_social", "porte_empresa",
    "ente_federativo_responsavel"
]

COLUNAS_ESTABELECIMENTOS = [
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial",
    "nome_fantasia", "id_situacao_cadastral", "data_situacao_cadastro",
    "motivo_cadastral", "nome_da_cidade_no_exterior", "pais",
    "data_de_inicio_atividade", "id_cnae", "cnae_fiscal_secundaria",
    "tipo_do_logradouro", "logradouro", "numero", "complemento",
    "bairro", "cep", "uf", "id_municipio", "ddd1", "telefone1",
    "ddd2", "telefone2", "ddd_do_fax", "fax", "correio_eletronico",
    "situacao_especial", "data_da_situacao_especial"
]

# Garantir pastas
for path in list(DOWNLOAD_DIR.values()) + list(EXTRACT_DIR.values()):
    os.makedirs(path, exist_ok=True)

# Download com barra de progresso
def download_file(url, dest):
    if os.path.exists(dest):
        print(f"Arquivo já existe: {dest}")
        return
    resp = requests.get(url, stream=True, timeout=240)
    total = int(resp.headers.get("content-length", 0))
    with open(dest, "wb") as file, tqdm(
        desc=f"Baixando {os.path.basename(dest)}",
        total=total, unit="B", unit_scale=True
    ) as bar:
        for chunk in resp.iter_content(1024 * 1024):
            file.write(chunk)
            bar.update(len(chunk))

# Extrair ZIP
def extract_zip(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
    except zipfile.BadZipFile:
        raise RuntimeError(f"❌ Erro: {zip_path} não é um arquivo ZIP válido. "
                           f"Verifique se o download foi concluído corretamente.")

# Adicionar colunas em chunks
def add_columns_in_chunks(file_path, colunas, chunksize=CHUNK_SIZE):
    temp_path = file_path + ".tmp"
    first_chunk = True
    for chunk in pd.read_csv(file_path, sep=";", header=None, dtype=str, chunksize=chunksize, encoding="latin1"):
        chunk.columns = colunas
        if first_chunk:
            chunk.to_csv(temp_path, sep=";", index=False, encoding="latin1", mode="w")
            first_chunk = False
        else:
            chunk.to_csv(temp_path, sep=";", index=False, encoding="latin1", mode="a", header=False)
    os.replace(temp_path, file_path)

# Renomear e adicionar colunas
def rename_and_add_columns(folder, tipo):
    colunas = COLUNAS_EMPRESAS if tipo == "empresas" else COLUNAS_ESTABELECIMENTOS
    for root, dirs, files in os.walk(folder):
        for file in files:
            old_path = os.path.join(root, file)
            if tipo == "empresas" and file.endswith(".EMPRECSV"):
                new_path = old_path + ".csv"
            elif tipo == "estabelecimentos" and file.endswith(".ESTABELE"):
                new_path = old_path + ".csv"
            else:
                continue
            os.rename(old_path, new_path)
            print(f"Renomeado: {file} -> {os.path.basename(new_path)}")
            add_columns_in_chunks(new_path, colunas)

# Criar tabelas
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS estabelecimentos")
    cursor.execute("DROP TABLE IF EXISTS empresas")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS empresas (
        cnpj_basico                 NUMERIC REFERENCES estabelecimentos (cnpj_basico),
        razao_social                TEXT,
        natureza_juridica           NUMERIC REFERENCES natureza_juridica (natureza_juridica),
        qualificacao_responsavel    NUMERIC REFERENCES qualificacao_responsavel (qualificacao_responsavel),
        capital_social              NUMERIC,
        porte_empresa               TEXT,
        ente_federativo_responsavel TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS estabelecimentos (
        cnpj_basico                 NUMERIC REFERENCES empresas (cnpj_basico),
        cnpj_ordem                  NUMERIC,
        cnpj_dv                     NUMERIC,
        identificador_matriz_filial NUMERIC,
        nome_fantasia               TEXT,
        id_situacao_cadastral       NUMERIC REFERENCES id_situacao_cadastral (id_situacao_cadastral),
        data_situacao_cadastro      TEXT,
        motivo_cadastral            NUMERIC,
        nome_da_cidade_no_exterior  TEXT,
        pais                        NUMERIC REFERENCES pais (pais),
        data_de_inicio_atividade    TEXT,
        id_cnae                     NUMERIC REFERENCES cnae (id_cnae),
        cnae_fiscal_secundaria      NUMERIC REFERENCES cnae (id_cnae),
        tipo_do_logradouro          TEXT,
        logradouro                  TEXT,
        numero                      NUMERIC,
        complemento                 TEXT,
        bairro                      TEXT,
        cep                         TEXT,
        uf                          TEXT,
        id_municipio                NUMERIC REFERENCES municipios (id_municipio),
        ddd1                        NUMERIC,
        telefone1                   NUMERIC,
        ddd2                        NUMERIC,
        telefone2                   NUMERIC,
        ddd_do_fax                  NUMERIC,
        fax                         NUMERIC,
        correio_eletronico          TEXT,
        situacao_especial           TEXT,
        data_da_situacao_especial   TEXT
    )
    """)
    conn.commit()

# Importar CSV em chunks
def import_csv_to_db(csv_path, table, conn):
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF;")
    for chunk in pd.read_csv(csv_path, sep=";", dtype=str, chunksize=CHUNK_SIZE, encoding="latin1"):
        chunk.to_sql(table, conn, if_exists="append", index=False)
    cursor.execute("PRAGMA foreign_keys = ON;")
    conn.commit()

# Criar índices
def create_indexes(conn):
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_empresas_cnpj ON empresas(cnpj_basico)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnpj ON estabelecimentos(cnpj_basico)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_estab_cnae ON estabelecimentos(id_cnae)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_estab_uf ON estabelecimentos(uf)")
    conn.commit()

# Exportar resultado do SELECT
def export_select(conn, query, output_path):
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"Arquivo final gerado: {output_path}")

# Query final
QUERY_FINAL = """
SELECT 
    e.cnpj_basico, 
    e.nome_fantasia, 
    emp.razao_social, 
    c.descricao_cnae, 
    e.bairro, 
    m.nome_municipio,
    e.tipo_do_logradouro,
    e.logradouro, 
    e.numero,
    e.cep, 
    e.complemento, 
    e.ddd1, 
    e.telefone1, 
    e.ddd2,
    e.telefone2, 
    e.correio_eletronico,
    e.data_de_inicio_atividade,
    e.data_situacao_cadastro,
    emp.capital_social,
    mot.descricao_situacao_cadastral
FROM 
    estabelecimentos e
JOIN 
    cnae c ON e.id_cnae = c.id_cnae
JOIN 
    empresas emp ON emp.cnpj_basico = e.cnpj_basico
JOIN
    municipios m on m.id_municipio = e.id_municipio
JOIN
    motivo_situacao_cadastral mot on mot.id_situacao_cadastral = e.id_situacao_cadastral
WHERE 
    e.ID_MUNICIPIO in (6313,7157,6669) 
    and e.id_situacao_cadastral in (2, 3, 8) 
    and e.id_cnae in (
        4321500,4330404,4330401,1622601,1622602,1622699,2330301,2330302,2330305,
        2599301,3313901,3314707,3329501,3511500,4120400,4213800,4221902,4221903,
        4221904,4221905,4222701,4292801,4299501,4299599,4311801,4311802,4312600,
        4313400,4319300,4321500,4322301,4322302,4322303,4329105,4329199,4330401,
        4330402,4330403,4330404,4330405,4330499,4391600,4399101,4399102,4399103,
        4399104,4399105,4399199,7111100,7112000,7119701,7119702,7410202
    )
"""

# Pipeline principal
def main():
    for tipo in ["empresas", "estabelecimentos"]:
        with open(URLS_FILES[tipo], "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
        for url in urls:
            filename = os.path.basename(url)
            if not filename.endswith(".zip"):
                continue
            dest = os.path.join(DOWNLOAD_DIR[tipo], filename)
            download_file(url, dest)
            extract_zip(dest, EXTRACT_DIR[tipo])
        rename_and_add_columns(EXTRACT_DIR[tipo], tipo)

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    for tipo, tabela in [("estabelecimentos", "estabelecimentos"), ("empresas", "empresas")]:
        for root, dirs, files in os.walk(EXTRACT_DIR[tipo]):
            for file in files:
                if file.endswith(".csv"):
                    import_csv_to_db(os.path.join(root, file), tabela, conn)

    create_indexes(conn)

    # Rodar o SELECT e exportar resultado
    export_select(conn, QUERY_FINAL, os.path.join(BASE_DIR, "resultado_final.csv"))

    conn.close()
    print("Precessamento concluido! Gloria a Deus!")

if __name__ == "__main__":
    main()

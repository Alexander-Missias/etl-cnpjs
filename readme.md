# ETL CNPJs — Profissionais da Construção

> Pipeline ETL em Python + SQLite para processar a base pública de CNPJs do Brasil e gerar uma base consultável com foco em profissionais e empresas do setor de construção (pintores, eletricistas, encanadores, pedreiros etc.).

---

## ✅ Resumo

Automatiza o que antes era feito manualmente: baixar, transformar e carregar os dados públicos (empresas, estabelecimentos, CNAE, municípios, situação cadastral) em um banco SQLite otimizado para consultas. O projeto foca em: desempenho (processamento em *chunks*), qualidade de dados e criação de conjuntos filtrados para prospecção comercial.

> **Observação importante:** é relevante e legítimo mencionar que você contou com **IA como apoio** (copiloto) no desenvolvimento — principalmente para validar trechos de código, gerar regex/CNAE mappings e ganhar produtividade. Isso mostra maturidade técnica e uso de ferramentas modernas.

---

## 📦 Tecnologias

- Python 3.9+
- Pandas
- SQLite (biblioteca `sqlite3` ou SQLAlchemy)
- tqdm (barra de progresso)
- requests (para download dos arquivos oficiais)

Arquivo `requirements.txt` sugerido:

```
pandas
sqlalchemy
tqdm
requests
python-dateutil
```

---

## 📁 Estrutura do repositório (sugestão)

```
etl-cnpjs/
├─ etl/
│  ├─ download.py
│  ├─ transform.py
│  ├─ load.py
│  └─ helpers.py
├─ notebooks/            # análises e EDA
├─ sql/                  # queries reutilizáveis
├─ data/                 # *NÃO* com dados sensíveis grandes (adicionar no .gitignore)
├─ README.md
├─ requirements.txt
└─ .gitignore
```

---

## 🔧 Quickstart — rodar localmente

1. Crie e ative um virtualenv:

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate
```

2. Instale dependências:

```bash
pip install -r requirements.txt
```

3. Execute o ETL (exemplo):

```bash
python etl/download.py   # baixa os arquivos oficiais (opcional)
python etl/load.py       # processa em chunks e carrega no SQLite
```

> **Importante:** não versionar a base inteira no Git (20 GB). Mantenha `data/` no `.gitignore` e compartilhe amostras ou scripts de rebuild.

---

## ⚙️ Detalhe técnico: _chunking_ e performance

### Por que processar em *chunks*?

- As fontes são muito grandes (ex.: tens de GB). Ler tudo na memória causa swap e torna a máquina inutilizável.
- Processar em blocos permite transformar e gravar incrementalmente.

### Estratégia prática

- Use `pandas.read_csv(..., chunksize=CHUNK_SIZE, compression='gzip')` para iterar sobre o CSV compactado.
- Defina `CHUNK_SIZE` conforme RAM disponível. Recomendo começar com **CHUNK_SIZE = 50_000** e testar; se ficar estável, aumentar para 100_000.
- Sempre selecione apenas as colunas necessárias com `usecols=[...]` e force tipos (`dtype=...`) para reduzir uso de memória.
- Use `df.astype({'col': 'category'})` para colunas com baixa cardinalidade (ex.: códigos de situação).

### PRAGMAS SQLite para acelerar cargas

Execute estes PRAGMA antes de cargas massivas (e volte ao padrão depois, se desejar):

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;   -- ou OFF para maior velocidade (risco de corrupção em queda de energia)
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -200000;   -- cache em páginas (~ ajusta conforme necessidade)
```

- **Dica:** crie índices **após** carregar os dados (indexação durante inserts torna a carga muito mais lenta).

### Exemplo: carregamento chunked rápido (sqlite3 + executemany)

```python
# etl/load_chunked.py
import sqlite3
import pandas as pd
from tqdm import tqdm

DB = 'data/cnpjs.db'
CSV = 'data/estabelecimentos.csv.gz'
CHUNK_SIZE = 50_000
BATCH_INSERT = 5000

def set_pragmas(conn):
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL;')
    cur.execute('PRAGMA synchronous=NORMAL;')
    cur.execute('PRAGMA temp_store=MEMORY;')
    cur.execute('PRAGMA cache_size=-200000;')
    conn.commit()

def process_chunk(df):
    cols = ['cnpj_basico','id_municipio','id_cnae','situacao_cadastral']
    df = df[cols].copy()
    df['cnpj_basico'] = df['cnpj_basico'].astype(str).str.zfill(14)
    # outras transformações/validações
    return df


def chunked_load():
    conn = sqlite3.connect(DB, timeout=30)
    set_pragmas(conn)
    cur = conn.cursor()

    # certifique-se que a tabela existe (create table if not exists ...)

    reader = pd.read_csv(CSV, compression='gzip', sep=';', chunksize=CHUNK_SIZE, dtype=str, usecols=['cnpj_basico','id_municipio','id_cnae','situacao_cadastral'], low_memory=False)
    for chunk in tqdm(reader):
        df = process_chunk(chunk)
        rows = [tuple(x) for x in df.to_numpy()]
        for i in range(0, len(rows), BATCH_INSERT):
            batch = rows[i:i+BATCH_INSERT]
            cur.executemany('INSERT INTO estabelecimentos (cnpj_basico, id_municipio, id_cnae, situacao_cadastral) VALUES (?,?,?,?)', batch)
            conn.commit()
    conn.close()

if __name__ == '__main__':
    chunked_load()
```

### Alternativa: `pandas.to_sql` com SQLAlchemy (mais simples)

```python
from sqlalchemy import create_engine
engine = create_engine('sqlite:///data/cnpjs.db', connect_args={{'timeout': 30}})
for chunk in pd.read_csv(..., chunksize=CHUNK_SIZE):
    chunk = process_chunk(chunk)
    chunk.to_sql('estabelecimentos', engine, if_exists='append', index=False, method='multi', chunksize=5000)
```

> `to_sql(..., method='multi')` envia pacotes multi-row e costuma ser mais rápido que inserts individuais quando a camada SQLAlchemy está bem configurada.

---

## 🏁 Pós-processamento e otimizações

- **Criar índices**:

```sql
CREATE INDEX idx_est_cnpj ON estabelecimentos(cnpj_basico);
CREATE INDEX idx_est_cnae ON estabelecimentos(id_cnae);
CREATE INDEX idx_est_mun ON estabelecimentos(id_municipio);
```

- Rodar `ANALYZE;` para ajudar o otimizador de queries.
- Se o DB final ficar com muito espaço livre, rodar `VACUUM;` (pode demorar).

---

## 🔍 Validações e QA (inclui uso de IA)

- Validar contagens por tabela entre arquivo original e DB carregado.
- Verificar valores nulos e anomalias por CNAE e município.
- Gerar amostras estratificadas para revisão manual.
- **IA como apoio**: gerar expressões regulares, revisar snippets de SQL/Python, sugerir mapeamentos CNAE → categoria (ex.: agrupar sub-classes de atividades de construção). Mencione isso no README — é legítimo e agrega valor.

---

## 💡 Insights que a base oferece (para negócio)

- **Lojistas de materiais**: identificar profissionais ativos por região e CNAE para campanhas locais e promoção de kits (ex.: kit pintura, kit elétrica).
- **Fornecedores/indústrias**: montar listas segmentadas por CNAE e situação cadastral para ações comerciais B2B.
- **Marketplace / Plataformas de serviços**: entender densidade de profissionais por município para priorizar expansão.

---


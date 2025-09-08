readme_content = '''
# ETL de CNPJs — Empresas e Estabelecimentos

✅ **Resumo**

Este projeto automatiza o processo de baixar, transformar e carregar dados públicos (empresas, estabelecimentos, CNAE, municípios, situação cadastral) em um banco SQLite otimizado para consultas.  
O foco é: **desempenho**, **qualidade de dados** e criação de **conjuntos filtrados** para prospecção comercial.

> Observação: o uso de IA como copiloto para validar trechos de código, gerar expressões regulares e mapear CNAE é legítimo e demonstra maturidade técnica.

---

## 📦 Tecnologias

- Python 3.9+
- Pandas
- SQLite (`sqlite3` ou SQLAlchemy)
- tqdm (barra de progresso)
- requests (download de arquivos oficiais)

**requirements.txt sugerido:**

```
pandas
sqlalchemy
tqdm
requests
python-dateutil
```

---

## 📁 Estrutura do repositório

```text
etl-cnpjs/
├─ etl/
│  ├─ download.py      # baixar arquivos oficiais
│  ├─ transform.py     # transformar dados
│  ├─ load.py          # carregar no SQLite
│  └─ helpers.py       # funções auxiliares
├─ notebooks/          # análises exploratórias (EDA)
├─ sql/                # queries reutilizáveis
├─ data/               # *NÃO versionar dados completos*
├─ docs/               # PDFs, MER, diagramas
├─ README.md
├─ requirements.txt
└─ .gitignore
```

> Dica: arquivos grandes (>1 GB) não devem ser versionados. Mantenha-os no `.gitignore` e compartilhe apenas amostras ou links de download.

---

## 🔗 Links de download (dados oficiais)

- **Empresas**: [link_empresas.txt](data/empresas.txt)  
- **Estabelecimentos**: [link_estabelecimentos.txt](data/estabelecimentos.txt)  
- **CNAE**: [link_cnae.txt](data/cnae.txt)  
- **Municípios**: [link_municipios.txt](data/municipios.txt)  
- **Situação cadastral**: [link_situacao.txt](data/situacao_cadastral.txt)  

> Use `etl/download.py` para baixar todos os arquivos oficiais automaticamente.

---

## 🔧 Quickstart — rodar localmente

### 1. Criar e ativar virtualenv

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Baixar dados

```bash
python etl/download.py
```

> Ele irá baixar os arquivos listados nos `.txt` de links e salvar em `data/`.

### 4. Processar e carregar no SQLite

```bash
python etl/load.py
```

- Processamento em **chunks** para grandes arquivos.  
- Criação de **banco SQLite** em `data/cnpjs.db`.

---

## ⚙️ Detalhes técnicos: performance

- Ler arquivos enormes (>20 GB) diretamente causa lentidão e uso excessivo de RAM.  
- Processar em **chunks** permite transformar e gravar incrementalmente.

**Exemplo usando Pandas + SQLite:**

```python
reader = pd.read_csv(CSV, compression='gzip', sep=';', chunksize=CHUNK_SIZE, dtype=str)
for chunk in reader:
    df = process_chunk(chunk)
    df.to_sql('estabelecimentos', engine, if_exists='append', index=False, method='multi')
```

**PRAGMAS SQLite para cargas rápidas:**

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -200000;
```

**Índices recomendados:**

```sql
CREATE INDEX idx_est_cnpj ON estabelecimentos(cnpj_basico);
CREATE INDEX idx_est_cnae ON estabelecimentos(id_cnae);
CREATE INDEX idx_est_mun ON estabelecimentos(id_municipio);
ANALYZE;
```

---

## 🔍 Validações e QA

- Comparar contagens por tabela entre arquivo original e DB carregado.  
- Verificar valores nulos e inconsistências por CNAE e município.  
- Gerar amostras estratificadas para revisão manual.  
- Uso de IA: validar regex, revisar snippets de SQL/Python, mapear CNAE para categorias.

---

## 🗂️ Pré-requisitos do Banco de Dados

O projeto assume que as seguintes tabelas já existem e estão populadas:

- `empresas`
- `estabelecimentos`
- `cnae`
- `municipios`
- `situacao_cadastral`

> Caso precise recriar a base, use os scripts em `etl/` para baixar, transformar e carregar os dados no SQLite.

---

## 📄 Documentação e MER

- MER e diagramas estão em `docs/MER.pdf`.  
- Você pode visualizar diretamente no GitHub clicando no arquivo.

---

## 💡 Insights para negócio

- **Lojistas de materiais:** identificar profissionais ativos por região e CNAE para campanhas locais.  
- **Fornecedores / indústrias:** montar listas segmentadas por CNAE e situação cadastral.  
- **Marketplaces / plataformas de serviços:** analisar densidade de profissionais por município para priorizar expansão.

---

## ⚠️ Avisos importantes

- **Não versionar dados completos** (20 GB) no GitHub.  
- Adicione arquivos grandes ao `.gitignore`.  
- Compartilhe **apenas amostras ou scripts de rebuild**.  
''' 

with open('README.md', 'w', encoding='utf-8') as f:
    f.write(readme_content)

print("README.md criado com sucesso!")

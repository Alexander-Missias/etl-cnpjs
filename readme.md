# ETL de CNPJs — Empresas e Estabelecimentos

✅ **Resumo**

Automatiza o processo de baixar, transformar e carregar dados públicos (empresas, estabelecimentos, CNAE, municípios, situação cadastral) em um banco SQLite otimizado para consultas.  
O foco do projeto é: **desempenho** (processamento em chunks), **qualidade de dados** e criação de **conjuntos filtrados** para prospecção comercial.

> Observação: o uso de IA como copiloto para validar trechos de código, gerar expressões regulares e mapeamentos CNAE é legítimo e demonstra maturidade técnica.

---

## 📦 Tecnologias

- Python 3.9+
- Pandas
- SQLite (`sqlite3` ou SQLAlchemy)
- tqdm (barra de progresso)
- requests (download de arquivos oficiais)

**requirements.txt sugerido:**


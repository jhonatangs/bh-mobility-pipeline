# 🚌 Pipeline de Dados - Mobilidade Urbana (BH)

![Status](https://img.shields.io/badge/Status-Completed-success)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-blueviolet)
![Playwright](https://img.shields.io/badge/Ingestion-Playwright-green)

Solução de Engenharia de Dados *end-to-end* para ingestão, processamento e análise de dados reais de mobilidade urbana (ônibus) de Belo Horizonte. A arquitetura implementa um **Lakehouse Local** resiliente, capaz de contornar instabilidades do portal de dados abertos governamental.

---

## 🏗 Arquitetura da Solução

O projeto segue a arquitetura **Medallion (Bronze, Silver, Gold)**, orquestrada via Airflow.

```mermaid
graph LR
    subgraph "Ingestão Resiliente (Bronze)"
        PBH[Portal PBH] -->|Playwright/Headless| GPS[Bronze GPS (Parquet)]
        PBH -->|Playwright/Scraping| MCO[Bronze MCO (Parquet)]
    end

    subgraph "Processamento (Silver)"
        GPS -->|Spark + Delta| SilverGPS[Silver GPS (Limpeza/Schema)]
        MCO -->|Spark + Delta| SilverMCO[Silver MCO (Dimensão)]
    end

    subgraph "Serving (Gold & Analytics)"
        SilverGPS -->|Join| Gold[Gold Mobility Analytics]
        SilverMCO -->|Join| Gold
        Gold -->|DuckDB SQL| Analytics[Relatório Final]
    end
```

### Destaques Técnicos

1.  **Ingestão via Playwright:** Implementação de *Web Scraping* avançado com navegador *headless* (Chromium) para contornar bloqueios (Erro 403) e capturar links dinâmicos no portal CKAN da prefeitura.
2.  **Schema Evolution & Mapping:** Tratamento dinâmico de colunas criptografadas da API (`LT` -> Latitude, `NV` -> Veículo) e variação de layouts CSV.
3.  **Lakehouse Local:** Uso de **Delta Lake** para garantir transações ACID e **DuckDB** para query engine OLAP de alta performance sem infraestrutura de nuvem.

---

## 🛠 Tech Stack

| Componente | Tecnologia | Justificativa |
| :--- | :--- | :--- |
| **Orquestração** | **Apache Airflow** (Astro) | Gerenciamento robusto de dependências e retentativas. |
| **Ingestão** | **Playwright (Python)** | Capacidade de emular navegador real para baixar dados onde `requests` padrão falha. |
| **Processamento** | **PySpark 3.5** | Processamento distribuído para grandes volumes. |
| **Storage** | **Delta Lake** | Versionamento, Schema Enforcement e performance. |
| **Warehouse** | **DuckDB** | Leitura direta de arquivos Delta/Parquet (Zero-Copy). |
| **Ambiente** | **Docker** | Isolamento total de dependências (Java, Drivers, Browsers). |

---

## 🚀 Como Executar

### Pré-requisitos
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) rodando.
* [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) instalado.
* Git.

### Passo a Passo

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/SEU-USUARIO/bh_mobility_pipeline.git](https://github.com/SEU-USUARIO/bh_mobility_pipeline.git)
    cd bh_mobility_pipeline
    ```

2.  **Inicie o Ambiente:**
    O build inicial pode demorar alguns minutos (instalação do Java, Spark e Browsers do Playwright).
    ```bash
    astro dev start
    ```

3.  **Acesse o Airflow:**
    * **URL:** `http://localhost:8080`
    * **User/Pass:** `admin` / `admin`

4.  **Execute o Pipeline:**
    * Ative a DAG `etl_urban_mobility_bh` e clique em **Trigger**.
    * Acompanhe a execução das tasks (cor verde = sucesso).

5.  **Validação:**
    Verifique os logs da task `analytics_check` para ver o relatório gerado pelo DuckDB com as top linhas ativas.

---

## 📊 Dicionário de Dados (Camada Gold)

Tabela: `mobility_analytics` (Formato Delta)

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `cod_linha` | String | Código identificador da linha. |
| `consorcio` | String | Consórcio responsável (via MCO). |
| `total_pings` | Long | Total de sinais de GPS recebidos. |
| `last_seen` | Timestamp | Última localização registrada. |
| `latitude` / `longitude` | Double | Coordenadas geográficas tratadas. |

> **Nota sobre Dados:** Devido a divergências entre os códigos de linha do sistema de GPS em Tempo Real (ex: códigos internos numéricos) e os códigos públicos do MCO (ex: alfanuméricos), algumas junções podem resultar em campos de dimensão nulos (`None`). A arquitetura prioriza a integridade dos dados de GPS (Left Join) para não descartar eventos de mobilidade.

---

## ⚠️ Troubleshooting

**Erro de Permissão (`Permission Denied` / `Errno 13`)**
Se ocorrer erro ao salvar arquivos na pasta `data/`:
* **Solução:** Execute `chmod -R 777 data` na raiz do projeto (Linux/Mac/WSL).

**Playwright: "Executable doesn't exist"**
Se o Airflow não encontrar o navegador:
* **Solução:** Certifique-se de que o `Dockerfile` instala as dependências como `root` e o binário do browser como usuário `astro`. Rode `astro dev restart` para reconstruir a imagem.

---

**Autor:** [Seu Nome]
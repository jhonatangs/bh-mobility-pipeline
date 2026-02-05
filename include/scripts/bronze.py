import json
import os
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import time


def get_json_via_playwright(url):
    """
    Baixa JSON via Playwright Request (API mode).
    """
    print(f"🎭 Playwright: Baixando dados de {url} ...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Contexto com User-Agent real
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )

        try:
            response = context.request.get(url, timeout=60000)
            if not response.ok:
                raise ValueError(f"Erro HTTP {response.status}")

            try:
                return response.json()
            except:
                content = response.text()
                if "{" in content:
                    json_str = content[content.find("{") : content.rfind("}") + 1]
                    return json.loads(json_str)
                raise ValueError("Conteúdo não é JSON.")

        except Exception as e:
            print(f"❌ Erro Playwright JSON: {e}")
            raise e
        finally:
            browser.close()


def extract_bus_data():
    print("🚀 Iniciando Ingestão BRONZE BUS (Dados Reais)...")
    target_url = "https://temporeal.pbh.gov.br/?param=D"

    try:
        raw_data = get_json_via_playwright(target_url)

        records = []
        if isinstance(raw_data, list):
            records = raw_data
        elif isinstance(raw_data, dict):
            records = (
                raw_data.get("entidades")
                or raw_data.get("records")
                or raw_data.get("result", {}).get("records")
                or []
            )

        if not records:
            raise ValueError("JSON vazio.")

        print(f"📦 {len(records)} registros de ônibus coletados.")

        ingestion_time = datetime.now()
        df = pd.DataFrame(records)
        df["_ingestion_timestamp"] = ingestion_time.isoformat()

        for col in df.columns:
            df[col] = df[col].astype(str)

        today = ingestion_time.strftime("%Y-%m-%d")
        output_path = f"/opt/airflow/data/bronze/bus_position/partition_date={today}"
        os.makedirs(output_path, exist_ok=True)

        file_name = f"bus_real_{ingestion_time.strftime('%H%M%S')}.parquet"
        df.to_parquet(f"{output_path}/{file_name}", index=False)
        print(f"💾 Bus Data salvo: {output_path}/{file_name}")

    except Exception as e:
        print(f"🔥 Erro Bronze Bus: {e}")
        raise e


def extract_mco_data():
    """
    Busca TODOS os CSVs do MCO e seleciona inteligentemente o MAIS RECENTE
    baseado no nome do arquivo ou texto do link.
    """
    print("🚌 Iniciando Ingestão BRONZE MCO (Seleção Inteligente)...")

    catalog_url = "https://dados.pbh.gov.br/dataset/mapa-de-controle-operacional-mco-consolidado-a-partir-de-abril-de-2022"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            print(f"🔎 Navegando no MCO: {catalog_url}")
            page.goto(catalog_url, timeout=60000, wait_until="domcontentloaded")

            # 1. Coleta TODOS os candidatos CSV
            candidates = page.evaluate("""() => {
                return Array.from(document.querySelectorAll('a'))
                    .map(a => ({
                        href: a.href,
                        text: a.innerText.trim(),
                        title: a.getAttribute('title') || ''
                    }))
                    .filter(link => link.href && link.href.toLowerCase().includes('.csv'))
            }""")

            if not candidates:
                raise ValueError("Nenhum CSV encontrado na página.")

            print(f"📋 Encontrados {len(candidates)} arquivos CSV disponíveis.")

            # 2. Lógica de Seleção: Tenta achar o ano mais recente
            current_year = datetime.now().year
            target_link = None

            # Tenta anos decrescentes: 2026 -> 2025 -> 2024
            for year in range(current_year, 2021, -1):
                year_str = str(year)
                # Filtra links que tem o ano no texto ou titulo
                year_links = [
                    l
                    for l in candidates
                    if year_str in l["text"]
                    or year_str in l["title"]
                    or year_str in l["href"]
                ]

                if year_links:
                    print(f"📆 Encontrados arquivos de {year}!")
                    # Geralmente o último da lista é o mês mais recente (Dezembro) se a lista for ordenada
                    # Ou podemos pegar o primeiro se for ordenação decrescente.
                    # Na dúvida do portal da PBH, pegamos o PRIMEIRO da lista filtrada por ano recente,
                    # pois portais costumam destacar o mais novo no topo.
                    target_link = year_links[0]
                    break

            # Fallback: Se não achou ano, pega o primeiro da lista geral
            if not target_link:
                print(
                    "⚠️ Ano não identificado nos nomes. Pegando o primeiro da lista geral."
                )
                target_link = candidates[0]

            print(
                f"🎯 Selecionado para download: {target_link['text']} ({target_link['href']})"
            )

            # 3. Download
            print("⬇️ Baixando...")
            response = page.context.request.get(target_link["href"], timeout=300000)

            if not response.ok:
                raise ValueError(f"Erro download: {response.status}")

            csv_content = response.text()

            # Processa
            from io import StringIO

            df = pd.read_csv(StringIO(csv_content), sep=";", low_memory=False)
            df.columns = [c.upper().strip() for c in df.columns]

            # Salva
            ingestion_time = datetime.now()
            df["_ingestion_timestamp"] = ingestion_time.isoformat()
            for col in df.columns:
                df[col] = df[col].astype(str)

            today = ingestion_time.strftime("%Y-%m-%d")
            output_path = f"/opt/airflow/data/bronze/mco/partition_date={today}"
            os.makedirs(output_path, exist_ok=True)

            df.to_parquet(f"{output_path}/mco_real.parquet", index=False)
            print(f"💾 MCO salvo com sucesso ({len(df)} linhas).")

        except Exception as e:
            print(f"❌ Erro MCO: {e}")
            raise e
        finally:
            browser.close()

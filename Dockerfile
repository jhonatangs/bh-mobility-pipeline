FROM quay.io/astronomer/astro-runtime:13.4.0

# --- ETAPA 1: Instalações que exigem ROOT (Sistema Operacional) ---
USER root

# 1. Instalar Java (para o Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean

# 2. Instalar o pacote Python Playwright
RUN pip install playwright

# 3. Instalar dependências de SISTEMA do Linux para o Chromium rodar
# (Isso precisa ser root pois usa apt-get por baixo dos panos)
RUN playwright install-deps chromium

# Configura variável do Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# --- ETAPA 2: Instalações do Usuário ASTRO (Quem roda o código) ---
USER astro

# 4. Baixar o binário do Navegador (Chromium)
# Fazemos isso JÁ como usuário 'astro' para que o arquivo fique salvo
# em /home/astro/.cache/ms-playwright/ e o script consiga achar.
RUN playwright install chromium
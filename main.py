import subprocess
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import shutil
import re

# --- Configuração de Logging ---
def setup_logging():
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "execucao_gcloud_downloader.log")

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    handlers = [
        RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=handlers)

setup_logging()

# --- Verificação da Ferramenta gcloud ---
def check_gcloud_availability():
    """Verifica se o comando 'gcloud' está disponível no PATH do sistema."""
    if shutil.which("gcloud"):
        logging.info("✅ 'gcloud' CLI encontrada no sistema.")
        return True
    else:
        logging.error("🔥 O comando 'gcloud' não foi encontrado. Por favor, instale e configure o Google Cloud SDK.")
        logging.error("➡️ Guia de instalação: https://cloud.google.com/sdk/docs/install")
        return False

# --- Definições de Códigos e Geração de URIs ---
DIRETORIO_OUTPUT_BASE = r"Output_GCS"
os.makedirs(DIRETORIO_OUTPUT_BASE, exist_ok=True)

#    ["23", "K", "NQ"], ["23", "K", "NR"], ["23", "K", "PR"], ["23", "K", "QQ"],
#    ["23", "K", "QR"], ["23", "K", "QS"], ["23", "K", "RQ"], ["23", "K", "RR"],
#    ["23", "K", "RS"], 

codigos = [
    ["23", "K", "RT"], ["23", "K", "KP"], ["24", "K", "TA"],
    ["24", "K", "TB"], ["24", "K", "TC"], ["24", "K", "TV"]
]

BUCKET_BASE_URI = "gs://gcp-public-data-sentinel-2/L2/tiles"

def get_recent_dates(num_days=15):
    """Retorna um conjunto de strings de data (YYYYMMDD) dos últimos N dias."""
    today = datetime.now()
    return { (today - timedelta(days=i)).strftime('%Y%m%d') for i in range(num_days) }

# --- Funções de Execução de Comandos ---

def get_available_safe_folders(uri_base):
    """Lista TUDO em um diretório e filtra as pastas .SAFE/ em Python."""
    # Comando simplificado para listar todo o conteúdo do diretório base
    command = ["gcloud", "storage", "ls", uri_base]
    logging.info(f"📂 Listando todo o conteúdo de: {uri_base}")
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True, shell=True
        )
        # Pega todas as linhas retornadas
        all_items = result.stdout.strip().split('\n')
        
        # Filtra a lista para pegar apenas as pastas principais que terminam com .SAFE/
        safe_folders = [item for item in all_items if item.endswith('.SAFE/')]
        
        if safe_folders:
            logging.info(f"✔️ Encontradas {len(safe_folders)} pastas .SAFE para análise.")
        else:
            logging.info("➡️ Nenhuma pasta .SAFE encontrada neste diretório.")
        return safe_folders

    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.decode('utf-8', errors='ignore')
        # Ignora o erro comum "Bucket Brigade" que não é crítico.
        if "Bucket Brigade" not in stderr_output:
             logging.warning(f"⚠️ Erro ao listar {uri_base}. Pode não existir ou estar vazio. Erro: {stderr_output}")
        else:
             logging.info(f"➡️ Nenhuma pasta .SAFE encontrada em {uri_base}.")
        return []

def download_folder(gcs_folder_uri, local_destination):
    """Baixa uma pasta completa do GCS para um diretório local."""
    local_destination_clean = os.path.normpath(local_destination)
    command = ["gcloud", "storage", "cp", "-r", gcs_folder_uri, local_destination_clean]
    logging.info(f"🚀 Começando o download com o comando: {' '.join(command)}")
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            logging.error(f"🔥 Falha no download da pasta '{gcs_folder_uri}'.")
            logging.error(f"➡️ Erro retornado pelo gcloud: {stderr.decode('utf-8', errors='ignore')}")
        else:
            logging.info(f"✔️ Download de '{gcs_folder_uri}' para '{local_destination_clean}' concluído com sucesso.")
    except Exception as e:
        logging.error(f"🔥 Um erro inesperado ocorreu durante o download: {e}")

# --- Script Principal ---
def main():
    if not check_gcloud_availability():
        return

    datas_recentes = get_recent_dates(15)
    logging.info(f"🔎 Procurando por dados dos últimos 15 dias (de {min(datas_recentes)} a {max(datas_recentes)})")

    for codigo in codigos:
        uri_base_por_codigo = f"{BUCKET_BASE_URI}/{codigo[0]}/{codigo[1]}/{codigo[2]}/"
        logging.info(f"\n{'='*20}\n⚙️  Processando código: {codigo} \n{'='*20}")

        pastas_disponiveis = get_available_safe_folders(uri_base_por_codigo)

        if not pastas_disponiveis:
            continue

        for pasta_uri in pastas_disponiveis:
            try:
                nome_pasta = os.path.basename(pasta_uri.strip('/'))
                match = re.search(r'_(\d{8})T', nome_pasta)
                
                if not match:
                    continue
                
                data_da_pasta = match.group(1)

                if data_da_pasta in datas_recentes:
                    logging.info(f"\n--- ✅ Pasta Encontrada! ---\nData: {data_da_pasta}\nCaminho: {pasta_uri}\n--------------------------")
                    
                    caminho_local_base = os.path.join(DIRETORIO_OUTPUT_BASE, codigo[0], codigo[1], codigo[2])
                    os.makedirs(caminho_local_base, exist_ok=True)
                    
                    caminho_local_final = os.path.join(caminho_local_base, nome_pasta)

                    if os.path.exists(caminho_local_final):
                        logging.info(f"🗄️  Diretório local já existe, pulando download: {caminho_local_final}")
                        continue
                    
                    download_folder(pasta_uri, caminho_local_base)

            except Exception as e:
                logging.error(f"🔥 Erro ao processar a pasta {pasta_uri}: {e}")

    logging.info("\n🎉 Script finalizado com sucesso!")

if __name__ == "__main__":
    main()

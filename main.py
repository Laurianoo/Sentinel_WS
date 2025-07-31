'''
Baixa dados do sentinel no repositório cloud console do google automaticamente para os 15 dias mais recentes
É necessário ter instalado a CLI gcloud e adicionada ao PATH do sistema, caso não tenha: https://cloud.google.com/sdk/docs/install?hl=pt-br
URL para abertura manual: https://console.cloud.google.com/storage/browser/gcp-public-data-sentinel-2/L2/tiles/
'''

import xml.etree.ElementTree as ET
import tempfile
import subprocess
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import shutil
import re

# --- Configuração de Logging ---
def setup_logging():
    # Função de logging, para armazenar as informações em um arquivo a parte para depuração
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, "execucao_gcloud_downloader.log")
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    handlers = [RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"), logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format=log_format, handlers=handlers)

setup_logging() # Inicia a função de Logging

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
DIRETORIO_OUTPUT_BASE = r"Output_GCS" # Pasta de saída dos arquivos
os.makedirs(DIRETORIO_OUTPUT_BASE, exist_ok=True) # Cria a pasta

# Caminhos de pastas e subpastas que o script percorre dentro do site:
codigos = [
    ["23", "K", "NQ"], ["23", "K", "NR"], ["23", "K", "PR"], ["23", "K", "QQ"],
    ["23", "K", "QR"], ["23", "K", "QS"], ["23", "K", "RQ"], ["23", "K", "RR"],
    ["23", "K", "RS"], ["23", "K", "RT"], ["23", "K", "KP"], 
    ["24", "K", "TA"], ["24", "K", "TB"], ["24", "K", "TC"], ["24", "K", "TV"]]

# URL base usada no script:
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
        result = subprocess.run(command, check=True, capture_output=True, text=True, shell=True)
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

def get_cloud_cover(safe_folder_uri):
    """
    Baixa o arquivo de metadados de uma pasta .SAFE, extrai a porcentagem
    de cobertura de nuvens e apaga o arquivo de metadados local. Tenta múltiplas
    tags de nuvem para maior compatibilidade.
    Retorna a porcentagem de nuvens como float ou None se falhar.
    """
    metadata_filename = "MTD_MSIL2A.xml"
    metadata_file_uri = f"{safe_folder_uri}{metadata_filename}"
    temp_xml_path = os.path.join(tempfile.gettempdir(), metadata_filename)

    # Lista de tags para procurar, em ordem de preferência.
    # A primeira que for encontrada será usada.
    cloud_tags_to_try = [
        'Cloud_Coverage_Assessment',
        'CLOUDY_PIXEL_OVER_LAND_PERCENTAGE',
        'CLOUDY_PIXEL_PERCENTAGE']

    command = ["gcloud", "storage", "cp", metadata_file_uri, temp_xml_path]
    logging.info(f"🔎 Verificando cobertura de nuvens em: {metadata_file_uri}")

    try:
        # Executa o download do arquivo de metadados
        subprocess.run(command, check=True, capture_output=True, text=True, shell=True)
        
        tree = ET.parse(temp_xml_path)
        root = tree.getroot()
        
        # Itera sobre a lista de tags possíveis
        for tag_name in cloud_tags_to_try:
            # A sintaxe './/' busca a tag em qualquer lugar do documento XML
            cloud_cover_element = root.find(f'.//{tag_name}')
            
            if cloud_cover_element is not None:
                cloud_cover = float(cloud_cover_element.text)
                logging.info(f"☁️ Cobertura de nuvens encontrada usando a tag '{tag_name}': {cloud_cover:.2f}%")
                return cloud_cover  # Retorna o valor da primeira tag encontrada

        # Se o loop terminar sem encontrar nenhuma das tags
        logging.warning(f"⚠️ Nenhuma das tags de nuvem {cloud_tags_to_try} foi encontrada em {metadata_filename}.")
        return None

    except subprocess.CalledProcessError as e:
        stderr_output = e.stderr.decode('utf-8', errors='ignore')
        logging.error(f"🔥 Falha ao baixar o arquivo de metadados '{metadata_file_uri}'. Erro: {stderr_output}")
        return None
    except ET.ParseError:
        logging.error(f"🔥 Falha ao analisar o arquivo XML: {temp_xml_path}")
        return None
    finally:
        # Garante que o arquivo temporário seja sempre removido
        if os.path.exists(temp_xml_path):
            os.remove(temp_xml_path)

# --- Script Principal ---
def main():
    if not check_gcloud_availability(): # Verifica a instalação da API
        return

    datas_recentes = get_recent_dates(15) # Usa a função para obter as datas recentes para contruir a query
    logging.info(f"🔎 Procurando por dados dos últimos 15 dias (de {min(datas_recentes)} a {max(datas_recentes)})")

    for codigo in codigos: # Loop para percorrer todas as pastas de interesse
        # Constrói a URL usando f string:
        uri_base_por_codigo = f"{BUCKET_BASE_URI}/{codigo[0]}/{codigo[1]}/{codigo[2]}/"
        logging.info(f"\n{'='*20}\n⚙️  Processando código: {codigo} \n{'='*20}")

        # Obtém uma lista das pastas disponiveis
        pastas_disponiveis = get_available_safe_folders(uri_base_por_codigo)

        if not pastas_disponiveis: # Se não tiver pastas disponiveis ele pula para a próxima execução do loop
            continue

        # Loop que percorre as pastas no site
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
                        logging.info(f"🗄️   Diretório local já existe, pulando download: {caminho_local_final}")
                        continue
                    
                    # --- VERIFICAÇÃO DE COBERTURA DE NUVENS ---
                    cloud_cover_percentage = get_cloud_cover(pasta_uri)
                    
                    # Se a verificação falhou (retornou None), pula para a próxima pasta
                    if cloud_cover_percentage is None:
                        logging.warning(f"⚠️ Não foi possível verificar a cobertura de nuvens para {nome_pasta}. Pulando.")
                        continue

                    # Verifica se a cobertura está dentro do limite de 30%
                    if cloud_cover_percentage <= 30.0:
                        logging.info(f"✔️ Cobertura de nuvens ({cloud_cover_percentage:.2f}%) está abaixo do limite de 30%. Baixando.")
                        # Faz o download da pasta:
                        download_folder(pasta_uri, caminho_local_base)
                    else:
                        logging.info(f"➡️ Cobertura de nuvens ({cloud_cover_percentage:.2f}%) excede o limite de 30%. Download de {nome_pasta} ignorado.")

            except Exception as e:
                logging.error(f"🔥 Erro ao processar a pasta {pasta_uri}: {e}")
    logging.info("\n🎉 Script finalizado com sucesso!")

# Executa o script:
if __name__ == "__main__":
    main()
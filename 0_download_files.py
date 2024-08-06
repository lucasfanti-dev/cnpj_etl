import os
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', handlers=[
    logging.FileHandler("../logs/0_download_files.log", encoding='utf-8'),
    logging.StreamHandler()
])

def download_file(url, save_dir):
    local_filename = os.path.join(save_dir, url.split('/')[-1])
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        with open(local_filename, 'wb') as f:
            downloaded_size = 0
            for chunk in r.iter_content(chunk_size=1048576):  # Chunk size of 1MB
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    progress = (downloaded_size / total_size) * 100
                    logging.info(f"Baixando {url}: {progress:.2f}% completado.")
    logging.info(f"Arquivo {url} baixado com sucesso.")
    return local_filename

def get_links_from_index(index_url):
    response = requests.get(index_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = []
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        if href.endswith('.zip') or href.endswith('.pdf') or href.endswith('.odt'):
            links.append(index_url + href)
    return links

if __name__ == "__main__":
    save_directory = '../1downloads'
    os.makedirs(save_directory, exist_ok=True)

    index_urls = [
        "https://dadosabertos.rfb.gov.br/CNPJ/",
        "https://dadosabertos.rfb.gov.br/CNPJ/regime_tributario/"
    ]

    all_links = []
    total_files = 0

    # Iterar sobre as URLs dos índices para listar todos os arquivos
    for index_url in index_urls:
        logging.info(f"Acessando índice: {index_url}")
        links = get_links_from_index(index_url)
        total_files += len(links)
        logging.info(f"Arquivos encontrados no índice {index_url}:")
        for link in links:
            logging.info(link)
            all_links.append(link)

    # Baixar todos os arquivos listados
    start_time = datetime.now()
    for link in all_links:
        logging.info(f"Baixando arquivo: {link}")
        try:
            download_file(link, save_directory)
            logging.info(f"Arquivo {link.split('/')[-1]} baixado com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao baixar o arquivo {link.split('/')[-1]}: {e}")

    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Duração total para baixar todos os arquivos: {duration}")

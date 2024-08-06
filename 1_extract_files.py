import os
import zipfile
import multiprocessing
import logging
from datetime import datetime
import time

# Configurar o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', handlers=[
    logging.FileHandler("../logs/1_extract_files.log", encoding='utf-8'),
    logging.StreamHandler()
])

def extract_file(args):
    zip_path, member, extract_to = args
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extract(member, extract_to)
        return member

def extract_zip(zip_path, extract_to, total_files):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        members = zip_ref.namelist()
        num_files = len(members)
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        for i, member in enumerate(pool.imap_unordered(extract_file, [(zip_path, m, extract_to) for m in members])):
            progress = ((i + 1) / num_files) * 100
            logging.info(f"Extraindo {member} de {zip_path}: {progress:.2f}% concluído")
        pool.close()
        pool.join()

def initialize_progress():
    logging.info("Progresso inicializado.")

if __name__ == "__main__":
    extract_to_directory = '../2extracted'
    os.makedirs(extract_to_directory, exist_ok=True)
    initialize_progress()

    zip_files = [os.path.join('../1downloads', f) for f in os.listdir('../1downloads') if f.endswith('.zip')]
    total_files = len(zip_files)

    # Listar todos os arquivos antes de iniciar a extração
    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                members = zip_ref.namelist()
                for member in members:
                    logging.info(f"Arquivo listado: {member} no zip: {zip_file}")
        except zipfile.BadZipFile:
            logging.error(f"O arquivo {zip_file} não é um arquivo ZIP válido.")

    # Aguardar 5 segundos antes de iniciar a extração
    time.sleep(5)

    # Iniciar a extração
    for zip_file in zip_files:
        try:
            logging.info(f"Iniciando extração de {zip_file}")
            extract_zip(zip_file, extract_to_directory, total_files)
            logging.info(f"Extração de {zip_file} concluída")
        except zipfile.BadZipFile:
            logging.error(f"O arquivo {zip_file} não é um arquivo ZIP válido.")

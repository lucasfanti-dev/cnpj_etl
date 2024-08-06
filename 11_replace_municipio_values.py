import os
import chardet
from pathlib import Path
import logging
from unidecode import unidecode
import ray
from datetime import datetime

# Configuração de Logging
log_filename = '../logs/11_replace_municipio_values.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Função para pular duas linhas antes do primeiro log
def log_initial_message():
    with open(log_filename, 'a') as f:
        f.write("\n\n")

log_initial_message()

# Inicializa Ray para processamento paralelo
ray.init()

ENCODINGS = [
    'latin1', 'utf-8', 'cp1252', 'iso-8859-1', 'iso-8859-15', 'mac_roman', 'ISO-8859-1',
    'ascii', 'utf-16', 'utf-16le', 'utf-16be', 'utf-32', 'utf-32le', 'utf-32be',
    'cp850', 'cp858', 'cp437', 'cp775', 'cp858', 'cp1006', 'cp1026', 'cp1140',
    'mac_latin2', 'mac_cyrillic', 'mac_greek', 'mac_iceland', 'mac_latin2',
    'mac_turkish', 'utf-7', 'utf-8-sig', 'latin-1', 'utf-32', 'iso-8859-2',
    'iso-8859-3', 'iso-8859-4', 'iso-8859-5', 'iso-8859-6', 'iso-8859-7', 'iso-8859-8',
    'iso-8859-9', 'iso-8859-10', 'iso-8859-11', 'iso-8859-13', 'iso-8859-14',
    'iso-8859-16', 'windows-1250', 'windows-1251', 'windows-1252', 'windows-1253',
    'windows-1254', 'windows-1255', 'windows-1256', 'windows-1257', 'windows-1258', 'koi8-r', 'koi8-u'
]

# Função para detectar a codificação de um arquivo
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    return result['encoding']

# Função para ler os dados dos arquivos Município e armazená-los em um dicionário
def read_municipio_files(municipio_folder):
    municipio_data = {}
    for file in Path(municipio_folder).glob('*MUNICCSV*'):
        logging.info(f'Lendo arquivo {file}...')
        try:
            encoding = detect_encoding(file)
            with open(file, 'r', encoding=encoding) as txtfile:
                lines = txtfile.readlines()
                for line in lines:
                    columns = line.strip().split(';')
                    if len(columns) > 1:  # Verifica se há pelo menos duas colunas
                        key = unidecode(columns[0].upper())
                        value = unidecode(columns[1].upper())
                        municipio_data[key] = f'{'"' + value.replace('"', '') + '"'}'
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {file}: {e}")
    return municipio_data

@ray.remote
def process_estabele_file(file, municipio_data):
    start_time = datetime.now()
    logging.info(f'Processando arquivo {file}...')
    try:
        encoding = detect_encoding(file)
        with open(file, 'r', encoding=encoding) as txtfile:
            lines = txtfile.readlines()
        with open(file, 'w', encoding=encoding, newline='') as txtfile:
            for line in lines:
                columns = line.strip().split(';')
                if len(columns) > 20:  # Verifica se a linha tem pelo menos 21 colunas
                    old_value = unidecode(columns[20].upper())
                    if old_value in municipio_data:
                        columns[20] = municipio_data[old_value]
                txtfile.write(';'.join(columns) + '\n')
        logging.info(f'Arquivo {file} processado com sucesso. Duracao: {datetime.now() - start_time}')
    except Exception as e:
        logging.error(f"Erro ao processar o arquivo {file}: {e}")

def process_estabele_files(estabele_folder, municipio_data):
    tasks = []
    for file in Path(estabele_folder).glob('*estabele*'):
        tasks.append(process_estabele_file.remote(file, municipio_data))
        logging.info(f'Arquivo enviado para processamento: {file}')
    ray.get(tasks)

def main():
    municipio_folder = "../3validated"  # Pasta com os arquivos Município
    estabele_folder = "../3validated"  # Pasta com os arquivos ESTABELE

    logging.info('Lendo dados dos arquivos MUNICCSV...')
    municipio_data = read_municipio_files(municipio_folder)
    logging.info('Dados dos arquivos MUNICCSV lidos com sucesso.')

    logging.info('Processando arquivos ESTABELE...')
    process_estabele_files(estabele_folder, municipio_data)
    logging.info('Arquivos ESTABELE processados com sucesso.')

if __name__ == "__main__":
    main()

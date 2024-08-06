import csv
import polars as pl
import ray
from pathlib import Path
import logging
from datetime import datetime

# Inicializa Ray para processamento paralelo
ray.init()

# Configurações de Logging
log_filename = '../logs/4_process_large_csv_chunks.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Função para pular duas linhas antes do primeiro log
def log_initial_message():
    with open(log_filename, 'a') as f:
        f.write("\n\n\n\n")

log_initial_message()

# Lista de encodings a serem tentados
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

# Lista de extensões de arquivos a serem processados
FILE_EXTENSIONS = ['*SIMPLES.CSV.*', '*.EMPRECSV', '*.ESTABELE']

@ray.remote
def process_chunk(chunk, output_path, base_filename, chunk_index, extension):
    try:
        logging.info(f'Processando chunk {chunk_index}...')
        chunk_filename = output_path / f"{base_filename}.{extension}_chunk_{chunk_index}.txt"
        with open(chunk_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writerows(chunk)
        logging.info(f'Chunk {chunk_index} processado e salvo em {chunk_filename}')
        return chunk_filename
    except Exception as e:
        logging.error(f'Erro ao processar chunk {chunk_index}: {e}')
        return None

def process_file_in_chunks(file_path, output_path, error_path, chunk_size):
    start_time = datetime.now()
    logging.info(f'Inicio do processamento do arquivo {file_path}')
    extension = file_path.suffix[1:].lower()  # Captura a extensão do arquivo e remove o ponto inicial
    for encoding in ENCODINGS:
        try:
            logging.info(f'Tentando ler o arquivo {file_path} com a codificacao {encoding}...')
            with open(file_path, 'r', encoding=encoding) as csvfile:
                reader = csv.reader(csvfile, delimiter=';', quotechar='"')
                data = list(reader)
            logging.info(f'Sucesso ao ler o arquivo {file_path} com a codificacao {encoding}')
            num_rows = len(data)
            base_filename = file_path.stem
            tasks = []

            for i in range(0, num_rows, chunk_size):
                chunk = data[i:i + chunk_size]
                tasks.append(process_chunk.remote(chunk, output_path, base_filename, i // chunk_size, extension))

            results = ray.get(tasks)
            logging.info(f'Fim do processamento do arquivo {file_path} as {datetime.now()}, duracao: {datetime.now() - start_time}')
            return results
        except Exception as e:
            logging.warning(f"Erro ao ler o arquivo com a codificacao {encoding}: {e}")

    # Movendo arquivo para a pasta de erros se não for possível processá-lo
    error_file_path = error_path / file_path.name
    file_path.rename(error_file_path)
    logging.error(f'Falha ao ler o arquivo {file_path} com as codificacoes tentadas. Arquivo movido para {error_file_path}')
    return None

def main(input_folder, output_folder, error_folder, chunk_size):
    input_path = Path(input_folder)
    output_path = Path(output_folder)
    error_path = Path(error_folder)
    output_path.mkdir(parents=True, exist_ok=True)
    error_path.mkdir(parents=True, exist_ok=True)

    for extension in FILE_EXTENSIONS:
        files = list(input_path.glob(extension))
        for file in files:
            process_file_in_chunks(file, output_path, error_path, chunk_size)

if __name__ == "__main__":
    input_folder = "../2extracted"
    output_folder = "../3validated"
    error_folder = "../errors"
    main(input_folder, output_folder, error_folder, chunk_size=1000000)

import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

# Configuração de Logging
log_filename = '../logs/8_remove_all_semicolons.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Função para pular duas linhas antes do primeiro log
def log_initial_message():
    with open(log_filename, 'a') as f:
        f.write("\n\n")

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

# Função para tentar diferentes encodings para abrir um arquivo
def try_encodings(file_path):
    for encoding in ENCODINGS:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                return file.readlines(), encoding
        except Exception as e:
            continue
    raise Exception(f'Nenhum encoding valido encontrado para o arquivo {file_path}')

# Função para processar um arquivo de texto
def process_file(txt_file):
    start_time = datetime.now()
    try:
        logging.info(f'Inicio do processamento do arquivo {txt_file}')
        lines, encoding_used = try_encodings(txt_file)
        logging.info(f'Encoding utilizado para {txt_file}: {encoding_used}')

        processed_lines = []
        for line in lines:
            line = line.strip()
            line = line.replace(';', '')
            processed_lines.append(line + '\n')

        with open(txt_file, 'w', encoding='utf-8') as file:
            file.writelines(processed_lines)
        
        logging.info(f'Arquivo {txt_file} processado com sucesso. Duracao: {datetime.now() - start_time}')
        return True
    except Exception as e:
        logging.error(f'Erro ao processar arquivo {txt_file}: {e}')
        return False

def process_files_in_folder(folder_path, error_folder, max_workers=8):
    try:
        txt_files = list(Path(folder_path).glob("*.txt"))
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_file, txt_files))
        
        for txt_file, success in zip(txt_files, results):
            if not success:
                error_file_path = error_folder / txt_file.name
                txt_file.rename(error_file_path)
                logging.error(f'Arquivo movido para {error_file_path} devido a erro durante o processamento.')
        
        logging.info(f'Todos os arquivos foram processados com sucesso.')
    except Exception as e:
        logging.error(f'Erro ao processar arquivos na pasta {folder_path}: {e}')

def main(input_folder, error_folder, max_workers=8):
    input_path = Path(input_folder)
    error_path = Path(error_folder)
    error_path.mkdir(parents=True, exist_ok=True)
    process_files_in_folder(input_path, error_path, max_workers)

if __name__ == "__main__":
    input_folder = "../3validated"
    error_folder = "../errors"
    main(input_folder, error_folder, max_workers=8)

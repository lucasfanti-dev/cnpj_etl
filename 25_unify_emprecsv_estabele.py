import os
import chardet
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil

# Configuração de Logging
log_filename = '../logs/25_unify_emprecsv_estabele.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', 
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Limite de memória (8 GB)
MAX_MEMORY = 8 * 1024 * 1024 * 1024  # 8 GB
CHUNK_SIZE = 1_000_000  # Tamanho do chunk (1 milhão de linhas)

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
    return chardet.detect(raw_data)['encoding']

def read_file_lines(file_path):
    try:
        encoding = detect_encoding(file_path)
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        return lines
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def should_write_intermediate():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss >= MAX_MEMORY

def write_intermediate_file(lines, output_folder, base_name, file_index):
    output_file_path = Path(output_folder) / f'{base_name}_{file_index}.txt'
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    return output_file_path

def process_files(input_folder, output_folder, unified_estabele, unified_files):
    start_time = datetime.now()
    intermediate_files = []
    file_index = 0
    line_count = 0

    unified_est_lines = read_file_lines(unified_estabele)
    if not unified_est_lines:
        logging.error(f"Error reading unified estabele file: {unified_estabele}")
        return

    unified_data = {}
    for ufile in unified_files:
        unified_lines = read_file_lines(ufile)
        if unified_lines:
            for line in unified_lines:
                key = line.split(';')[0]
                if key not in unified_data:
                    unified_data[key] = []
                unified_data[key].append(line.strip())

    with open(Path(output_folder) / f'unified_estabele_emprecsv_chunk_{file_index}.txt', 'w', encoding='utf-8') as final_output:
        for line in unified_est_lines:
            key = line.split(';')[0]
            if key in unified_data:
                for uline in unified_data[key]:
                    final_output.write(line.strip() + ';' + uline + '\n')
                    line_count += 1
            else:
                final_output.write(line)
                line_count += 1

            if line_count >= CHUNK_SIZE:
                file_index += 1
                line_count = 0
                final_output.close()
                final_output = open(Path(output_folder) / f'unified_estabele_emprecsv_chunk_{file_index}.txt', 'w', encoding='utf-8')

    logging.info(f"Successfully processed all files for keyword 'estabele'")

def main():
    input_folder = "../4unified_simples_emprecsv"  # Pasta de entrada com os arquivos "unified"
    output_folder = "../9unified_emprecsv_estabele"  # Pasta de saída para arquivos concatenados
    os.makedirs(output_folder, exist_ok=True)

    unified_estabele = "../8column_simples_estabele/column_simples_estabele_estabele.txt"
    unified_files = ["../4unified_simples_emprecsv/unified_emprecsv.txt"]

    process_files(input_folder, output_folder, unified_estabele, unified_files)

    logging.info('All files have been processed.')

if __name__ == "__main__":
    main()

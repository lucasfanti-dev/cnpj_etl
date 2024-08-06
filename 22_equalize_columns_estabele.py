import os
import chardet
import psutil
from pathlib import Path
import logging
from datetime import datetime

# Configuração de Logging
log_filename = '../logs/22_equalize_columns_estabele.log'
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
            for line in f:
                yield line
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")

def write_intermediate_file(lines, output_folder, keyword, index):
    output_file_path = Path(output_folder) / f'column_intermediate_{keyword.lower()}_{index}.txt'
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        logging.info(f"Intermediate file {output_file_path} created with {len(lines)} lines.")
    except Exception as e:
        logging.error(f"Error writing intermediate file {output_file_path}: {e}")
    return output_file_path

def should_write_intermediate(total_memory):
    process = psutil.Process(os.getpid())
    return process.memory_info().rss + total_memory >= MAX_MEMORY

def equalize_columns(lines):
    max_columns = max(len(line.split(';')) for line in lines)
    equalized_lines = []
    for line in lines:
        columns = line.strip().split(';')
        while len(columns) < max_columns:
            columns.append('""')
        equalized_lines.append(';'.join(columns) + '\n')
    return equalized_lines

def process_files(keyword, input_folder, output_folder, max_memory=MAX_MEMORY):
    start_time = datetime.now()
    intermediate_files = []
    total_memory = 0

    logging.info(f"Checking for files in folder: {input_folder}")
    files = list(Path(input_folder).glob(f'*{keyword}*'))
    logging.info(f"Found {len(files)} files matching keyword '{keyword}'")

    if not files:
        logging.warning(f"No files found matching keyword '{keyword}' in {input_folder}")
        return

    all_lines = []

    for file in files:
        try:
            for line in read_file_lines(file):
                all_lines.append(line)
                total_memory += len(line)
                if len(all_lines) >= CHUNK_SIZE or should_write_intermediate(total_memory):
                    intermediate_files.append(write_intermediate_file(all_lines, output_folder, keyword, len(intermediate_files)))
                    all_lines = []
                    total_memory = 0
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")

    if all_lines:
        intermediate_files.append(write_intermediate_file(all_lines, output_folder, keyword, len(intermediate_files)))

    # Igualar colunas de todas as linhas nos arquivos intermediários
    for i, temp_file in enumerate(intermediate_files):
        try:
            with open(temp_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            equalized_lines = equalize_columns(lines)
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.writelines(equalized_lines)
            logging.info(f"Equalized columns in intermediate file {temp_file}.")
        except Exception as e:
            logging.error(f"Error equalizing columns in file {temp_file}: {e}")

    logging.info(f"Successfully processed {len(intermediate_files)} intermediate files for keyword '{keyword}'")

    if intermediate_files:
        final_output_path = Path(output_folder) / f'column_{keyword.lower()}.txt'
        try:
            with open(final_output_path, 'w', encoding='utf-8') as final_output:
                for temp_file in intermediate_files:
                    with open(temp_file, 'r', encoding='utf-8') as f:
                        final_output.writelines(f.readlines())
            logging.info(f'Final output file {final_output_path} created successfully. Duration: {datetime.now() - start_time}')
        except Exception as e:
            logging.error(f"Error writing final output file {final_output_path}: {e}")

        for f in intermediate_files:
            try:
                os.remove(f)
                logging.info(f'Intermediate file {f} removed.')
            except Exception as e:
                logging.error(f"Error removing intermediate file {f}: {e}")

def main():
    input_folder = "../5unified_estabele"
    output_folder = "../6column_estabele"
    os.makedirs(output_folder, exist_ok=True)

    keywords = ['estabele']
    for keyword in keywords:
        process_files(keyword, input_folder, output_folder)

    logging.info('All files have been processed.')

if __name__ == "__main__":
    main()

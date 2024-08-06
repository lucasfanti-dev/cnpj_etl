import os
from pathlib import Path
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuração de Logging
log_filename = '../logs/28_remove_columns.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', 
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Pasta de entrada
input_folder = Path("../11file_division")

def remove_columns_from_file(file_path, columns_to_remove):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Ordena os índices das colunas a serem removidas em ordem decrescente
        columns_to_remove_sorted = sorted(columns_to_remove, reverse=True)

        updated_lines = []
        for line in lines:
            columns = line.split(';')
            # Remove as colunas a serem removidas na ordem decrescente
            for col_index in columns_to_remove_sorted:
                if col_index < len(columns):
                    del columns[col_index]
            updated_lines.append(';'.join(columns))

        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(updated_lines)

        logging.info(f"Successfully processed file: {file_path}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

def process_files_in_folder(folder, columns_to_remove):
    files = list(folder.glob('*.txt'))
    logging.info(f"Found {len(files)} files in folder '{folder}'")

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_file = {executor.submit(remove_columns_from_file, file, columns_to_remove): file for file in files}
        for future in as_completed(future_to_file):
            try:
                future.result()  # Trigger exception if any
            except Exception as e:
                logging.error(f"Error processing file: {e}")

def main():
    if not input_folder.exists():
        logging.error(f"Input folder {input_folder} does not exist")
        return

    columns_to_remove = [8, 9, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 43, 45, 46, 47, 53]  # Índices das colunas a serem removidas
    process_files_in_folder(input_folder, columns_to_remove)
    logging.info('All files have been processed.')

if __name__ == "__main__":
    main()

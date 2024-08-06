import os
import chardet
import psutil
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# Configuração de Logging
log_filename = '../logs/21_unify_estabele_by_date.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', 
                    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Limite de memória (4 GB)
MAX_MEMORY = 8 * 1024 * 1024 * 1024  # 4 GB

# Lista de anos e meses a serem incluídos
INCLUDE_YEARS = [2024]
INCLUDE_MONTHS = [1,2,3,4,5,6,7,8,9,10,11,12]

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
    return chardet.detect(raw_data)['encoding']

def read_file(file_path):
    try:
        encoding = detect_encoding(file_path)
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        return lines
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def write_intermediate_file(lines, output_folder, keyword, index):
    output_file_path = Path(output_folder) / f'intermediate_{keyword.lower()}_{index}.txt'
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    return output_file_path

def should_write_intermediate(total_memory):
    process = psutil.Process(os.getpid())
    return process.memory_info().rss + total_memory >= MAX_MEMORY

def extract_year_month(date_str):
    try:
        date_str = date_str.replace('"', '')
        year = int(date_str[:4])
        month = int(date_str[4:6])
        return year, month
    except Exception as e:
        logging.error(f"Error extracting year and month from date: {date_str}. Error: {e}")
        return None, None

def filter_lines_by_date(lines, year_col_index, include_years, include_months):
    filtered_lines = []
    for line in lines:
        columns = line.split(';')
        if len(columns) > year_col_index:
            year, month = extract_year_month(columns[year_col_index])
            if year in include_years and month in include_months:
                filtered_lines.append(line)
    return filtered_lines

def process_files(keyword, input_folder, output_folder, max_memory=MAX_MEMORY, year_col_index=10):
    start_time = datetime.now()
    intermediate_files = []
    total_memory = 0

    files = list(Path(input_folder).glob(f'*{keyword}*'))
    logging.info(f"Found {len(files)} files matching keyword '{keyword}'")

    if not files:
        logging.warning(f"No files found matching keyword '{keyword}' in {input_folder}")
        return

    all_lines = []

    with ProcessPoolExecutor(max_workers=2) as executor:
        future_to_file = {executor.submit(read_file, file): file for file in files}
        for future in as_completed(future_to_file):
            lines = future.result()
            if lines:
                filtered_lines = filter_lines_by_date(lines, year_col_index, INCLUDE_YEARS, INCLUDE_MONTHS)
                all_lines.extend(filtered_lines)
                total_memory += sum(len(line) for line in filtered_lines)
                if should_write_intermediate(total_memory):
                    intermediate_files.append(write_intermediate_file(all_lines, output_folder, keyword, len(intermediate_files)))
                    all_lines = []
                    total_memory = 0

    if all_lines:
        intermediate_files.append(write_intermediate_file(all_lines, output_folder, keyword, len(intermediate_files)))

    logging.info(f"Successfully processed {len(intermediate_files)} intermediate files for keyword '{keyword}'")

    if intermediate_files:
        with open(Path(output_folder) / f'unified_{keyword.lower()}.txt', 'w', encoding='utf-8') as final_output:
            for temp_file in intermediate_files:
                with open(temp_file, 'r', encoding='utf-8') as f:
                    final_output.writelines(f.readlines())

        logging.info(f'Final output file created successfully. Duration: {datetime.now() - start_time}')

        for f in intermediate_files:
            os.remove(f)
        logging.info(f'All intermediate files have been removed.')

def main():
    input_folder = "../3validated"
    output_folder = "../5unified_estabele"
    os.makedirs(output_folder, exist_ok=True)

    keywords = ['estabele']
    for keyword in keywords:
        process_files(keyword, input_folder, output_folder)

    logging.info('All files have been processed.')

if __name__ == "__main__":
    main()

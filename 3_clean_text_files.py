import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from unidecode import unidecode

# Configuração de Logging
log_filename = '../logs/3_clean_text_files.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])

# Função para pular duas linhas antes do primeiro log
def log_initial_message():
    with open(log_filename, 'a') as f:
        f.write("\n\n")

log_initial_message()

# Lista de extensões de arquivos a serem processados
FILE_EXTENSIONS = ['*.txt']

# Função para substituir caracteres acentuados pelos seus equivalentes sem acento
def remove_accented_characters(text):
    return unidecode(text)

# Função para processar um arquivo de texto
def process_file(txt_file):
    start_time = datetime.now()
    try:
        logging.info(f'Início do processamento do arquivo {txt_file}')
        with open(txt_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        processed_lines = []
        for line in lines:
            line = line.strip()  # Remove espaços em branco no início e no final
            line = line.upper()
            line = remove_accented_characters(line)  # Remove acentos
            processed_lines.append(line + '\n')

        # Sobrescrever o arquivo original com o texto processado
        with open(txt_file, 'w', encoding='utf-8') as file:
            file.writelines(processed_lines)
        
        logging.info(f'Arquivo {txt_file} processado com sucesso. Duração: {datetime.now() - start_time}')
        return True
    except Exception as e:
        logging.error(f'Erro ao processar arquivo {txt_file}: {e}')
        return False

# Função para processar todos os arquivos de texto em uma pasta
def process_files_in_folder(folder_path, error_folder, max_workers=8):
    try:
        txt_files = []
        for ext in FILE_EXTENSIONS:
            txt_files.extend(Path(folder_path).glob(ext))
        
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

# Função principal para definir a pasta de entrada e iniciar o processamento
def main(input_folder, error_folder, max_workers=8):
    input_path = Path(input_folder)
    error_path = Path(error_folder)
    error_path.mkdir(parents=True, exist_ok=True)
    process_files_in_folder(input_path, error_path, max_workers)

# Ponto de entrada do script
if __name__ == "__main__":
    input_folder = "../3validated"
    error_folder = "../errors"
    main(input_folder, error_folder, max_workers=8)

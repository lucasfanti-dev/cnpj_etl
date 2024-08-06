import subprocess

def run_script(script_name):
    try:
        # Executa o script e captura a saída e erros
        result = subprocess.run(['python', script_name], capture_output=True, text=True)
        # Verifica se houve algum erro na execução
        if result.returncode != 0:
            print(f"Erro ao executar {script_name}: {result.stderr}")
        else:
            print(f"Saída de {script_name}: {result.stdout}")
    except Exception as e:
        print(f"Erro ao tentar executar o script {script_name}: {str(e)}")

if __name__ == "__main__":
    scripts = [
        #'0_download_files.py',
        #'1_extract_files.py',
        #'2_process_csv_chunks.py',
        #'3_clean_text_files.py',
        #'4_process_large_csv_chunks.py',
        #'5_advanced_clean_text_files.py',
        #'6_clean_text_fields.py',
        #'7_replace_semicolon_pairs.py',
        #'8_remove_all_semicolons.py',
        #'9_restore_semicolons.py',
        #'10_replace_cnaecsv_values.py',
        #'11_replace_municipio_values.py',
        #'12_replace_matriz_filial.py',
        #'13_replace_situacao_cadastral.py',
        #'14_replace_natureza_juridica.py',
        #'15_replace_qualificacoes.py',
        #'16_replace_capital_social.py',
        #'17_replace_porte_empresa.py',
        #'18_replace_optante_simples.py',
        #'19_replace_moticsv_values.py',
        #'20_unify_simples_emprecsv.py',
        '21_unify_estabele_by_date.py',
        '22_equalize_columns_estabele.py',
        '23_unify_simples_estabele.py',
        '24_column_equalizer_simples_estabele.py',
        '25_unify_emprecsv_estabele.py',
        '26_equalize_columns_emprecsv_estabele.py',
        '27_divide_files_estabele.py',
        '28_remove_columns.py',
    ]

    for script in scripts:
        run_script(script)

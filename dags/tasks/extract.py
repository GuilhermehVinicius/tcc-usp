import pandas as pd
import glob

def extract():
    colunas = [
        "Nome", "Contrato", "Situação", "Data de nascimento", 
        "Data de cadastro", "Sexo", "Endereco", "Número", 
        "Bairro", "Cidade"
    ]

    clientes = pd.read_csv("include/data/clientes.csv", sep=",", usecols=colunas)

    # Caminho onde estão os arquivos (coloque o caminho correto da sua pasta)
    caminho = 'include/data/'  # Diretório atual ou o caminho onde estão os arquivos

    # Busca todos os arquivos que seguem o padrão "acessos-*.csv"
    arquivos = glob.glob(f'{caminho}acessos-*.csv')

    # Lista para armazenar cada DataFrame
    dfs = []

    # Percorre todos os arquivos e carrega no pandas
    for arquivo in arquivos:
        df = pd.read_csv(arquivo)
        dfs.append(df)

    # Junta todos os DataFrames em um só
    acessos = pd.concat(dfs, ignore_index=True)



    return clientes, acessos
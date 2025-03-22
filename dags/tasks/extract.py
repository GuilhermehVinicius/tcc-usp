import pandas as pd
import glob

def extract():
    colunas = [
        "Nome", "Contrato", "Situação", "Data de nascimento", 
        "Data de cadastro", "Sexo", "Endereco", "Número", 
        "Bairro", "Cidade"
    ]

    caminho = 'include/data/'

    clientes = pd.read_csv(f"{caminho}clientes.csv", sep=",", usecols=colunas)

    arquivos = glob.glob(f'{caminho}acessos-*.csv')

    dfs = []

    for arquivo in arquivos:
        df = pd.read_csv(arquivo)
        dfs.append(df)

    acessos = pd.concat(dfs, ignore_index=True)

    wellhub = pd.read_csv(f"{caminho}gym-wellhub.csv", sep=",")

    servicos_wellhub = pd.read_csv(f'{caminho}servicos.csv', sep=",")


    return clientes, acessos, wellhub, servicos_wellhub
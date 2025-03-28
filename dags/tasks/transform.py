import pandas as pd
import numpy as np

def normalize_text(text_series):
    """Normaliza strings removendo espaços extras e acentos"""
    return (text_series.str.lower()
                      .str.strip()
                      .str.normalize("NFKD")
                      .str.encode("ascii", "ignore")
                      .str.decode("utf-8"))

def transformar_clientes(clientes):
    """Transforma o DataFrame de clientes, realizando limpeza, normalização e enriquecimento de dados."""
    
    # Normalizar nome
    clientes["Nome"] = normalize_text(clientes["Nome"])

    # Ordenar e remover duplicatas
    clientes = clientes.sort_values(by=["Nome", "Data de cadastro"]).drop_duplicates(subset=["Nome"], keep="first")

    # Normalizar contrato
    clientes["Contrato"] = normalize_text(clientes["Contrato"])

    # Lista de funcionários para exclusão
    FUNCIONARIOS = {
        "alvaro gomes mateus neto", "douglas taveira regano", "bruno ferrari funchal", "lorena dos santos silva",
        "neydson domingos da silva", "jessica lane pereira", "jaqueline moreira pessoni",
        "loislene nascimento silva", "breno batista diniz", "samella lorranneh maia costa",
        "ana laura de freitas"
    }

    clientes = clientes[~clientes["Nome"].isin(FUNCIONARIOS)]

    # Criar endereço completo
    clientes["Endereco completo"] = clientes[["Endereco", "Bairro", "Cidade"]].astype(str).agg(", ".join, axis=1).replace(", , ", "", regex=True)

    clientes.loc[clientes["Endereco completo"] != "", "Endereco completo"] += "-SP, Brasil"

    # Converter datas
    clientes["Data de nascimento"] = pd.to_datetime(clientes["Data de nascimento"], errors="coerce")

    # Calcular idade e criar faixa etária
    data_atual = pd.Timestamp.today()
    clientes["Idade"] = ((data_atual - clientes["Data de nascimento"]).dt.days // 365).fillna(0).astype(int)

    faixas = [0, 18, 30, 45, 60, np.inf]
    labels = ["0-18", "19-30", "31-45", "46-60", "60+"]
    clientes["Faixa etaria"] = pd.cut(clientes["Idade"], bins=faixas, labels=labels, right=True)

    # Criar IDs
    clientes["id_cliente"] = clientes.index + 1

    clientes = clientes.rename(columns={'Nome': 'nome', 'Contrato': 'contrato', 'Situação': 'situacao',
                                        'Data de cadastro': 'data_cadastro', 'Sexo': 'sexo', 'Endereco completo': 'endereco', 
                                        'Idade': 'idade', 'Faixa etaria': 'faixa_etaria'})

    # Selecionar colunas finais de clientes
    return clientes[["id_cliente", "nome", "contrato", "situacao",
                     "data_cadastro", "sexo", "endereco", "idade", "faixa_etaria"]]


def transformar_acessos(acessos, clientes):
    """Transforma o DataFrame de acessos, limpando e categorizando os dados"""
    
    # Normalizar nomes nos acessos
    acessos["Cliente"] = normalize_text(acessos["Cliente"])

    # Filtrar acessos liberados
    acessos = acessos[acessos["Acesso liberado"] != "Não"]

    # Converter datas
    acessos["Acesso"] = pd.to_datetime(acessos["Acesso"], errors="coerce")

    # Criar colunas separadas de data e hora
    acessos["Data de acessos"] = acessos["Acesso"].dt.date
    acessos["Horas"] = acessos["Acesso"].dt.time

    # Criar faixas de horário
    faixa_horaria_labels = ["05:00-08:00", "08:01-12:00", "12:01-15:00", "15:01-18:00", "18:01-22:00"]
    faixa_horaria_bins = [pd.Timestamp("05:00"), pd.Timestamp("08:00"), pd.Timestamp("12:00"),
                          pd.Timestamp("15:00"), pd.Timestamp("18:00"), pd.Timestamp("22:00")]

    acessos["Faixa horario"] = pd.cut(acessos["Acesso"].dt.time.map(lambda x: pd.Timestamp(x.strftime("%H:%M"))),
                                       bins=faixa_horaria_bins,
                                       labels=faixa_horaria_labels,
                                       include_lowest=True)
    

    # Realizar merge com os dados dos clientes
    acessos = acessos.merge(clientes, left_on="Cliente", right_on="nome", how="inner")

    acessos = acessos.rename(columns={'Data de acessos': 'data_acesso', 'Faixa horario': 'faixa_horario'})

    acessos["id_acesso"] = acessos.index + 1

    
    # Selecionar e renomear colunas finais
    return acessos[["id_acesso", "id_cliente", "data_acesso", "faixa_horario"]].drop_duplicates()


def transformar_wellhub(wellhub,servicos_wellhub):
    wellhub = wellhub.rename(columns={'date': 'data', 
                                      'name': 'nome', 
                                      'address':'endereco',
                                      'services':'servicos',
                                      'comorbidities':'comorbidades',
                                      'base_plan':'plano_base',
                                      'values':'valor_plano'})
    

    wellhub["id_gym"] = wellhub.index + 1

    # Dividir os valores separados por vírgula em listas
    wellhub['servicos'] = wellhub['servicos'].str.split(',')
    wellhub['comorbidades'] = wellhub['comorbidades'].str.split(',')

    # Usar o método explode para transformar as listas em linhas separadas
    wellhub = wellhub.explode('servicos').explode('comorbidades')

    wellhub = wellhub.reset_index(drop=True)    

    wellhub["id_registro"] = wellhub.index + 1

    wellhub = wellhub.merge(servicos_wellhub, left_on="servicos", right_on="servicos", how="left")

    return wellhub
    


def criar_calendario():

    # DataFrame de calendário
    datas = pd.date_range(start="2023-10-01", end="2025-12-31")
    calender = pd.DataFrame({
        "data": datas,
        "ano": datas.year,
        "mes": datas.month,
        "nome_mes": datas.strftime("%b").str.capitalize(),
        "trimestre": datas.quarter.map(lambda x: f"T{x}"),
        "dia": datas.day,
        "dia_semana": datas.strftime("%A").str.capitalize(),
    })

    return calender




def transform(clientes, acessos, wellhub,servicos_wellhub):
    """Função principal que chama as transformações de clientes e acessos"""
    clientes_transformados = transformar_clientes(clientes)
    acessos_transformados = transformar_acessos(acessos, clientes_transformados)
    wellhub_transformados = transformar_wellhub(wellhub,servicos_wellhub)
    calendario_criado = criar_calendario()

    return clientes_transformados, acessos_transformados, wellhub_transformados, calendario_criado

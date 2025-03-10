from dw.schema import create_tables
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="postgres_dw")
engine_dw = hook.get_sqlalchemy_engine()

def load(clientes_transformados, acessos_transformados, calendario_criado):
    
    create_tables()

    # Processamento das tabelas estáticas
    STATIC_TABLES = {
        "dClientes": clientes_transformados,
        "dCalendar": calendario_criado,
        "fAcessos": acessos_transformados
    }

    # Função para salvar DataFrame no banco de dados
    def save_to_db(df, table_name):
        try:
            df.to_sql(table_name, con=engine_dw, if_exists = 'append', index=False)
            print(f"Tabela '{table_name}' salva com sucesso!")
        except Exception as e:
            print(f"Erro ao salvar a tabela '{table_name}': {e}")

    for table_name, df in STATIC_TABLES.items():
        save_to_db(df, table_name)



    #clientes_transformados.to_csv("include/clientes_transformados.csv",sep=",")

    #acessos_transformados.to_csv("include/acessos_transformados.csv",sep=",")


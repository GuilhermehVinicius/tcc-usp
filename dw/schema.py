from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.orm import declarative_base, relationship
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="postgres_dw")
engine_dw = hook.get_sqlalchemy_engine()

Base = declarative_base()

class DClientes(Base):
    __tablename__ = 'dClientes'
    id_cliente = Column(String(500), primary_key=True)
    nome = Column(String(500), nullable=False)
    contrato = Column(String(500))
    situacao = Column(String(500))
    data_cadastro = Column(Date)
    sexo = Column(String(500))
    endereco = Column(String(500))
    idade = Column(String(500))
    faixa_etaria = Column(String(500))

    acessos = relationship("FAcessos", back_populates="cliente")
    

class DCalendar(Base):
    __tablename__ = 'dCalendar'
    date = Column(Date, primary_key=True)
    year = Column(String(4), nullable=False)
    month = Column(String(2))
    nameMonth = Column(String(20))
    quarter = Column(String(3))
    day = Column(Integer)
    weekDay = Column(String(100))

    acessos = relationship("FAcessos", back_populates="dCalendar")
    wellhub = relationship("DGymWellhub", back_populates="dCalendar")

class DGymWellhub(Base):
    __tablename__ = 'dGymWellhub'
    id_registro = Column(Integer, autoincrement=True, primary_key=True)
    id_gym = Column(Integer)
    data = Column(Date, ForeignKey("dCalendar.date"), nullable=False)
    nome = Column(String(1000))
    endereco = Column(String(1000))
    servicos = Column(String(1000))
    grupo = Column(String(1000))
    comorbidades = Column(String(1000))
    plano_base = Column(String(500))
    valor_plano = Column(Float)

    dCalendar = relationship("DCalendar", back_populates="wellhub")
    

class FAcessos(Base):
    __tablename__ = 'fAcessos'
    id_acesso  = Column(String(500), primary_key=True)
    id_cliente = Column(String(500), ForeignKey("dClientes.id_cliente"))
    data_acesso = Column(Date, ForeignKey("dCalendar.date"))
    faixa_horario = Column(String(500), nullable=False)

    dCalendar = relationship("DCalendar", back_populates="acessos")
    cliente = relationship("DClientes", back_populates="acessos")

def create_tables():
    Base.metadata.create_all(bind=engine_dw)

if __name__ == "__main__":
    create_tables()
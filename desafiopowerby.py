import csv
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
import pandas as pd

engine = create_engine('sqlite:///clientes.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()



#Mapeamento do banco conforme solicitado
class Clientes(Base):
     __tablename__ = 'Clientes'
     
     id = Column("id", Integer, primary_key=True)
     NomeDoCliente = Column("NomeDoCliente", String(150))
     CEP = Column("CEP", String(50))
     DataDeNascimento = Column("DataDeNascimento", Date)

     #funçao para calculo de idade
     def calcular_idade(self):
         hoje = datetime.now()  # Pega a data atual
         idade = hoje.year - self.DataDeNascimento.year - ((hoje.month, hoje.day) < (self.DataDeNascimento.month, self.DataDeNascimento.day))
         return idade

     def ufCep(self):
         cepprefix = int(self.CEP[:2])
         if cepprefix<20:
            return 'SP'
         elif 20<cepprefix<29:
             return 'RJ'
         elif cepprefix == 69:
             return 'AC'

     def __init__(self, NomeDoCliente, CEP, DataDeNascimento):
         self.NomeDoCliente = NomeDoCliente
         self.CEP = CEP
         self.DataDeNascimento = DataDeNascimento

#iniciando banco
Base.metadata.create_all(bind=engine)

#funçao para classificar faixa etaria
def classificar_faixa_etaria():
    faixa_20_30 = 0
    faixa_31_60 = 0
    faixa_maior_60 = 0
    # Consultar todos os clientes no banco
    clientes = session.query(Clientes).all()

    for cliente in clientes:
        idade = cliente.calcular_idade()
        if 20 <= idade <= 30:
            faixa_20_30 += 1
        elif 31 <= idade <= 60:
            faixa_31_60 += 1
        elif idade > 60:
            faixa_maior_60 += 1
    print(f"Faixa 20 - 30 anos: {faixa_20_30} cliente(s)")
    print(f"Faixa 31 - 60 anos: {faixa_31_60} cliente(s)")
    print(f"Faixa maior que 60 anos: {faixa_maior_60} cliente(s)")

#classificar por UF
def classificar_UF():
    clientesSP = 0
    clientesRJ = 0
    clientesAC = 0
    # Consultar todos os clientes no banco
    clientes = session.query(Clientes).all()

    for cliente in clientes:
        UF = cliente.ufCep()
        if UF == 'SP':
            clientesSP += 1
        elif UF == 'RJ':
            clientesRJ += 1
        elif UF== 'AC':
            clientesAC += 1
    print(f"Cliente de SP: {clientesSP} cliente(s)")
    print(f"Cliente de RJ: {clientesRJ} cliente(s)")
    print(f"Cliente de AC: {clientesAC} cliente(s)")

#criar tabela final
def criarTabela():
    clientesUFidade = {'UF' : ['SP','RJ','AC'],
                       '20-30 anos' : [0, 0, 0],
                       '31-60 anos' : [0, 0, 0],
                       'Mais de 60 anos' : [0, 0, 0]}
    clientesUFidade_df = pd.DataFrame(clientesUFidade)
    # Consultar todos os clientes no banco
    clientes = session.query(Clientes).all()

    for cliente in clientes:
        UF = cliente.ufCep()
        idade = cliente.calcular_idade()
        if UF == 'SP' and 20 <= idade <= 30:
            clientesUFidade_df.iloc[0,1] += 1
        elif UF == 'SP' and 31 <= idade <= 60:
            clientesUFidade_df.iloc[0,2] += 1
        elif UF == 'SP' and idade > 60:
            clientesUFidade_df.iloc[0,3] += 1
        if UF == 'RJ' and 20 <= idade <= 30:
            clientesUFidade_df.iloc[1,1] += 1
        elif UF == 'RJ' and 31 <= idade <= 60:
            clientesUFidade_df.iloc[1,2] += 1
        elif UF == 'RJ' and idade > 60:
            clientesUFidade_df.iloc[1,3] += 1
        if UF == 'AC' and 20 <= idade <= 30:
            clientesUFidade_df.iloc[2,1] += 1
        elif UF == 'AC' and 31 <= idade <= 60:
            clientesUFidade_df.iloc[2,2] += 1
        elif UF == 'AC' and idade > 60:
            clientesUFidade_df.iloc[2,3] += 1
    return clientesUFidade_df

with open ("tbclientes.csv","r") as tabela:
    tabela_csv = csv.reader(tabela, delimiter=";")
    for i, linha in enumerate(tabela_csv):
        if i != 0 :
            data = datetime.strptime(linha[2], '%d/%m/%Y').date()
            clienteNovo = Clientes(NomeDoCliente=linha[0], CEP=linha[1], DataDeNascimento=data)
            session.add(clienteNovo)
            session.commit()
classificar_faixa_etaria()
classificar_UF()
TabelaPowerBI = criarTabela()
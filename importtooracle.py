import sqlalchemy
import datetime
import numpy as np
import cx_Oracle
import oraclecon as con
import pandas as pd

engine = sqlalchemy.create_engine(con.strengine)

df = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\countryxref.xlsx',
                   dtype=str, usecols = "A:C", sheet_name='Sheet3',
                   names = ['siscntry', 'cntryname', 'liaisoncntry'])

df.to_sql('countryxref', engine, con.schema, if_exists='replace', chunksize=1,
                 dtype=
                 {  'siscntry':sqlalchemy.types.VARCHAR(3),
                    'cntryname':sqlalchemy.types.VARCHAR(df.cntryname.str.len().max()),
                    'liaisoncntry':sqlalchemy.types.VARCHAR(df.liaisoncntry.str.len().max())
                 })


'''
df = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR2Original.xlsx',
                   dtype=str, skiprows=1, usecols = "C:H", sheet_name='95_and_up',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])
df1 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR2Original.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])

df2 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matches.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])

df3 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matches.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])

df4 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR3Original.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])

df5 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR3Original.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])
df6 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR4Original.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])

df7 = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\matchesR4Original.xlsx',
                   dtype = str, skiprows=1, usecols = "C:H", sheet_name='90_to_94',
                   names = ['matchratio', 'namel', 'nameiu', 'cntry', 'mdbcode', 'siscode'])



dfconcat = pd.concat([df,df1],ignore_index=True)
dfconcat = pd.concat([dfconcat,df2],ignore_index=True)
dfconcat = pd.concat([dfconcat,df3],ignore_index=True)
dfconcat = pd.concat([dfconcat,df4],ignore_index=True)
dfconcat = pd.concat([dfconcat,df5],ignore_index=True)
dfconcat = pd.concat([dfconcat,df6],ignore_index=True)
dfconcat = pd.concat([dfconcat,df7],ignore_index=True)


dfconcat.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)

engine = sqlalchemy.create_engine(con.strengine)

# df.to_sql('liaisonorgsnonus', engine, con.schema, if_exists='replace', chunksize=1)

dfconcat.to_sql('extorgoriginalmatches', engine, con.schema, if_exists='replace', chunksize=1,
                 dtype=
                 {  'namel':sqlalchemy.types.VARCHAR(100),
                    'nameiu':sqlalchemy.types.VARCHAR(df.nameiu.str.len().max()),
                    'cntry':sqlalchemy.types.VARCHAR(df.cntry.str.len().max()),
                    'mdbcode':sqlalchemy.types.VARCHAR(df.mdbcode.str.len().max()),
                    'siscode':sqlalchemy.types.VARCHAR(df.siscode.str.len().max())
                 })
'''

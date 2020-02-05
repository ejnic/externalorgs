import pandas as pd
import sqlalchemy
import oraclecon as con


df = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\OrgIDMatchesforLiaison.xlsx',
                   skiprows=1, usecols = "B:H", sheet_name='Sheet1',
                   names = ['keep', 'matchratio', 'nameL', 'nameIU', 'cntry', 'liaison', 'iu'],
                   converters={'iu':str})
df.keep.fillna('Yes', inplace=True)
df = df[(df.keep == 'Yes')]

engine = sqlalchemy.create_engine(con.strengine)

df.to_sql('extorgsmatched', engine, con.schema, if_exists='replace', chunksize=1000,
                 dtype=
                 {'liaison':sqlalchemy.types.VARCHAR(df.liaison.str.len().max()),
                 'iu':sqlalchemy.types.VARCHAR(df.iu.str.len().max())
                 })

print(df.head(10))


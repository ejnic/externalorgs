import pandas as pd
import sqlalchemy
import oraclecon as con


df = pd.read_excel('C:\\Users\\ejnic\\Google Drive Personal\\Python\\externalorgs\\files\\OrgIDMatchesforLiaison.xlsx',
                   skiprows=1, usecols = "A:G", sheet_name='Sheet1',
                   names = ['keep', 'matchratio', 'nameL', 'nameIU', 'cntry', 'liaison', 'iu'])



engine = sqlalchemy.create_engine(con.strengine)

df.to_sql('extorgsmatched', engine, con.schema, if_exists='replace', chunksize=1000,
                 dtype=
                 {'campus':sqlalchemy.types.VARCHAR(df.campus.str.len().max()),
                 'first_name':sqlalchemy.types.VARCHAR(df.first_name.str.len().max()),
                 'last_name':sqlalchemy.types.VARCHAR(df.last_name.str.len().max()),
                 'email':sqlalchemy.types.VARCHAR(df.email.str.len().max()),
                 'programname':sqlalchemy.types.VARCHAR(df.programname.str.len().max()),
                 'webadmitname':sqlalchemy.types.VARCHAR(df.webadmitname.str.len().max()),
                 'roles':sqlalchemy.types.VARCHAR(df.roles.str.len().max()),
                 'networkid':sqlalchemy.types.VARCHAR(25),
                 'is_active':sqlalchemy.types.VARCHAR(2),
                 'users_created_at':sqlalchemy.types.VARCHAR(50),
                 'users_created_at2':sqlalchemy.types.VARCHAR(50),
                 'last_login_at':sqlalchemy.types.VARCHAR(50),
                 'login_count':sqlalchemy.types.VARCHAR(25)
                 })

print(df.head(10))


import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import sqlalchemy
import datetime
import numpy as np
import cx_Oracle
import oraclecon as con
import ExtOrgClass as eo

engine = sqlalchemy.create_engine(con.strengine)

#load external orgs from file from Liaison
dfl = pd.read_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\LiaisonMasterCollegeCodeList.xlsx', skiprows=1, usecols = "A:D", names = ['orgname','state','country','mdbcode'])
dfcountryl = pd.DataFrame(dfl.country.unique())
print(dfcountryl)
for ind in dfl.index:
    if dfl['country'][ind] == 'KE':
        print(dfl['orgname'][ind])

#     if
#     print(df['country'][ind])


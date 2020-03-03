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

#load external orgs from file from Liaison
dfl = pd.read_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\LiaisonMasterCollegeCodeList.xlsx', skiprows=1, usecols = "A:D", names = ['orgname','state','country','mdbcode'])
dforgs = eo.Orgs()


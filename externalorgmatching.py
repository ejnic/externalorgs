#!/usr/bin/env python
# coding: utf-8
# import subprocess

import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import sqlalchemy
import datetime
# import jellyfish
import numpy as np
import cx_Oracle
import oraclecon as con

# subprocess.call([r'C:\Python\instantclient.bat'])



#load external orgs from file from Liaison
df = pd.read_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\LiaisonMasterCollegeCodeList.xlsx', skiprows=1, usecols = "A:D", names = ['orgname','state','country','mdbcode'])
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)
df['orgname'] = df['orgname'].str.upper()
df = df[df.country != 'US']
df['orgtype'] = ""
df['orgmatch'] = df['orgname']

#IU EXTERNAL ORGS
engine = sqlalchemy.create_engine(con.strengine)
strsql = '''select  
    to_char(a.EXT_ORG_ID) as orgid, 
    a.ext_org_schl_typ_cd as typecd, 
    CASE WHEN a.ext_org_long_desc = ' ' THEN a.ext_org_desc ELSE a.ext_org_long_desc END as orgname,
    a.ext_org_cntry_cd as country, 
    a.ext_org_st_cd as state
from 
    dss_rds.PSE_EXT_ORG_GT a
WHERE 
    a.ext_org_cntry_cd != 'USA'
    and 
        a.ext_org_eff_stat_cd = 'A'
    and
        a.ext_org_cntry_cd in ('CAN','CHN','IND', 'GBR')
    and
        a.EXT_ORG_SCHL_TYP_CD in ('CC' ,'COL' , 'PRF')'''
#a.ext_org_cntry_cd in ('CAN','CHN','IND', 'GBR')
dfiu = pd.read_sql_query(strsql, engine)
dfiu['orgname'] = dfiu['orgname'].str.upper()
dfiu['orgtype'] = ""
dfiu['orgmatch'] = dfiu['orgname']



df.loc[df['orgname'].str.contains(r'UNIV'), 'orgtype'] = 'UNIVERSITY'
df['orgname'] = df['orgname'].str.replace("\sUNIVERSITY", "")
df.loc[df['orgname'].str.contains(r'COLL'), 'orgtype'] = 'COLLEGE'
df['orgname'] = df['orgname'].str.replace("COLLEGE", "")
df.loc[df['orgname'].str.contains(r'SCHOOL'), 'orgtype'] = 'SCHOOL'
df['orgname'] = df['orgname'].str.replace("\sSCHOOL", "")
df.loc[df['orgname'].str.contains(r'INSTITUTE'), 'orgtype'] = 'INSTITUTE'
df['orgname'] = df['orgname'].str.replace('INSTITUTE', '')
df.loc[df['orgname'].str.contains(r'ACADEMY'), 'orgtype'] = 'ACADEMY'
df['orgname'] = df['orgname'].str.replace('ACADEMY', '')
df.loc[df['orgname'].str.contains(r'CONSERVATORY'), 'orgtype'] = 'CONSERVATORY'
df['orgname'] = df['orgname'].str.replace("CONSERVATORY", " ")

df['orgname'] = df['orgname'].str.replace(" OF ", " ")
df['orgname'] = df['orgname'].str.replace("OF ", " ")
df['orgname'] = df['orgname'].str.replace(" AND ", " ")
df['orgname'] = df['orgname'].str.replace(" & ", " ")
df['orgname'] = df['orgname'].str.replace(" THE ", " ")
df['orgname'] = df['orgname'].str.replace("THE ", "")
df['orgname'] = df['orgname'].str.replace(".", "")
df['orgname'] = df['orgname'].str.replace(",", "")
df['orgname'] = df['orgname'].str.replace("'", "")
df['orgtype'] = df['orgtype'].replace(np.nan, '')

df['neworgname']=df['orgname'] + ' ' + df['orgtype']

df['system'] = 'Liaison'

#df['namenysiis']=jellyfish.nysiis(df['neworgname'])

df.to_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\df.xlsx')
dfleft = df.iloc[:,[3,4,6,2,7]]
dfleft.columns = ['code', 'orgname','matchname','country','system']

dfleftin = dfleft[dfleft.country == 'IN']
dfleftch = dfleft[dfleft.country == 'CH']
dfleftgb = dfleft[dfleft.country == 'GB']
dfleftca = dfleft[dfleft.country == 'CA']



dfiu.loc[dfiu['orgname'].str.contains(r'UNIV'), 'orgtype'] = 'UNIVERSITY'
dfiu['orgname'] = dfiu['orgname'].str.replace("UNIVERSITY", "")
dfiu['orgname'] = dfiu['orgname'].str.replace("\sUNIV", "")
dfiu.loc[dfiu['orgname'].str.contains(r'COLL'), 'orgtype'] = 'COLLEGE'
dfiu['orgname'] = dfiu['orgname'].str.replace("\sCOLLEGE", "")
dfiu.loc[dfiu['orgname'].str.contains(r'SCHOOL'), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.contains(r' SCH '), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.endswith(r' SCH'), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.contains(r' SCHL '), 'orgtype'] = 'SCHOOL'
dfiu['orgname'] = dfiu['orgname'].str.replace('\sSCHOOL', '')
dfiu['orgname'] = dfiu['orgname'].str.replace(' SCH ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace('\s SCHL ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace('\s SCH', '')
dfiu.loc[dfiu['orgname'].str.contains(r'INSTITUTE'), 'orgtype'] = 'INSTITUTE'
dfiu['orgname'] = dfiu['orgname'].str.replace('\sINSTITUTE', '')
dfiu.loc[dfiu['orgname'].str.contains(r'ACADEMY'), 'orgtype'] = 'ACADEMY'
dfiu['orgname'] = dfiu['orgname'].str.replace('\sACADEMY', '')
dfiu.loc[dfiu['orgname'].str.contains(r' INST '), 'orgtype'] = 'INSTITUTE'
dfiu['orgname'] = dfiu['orgname'].str.replace('\s INST ', '')
dfiu.loc[dfiu['orgname'].str.startswith(r'INST '), 'orgtype'] = 'INSTITUTE'
dfiu['orgname'] = dfiu['orgname'].str.replace('INST ', '')
dfiu.loc[dfiu['orgname'].str.startswith(r'U '), 'orgtype'] = 'UNIVERSITY'
dfiu.loc[dfiu['orgname'].str.contains(r' U '), 'orgtype'] = 'UNIVERSITY'
dfiu.loc[dfiu['orgname'].str.endswith(r' U'), 'orgtype'] = 'UNIVERSITY'
dfiu['orgname'] = dfiu['orgname'].str.replace(' U', '')
dfiu['orgname'] = dfiu['orgname'].str.replace('U ', '')
dfiu['orgname'] = dfiu['orgname'].str.replace(' U ', ' ')
dfiu.loc[dfiu['orgname'].str.contains(r' COL '), 'orgtype'] = 'COLLEGE'
dfiu.loc[dfiu['orgname'].str.startswith(r'COLL '), 'orgtype'] = 'COLLEGE'
dfiu['orgname'] = dfiu['orgname'].str.replace("\s COL ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace("COLL ", "")
dfiu.loc[dfiu['orgname'].str.contains(r'CONSERVATORY'), 'orgtype'] = 'CONSERVATORY'
dfiu['orgname'] = dfiu['orgname'].str.replace("CONSERVATORY", " ")

dfiu['orgname'] = dfiu['orgname'].str.replace(' OF ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace('OF ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace(' AND ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace(' & ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace(' THE ', ' ')
dfiu['orgname'] = dfiu['orgname'].str.replace('THE ', '')
dfiu['orgname'] = dfiu['orgname'].str.replace('.', '')
dfiu['orgname'] = dfiu['orgname'].str.replace(',', '')
dfiu['orgname'] = dfiu['orgname'].str.replace(''', '')

dfiu['orgtype'] = dfiu['orgtype'].replace(np.nan, '')
dfiu['neworgname'] = dfiu['orgname'] + ' ' + dfiu['orgtype']
dfiu['system'] = 'SIS'

dfiu.to_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\dfiu.xlsx')
dfright = dfiu.iloc[:, [0,6,7,3,8]]
dfright.columns = ['code', 'orgname','matchname','country', 'system']

dfrightin = dfright[dfright.country == 'IND']
dfrightch = dfright[dfright.country == 'CHN']
dfrightgb = dfright[dfright.country == 'GBR']
dfrightca = dfright[dfright.country == 'CAN']

dfleftch['key'] = 0
dfrightch['key'] = 0
dfleftin['key'] = 0
dfrightin['key'] = 0
dfleftgb['key'] = 0
dfrightgb['key'] = 0
dfleftca['key'] = 0
dfrightca['key'] = 0
dfcartch = pd.merge(dfleftch, dfrightch, on='key')
dfcartin = pd.merge(dfleftin, dfrightin, on='key')
dfcartgb = pd.merge(dfleftgb, dfrightgb, on='key')
dfcartca = pd.merge(dfleftca, dfrightca, on='key')

dfcartch['matchratio'] = dfcartch.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartin['matchratio'] = dfcartin.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartgb['matchratio'] = dfcartgb.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartca['matchratio'] = dfcartca.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfconcat = dfcartch
dfconcat = pd.concat([dfconcat, dfcartin], ignore_index=True)
dfconcat = pd.concat([dfconcat, dfcartgb], ignore_index=True)
dfconcat = pd.concat([dfconcat, dfcartca], ignore_index=True)

dfconcat.sort_values(by=['code_x', 'matchratio'], ascending=True, inplace=True)
dfunique = dfconcat.drop_duplicates(subset=['code_x'], keep='last')
dfunique.sort_values(by=['code_y', 'matchratio'], ascending=True, inplace=True)
dfunique = dfunique.drop_duplicates(subset=['code_y'], keep='last')

dfconcat['code_y'] = dfconcat['code_y'].apply(str)
dfconcat['code_y'] = dfconcat['code_y'].apply('{:0>10}'.format)

dfunique['keep'] = ''
dfunique = dfunique[['keep', 'matchratio', 'orgname_x', 'orgname_y', 'country_y', 'code_x', 'code_y']]
dfunique.columns = ['keep', 'matchratio', 'nameL', 'nameIU', 'cntry', 'liaison', 'iu']
dfunique.sort_values(by=['cntry', 'nameL'], ascending=True, inplace=True)

matches94 = dfunique.query('matchratio > 94')

matches9093 = dfunique.query('89 < matchratio < 95')

with pd.ExcelWriter('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\matches.xlsx') as writer:  # doctest: +SKIP
    matches94.to_excel(writer, sheet_name='95_and_up')
    matches9093.to_excel(writer, sheet_name='90_to_94')
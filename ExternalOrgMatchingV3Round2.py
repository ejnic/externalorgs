#!/usr/bin/env python
# coding: utf-8

import fuzzywuzzy
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
import pandas as pd
import sqlalchemy
import datetime
import jellyfish
import numpy as np
import oraclecon as con

engine = sqlalchemy.create_engine(con.strengine)

#load external orgs from file from Liaison
df = pd.read_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\LiaisonMasterCollegeCodeList.xlsx', skiprows=1, usecols = "A:D", names = ['orgname','state','country','mdbcode'])
df.replace({r'[^\x00-\x7F]+':''}, regex=True, inplace=True)

df = df[df.country != 'US']

df['orgmatch'] = df['orgname']
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

dfleftmx = dfleft[dfleft.country == 'MX']
dfleftrs = dfleft[dfleft.country == 'RS']
dfleftks = dfleft[dfleft.country == 'KS']
dfleftfr = dfleft[dfleft.country == 'FR']
dfleftng = dfleft[dfleft.country == 'NI']

#IU EXTERNAL ORGS
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
        a.ext_org_cntry_cd in ('RUS','FRA','KOR', 'MEX', 'NGA')
    and
        a.EXT_ORG_SCHL_TYP_CD in ('CC' ,'COL' , 'PRF')'''

#a.ext_org_cntry_cd in ('RUS','FRA','KOR', 'MEX', 'NGA')
dfiu = pd.read_sql_query(strsql, engine)
dfiu['orgname'] = dfiu['orgname'].str.upper()
dfiu['orgtype'] = ""
dfiu['orgmatch'] = dfiu['orgname']

dfiu.loc[dfiu['orgname'].str.contains(r'UNIV'), 'orgtype'] = 'UNIVERSITY'
dfiu['orgname'] = dfiu['orgname'].str.replace("UNIVERSITY", "")
dfiu['orgname'] = dfiu['orgname'].str.replace("\sUNIV", "")
dfiu.loc[dfiu['orgname'].str.contains(r'COLL'), 'orgtype'] = 'COLLEGE'
dfiu['orgname'] = dfiu['orgname'].str.replace("\sCOLLEGE", "")
dfiu.loc[dfiu['orgname'].str.contains(r'SCHOOL'), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.contains(r' SCH '), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.endswith(r' SCH'), 'orgtype'] = 'SCHOOL'
dfiu.loc[dfiu['orgname'].str.contains(r' SCHL '), 'orgtype'] = 'SCHOOL'
dfiu['orgname'] = dfiu['orgname'].str.replace("\sSCHOOL", "")
dfiu['orgname'] = dfiu['orgname'].str.replace(" SCH ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace("\s SCHL ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace("\s SCH", "")
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

dfiu['orgname'] = dfiu['orgname'].str.replace(" OF ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace("OF ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace(" AND ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace(" & ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace(" THE ", " ")
dfiu['orgname'] = dfiu['orgname'].str.replace("THE ", "")
dfiu['orgname'] = dfiu['orgname'].str.replace(".", "")
dfiu['orgname'] = dfiu['orgname'].str.replace(",", "")
dfiu['orgname'] = dfiu['orgname'].str.replace("'", "")

dfiu['orgtype'] = dfiu['orgtype'].replace(np.nan, '')
dfiu['neworgname']=dfiu['orgname'] + ' ' + dfiu['orgtype']
dfiu['system'] = 'SIS'

dfiu.to_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\dfiu.xlsx')
dfright = dfiu.iloc[:,[0,6,7,3,8]]
dfright.columns = ['code', 'orgname','matchname','country','system']

# a.ext_org_cntry_cd in ('RUS','FRA','KOR', 'MEX', 'NGA')
dfrightrs = dfright[dfright.country == 'RUS']
dfrightfr = dfright[dfright.country == 'FRA']
dfrightks = dfright[dfright.country == 'KOR']
dfrightmx = dfright[dfright.country == 'MEX']
dfrightng = dfright[dfright.country == 'NGA']

dfleftrs['key'] = 0
dfrightrs['key'] = 0
dfleftfr['key'] = 0
dfrightfr['key'] = 0
dfleftks['key'] = 0
dfrightks['key'] = 0
dfleftmx['key'] = 0
dfrightmx['key'] = 0
dfleftng['key'] = 0
dfrightng['key'] = 0
dfcartrs = pd.merge(dfleftrs, dfrightrs, on='key')
dfcartfr = pd.merge(dfleftfr, dfrightfr, on='key')
dfcartks = pd.merge(dfleftks, dfrightks, on='key')
dfcartmx = pd.merge(dfleftmx, dfrightmx, on='key')
dfcartng = pd.merge(dfleftng, dfrightng, on='key')

# a.ext_org_cntry_cd in ('RUS','FRA','KOR', 'MEX', 'NGA')
dfcartrs['matchratio'] = dfcartrs.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartfr['matchratio'] = dfcartfr.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartks['matchratio'] = dfcartks.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartmx['matchratio'] = dfcartmx.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartng['matchratio'] = dfcartng.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfconcat = dfcartrs
dfconcat = pd.concat([dfconcat,dfcartfr],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartks],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartmx],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartng],ignore_index=True)

dfconcat.sort_values(by=['code_x', 'matchratio'], ascending=True, inplace=True)
dfunique = dfconcat.drop_duplicates(subset=['code_x'], keep='last')
dfunique.sort_values(by=['code_y', 'matchratio'], ascending=True, inplace=True)
dfunique = dfunique.drop_duplicates(subset=['code_y'], keep='last')

dfconcat['code_y'] = dfconcat['code_y'].apply(str)
dfconcat['code_y'] = dfconcat['code_y'].apply('{:0>10}'.format)

dfunique['keep']=''
dfunique = dfunique[['keep', 'matchratio','orgname_x', 'orgname_y', 'country_y', 'code_x', 'code_y' ]]
dfunique.columns = ['keep', 'matchratio','nameL', 'nameIU', 'cntry', 'liaison', 'iu']
dfunique.sort_values(by=['cntry', 'nameL'], ascending=True, inplace=True)

matches95 = dfunique.query('matchratio > 94')

matches9094 = dfunique.query('89 < matchratio < 95')

with pd.ExcelWriter('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\pycharmsmatchesR2.xlsx') as writer:  # doctest: +SKIP
    matches95.to_excel(writer, sheet_name='95_and_up')
    matches9094.to_excel(writer, sheet_name='90_to_94')

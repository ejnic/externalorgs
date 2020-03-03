import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import sqlalchemy
import datetime
import oraclecon as con
import numpy as np

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

df.to_excel('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\df.xlsx')
dfleft = df.iloc[:,[3,4,6,2,7]]
dfleft.columns = ['code', 'orgname','matchname','country','system']

dfleftae = dfleft[dfleft.country == 'AE']
dfleftbe = dfleft[dfleft.country == 'BE']
dfleftbu = dfleft[dfleft.country == 'BU']
dfleftbo = dfleft[dfleft.country == 'BO']
dfleftba = dfleft[dfleft.country == 'BA']
dfleftbl = dfleft[dfleft.country == 'BL']
dfleftcu = dfleft[dfleft.country == 'CU']
dfleftet = dfleft[dfleft.country == 'ET']
dfleftfi = dfleft[dfleft.country == 'FI']
dfleftgm = dfleft[dfleft.country == 'GM']
dfleftha = dfleft[dfleft.country == 'HA']
dfleftid = dfleft[dfleft.country == 'ID']
dfleftei = dfleft[dfleft.country == 'EI']
dfleftjm = dfleft[dfleft.country == 'JM']
dfleftja = dfleft[dfleft.country == 'JA']
dfleftkz = dfleft[dfleft.country == 'KZ']
dfleftkg = dfleft[dfleft.country == 'KG']
dfleftzi = dfleft[dfleft.country == 'ZI']
dfleftza = dfleft[dfleft.country == 'ZA']
dflefttw = dfleft[dfleft.country == 'TW']
dflefttu = dfleft[dfleft.country == 'TU']

dfleftsz = dfleft[dfleft.country == 'SZ']
dfleftsp = dfleft[dfleft.country == 'SP']
dfleftsf = dfleft[dfleft.country == 'SF']
dfleftsa = dfleft[dfleft.country == 'SA']
dfleftpl = dfleft[dfleft.country == 'PL']
dfleftno = dfleft[dfleft.country == 'NO']
dfleftke = dfleft[dfleft.country == 'KE']
dfleftit = dfleft[dfleft.country == 'IT']
dfleftis = dfleft[dfleft.country == 'IS']
dfleftgr = dfleft[dfleft.country == 'GR']

dfleftgh = dfleft[dfleft.country == 'GH']
dfleftda = dfleft[dfleft.country == 'DA']
dfleftcs = dfleft[dfleft.country == 'CS']
dfleftbh = dfleft[dfleft.country == 'BH']
dfleftbc = dfleft[dfleft.country == 'BC']
dfleftbb = dfleft[dfleft.country == 'BB']
dfleftba = dfleft[dfleft.country == 'BA']
dfleftau = dfleft[dfleft.country == 'AU']
dfleftar = dfleft[dfleft.country == 'AR']

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
        a.ext_org_cntry_cd in ('ARE','BEL','BGR','BLR','BMU','BOL','CUB','ETH','FLK','GMB','HTI','IDN','IRL','JAM',
        'KAZ','KGZ','PAN','PRY', 'ZWE', 'ZMB','TWN','TUR','CHE','ESP','ZAF','SAU','POL','NOR','KEN','ITA',
        'ISR','GRC','GHA', 'DNK','CRI','BLZ','BWA','BRB','BHR', 'AUT','ARG')
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
dfrightae = dfright[dfright.country == 'ARE']
dfrightbe = dfright[dfright.country == 'BEL']
dfrightbu = dfright[dfright.country == 'BGR']
dfrightbo = dfright[dfright.country == 'BLR']
dfrightba = dfright[dfright.country == 'BMU']
dfrightbl = dfright[dfright.country == 'BOL']
dfrightcu = dfright[dfright.country == 'CUB']
dfrightet = dfright[dfright.country == 'ETH']
dfrightfi = dfright[dfright.country == 'FLK']
dfrightgm = dfright[dfright.country == 'GMB']
dfrightha = dfright[dfright.country == 'HTI']
dfrightid = dfright[dfright.country == 'IDN']
dfrightei = dfright[dfright.country == 'IRL']
dfrightjm = dfright[dfright.country == 'JAM']
dfrightkz = dfright[dfright.country == 'KAZ']
dfrightkg = dfright[dfright.country == 'KGZ']
dfrightzi = dfright[dfright.country == 'ZWE']
dfrightza = dfright[dfright.country == 'ZMB']
dfrighttw = dfright[dfright.country == 'TWN']
dfrighttu = dfright[dfright.country == 'TUR']

dfrightsz = dfright[dfright.country == 'CHE']
dfrightsp = dfright[dfright.country == 'ESP']
dfrightsf = dfright[dfright.country == 'ZAF']
dfrightsa = dfright[dfright.country == 'SAU']
dfrightpl = dfright[dfright.country == 'POL']
dfrightno = dfright[dfright.country == 'NOR']
dfrightke = dfright[dfright.country == 'KEN']
dfrightit = dfright[dfright.country == 'ITA']
dfrightis = dfright[dfright.country == 'ISR']
dfrightgr = dfright[dfright.country == 'GRC']

dfrightgh = dfright[dfright.country == 'GHA']
dfrightda = dfright[dfright.country == 'DNK']
dfrightcs = dfright[dfright.country == 'CRI']
dfrightbh = dfright[dfright.country == 'BLZ']
dfrightbc = dfright[dfright.country == 'BWA']
dfrightbb = dfright[dfright.country == 'BRB']
dfrightba = dfright[dfright.country == 'BHR']
dfrightau = dfright[dfright.country == 'AUT']
dfrightar = dfright[dfright.country == 'ARG']


dfleftae['key'] = 0
dfleftbe['key'] = 0
dfleftbu['key'] = 0
dfleftbo['key'] = 0
dfleftba['key'] = 0
dfleftbl['key'] = 0
dfleftcu['key'] = 0
dfleftet['key'] = 0
dfleftfi['key'] = 0
dfleftgm['key'] = 0
dfleftha['key'] = 0
dfleftid['key'] = 0
dfleftei['key'] = 0
dfleftjm['key'] = 0
dfleftkz['key'] = 0
dfleftkg['key'] = 0
dfleftzi['key'] = 0
dfleftza['key'] = 0
dflefttw['key'] = 0
dflefttu['key'] = 0

dfleftsz['key'] = 0
dfleftsp['key'] = 0
dfleftsf['key'] = 0
dfleftsa['key'] = 0
dfleftpl['key'] = 0
dfleftno['key'] = 0
dfleftke['key'] = 0
dfleftit['key'] = 0
dfleftis['key'] = 0
dfleftgr['key'] = 0

dfleftgh['key'] = 0
dfleftda['key'] = 0
dfleftcs['key'] = 0
dfleftbh['key'] = 0
dfleftbc['key'] = 0
dfleftbb['key'] = 0
dfleftba['key'] = 0
dfleftau['key'] = 0
dfleftar['key'] = 0

dfrightae['key'] = 0
dfrightbe['key'] = 0
dfrightbu['key'] = 0
dfrightbo['key'] = 0
dfrightba['key'] = 0
dfrightbl['key'] = 0
dfrightcu['key'] = 0
dfrightet['key'] = 0
dfrightfi['key'] = 0
dfrightgm['key'] = 0
dfrightha['key'] = 0
dfrightid['key'] = 0
dfrightei['key'] = 0
dfrightjm['key'] = 0
dfrightkz['key'] = 0
dfrightkg['key'] = 0
dfrightzi['key'] = 0
dfrightza['key'] = 0
dfrighttw['key'] = 0
dfrighttu['key'] = 0

dfrightsz['key'] = 0
dfrightsp['key'] = 0
dfrightsf['key'] = 0
dfrightsa['key'] = 0
dfrightpl['key'] = 0
dfrightno['key'] = 0
dfrightke['key'] = 0
dfrightit['key'] = 0
dfrightis['key'] = 0
dfrightgr['key'] = 0

dfrightgh['key'] = 0
dfrightda['key'] = 0
dfrightcs['key'] = 0
dfrightbh['key'] = 0
dfrightbc['key'] = 0
dfrightbb['key'] = 0
dfrightba['key'] = 0
dfrightau['key'] = 0
dfrightar['key'] = 0


dfcartae = pd.merge(dfleftae, dfrightae, on='key')
dfcartbe = pd.merge(dfleftbe, dfrightbe, on='key')
dfcartbu = pd.merge(dfleftbu, dfrightbu, on='key')
dfcartbo = pd.merge(dfleftbo, dfrightbo, on='key')
dfcartba = pd.merge(dfleftba, dfrightba, on='key')
dfcartbl = pd.merge(dfleftbl, dfrightbl, on='key')
dfcartcu = pd.merge(dfleftcu, dfrightcu, on='key')
dfcartet = pd.merge(dfleftet, dfrightet, on='key')
dfcartfi = pd.merge(dfleftfi, dfrightfi, on='key')
dfcartgm = pd.merge(dfleftgm, dfrightgm, on='key')
dfcartha = pd.merge(dfleftha, dfrightha, on='key')
dfcartid = pd.merge(dfleftid, dfrightid, on='key')
dfcartei = pd.merge(dfleftei, dfrightei, on='key')
dfcartjm = pd.merge(dfleftjm, dfrightjm, on='key')
dfcartkz = pd.merge(dfleftkz, dfrightkz, on='key')
dfcartkg = pd.merge(dfleftkg, dfrightkg, on='key')
dfcartzi = pd.merge(dfleftzi, dfrightzi, on='key')
dfcartza = pd.merge(dfleftza, dfrightza, on='key')
dfcarttw = pd.merge(dflefttw, dfrighttw, on='key')
dfcarttu = pd.merge(dflefttu, dfrighttu, on='key')

dfcartsz = pd.merge(dfleftsz, dfrightsz, on='key')
dfcartsp = pd.merge(dfleftsp, dfrightsp, on='key')
dfcartsf = pd.merge(dfleftsf, dfrightsf, on='key')
dfcartsa = pd.merge(dfleftsa, dfrightsa, on='key')
dfcartpl = pd.merge(dfleftpl, dfrightpl, on='key')
dfcartno = pd.merge(dfleftno, dfrightno, on='key')
dfcartke = pd.merge(dfleftke, dfrightke, on='key')
dfcartit = pd.merge(dfleftit, dfrightit, on='key')
dfcartis = pd.merge(dfleftis, dfrightis, on='key')
dfcartgr = pd.merge(dfleftgr, dfrightgr, on='key')

dfcartgh = pd.merge(dfleftgh, dfrightgh, on='key')
dfcartda = pd.merge(dfleftda, dfrightda, on='key')
dfcartcs = pd.merge(dfleftcs, dfrightcs, on='key')
dfcartbh = pd.merge(dfleftbh, dfrightbh, on='key')
dfcartbc = pd.merge(dfleftbc, dfrightbc, on='key')
dfcartbb = pd.merge(dfleftbb, dfrightbb, on='key')
dfcartba = pd.merge(dfleftba, dfrightba, on='key')
dfcartau = pd.merge(dfleftau, dfrightau, on='key')
dfcartar = pd.merge(dfleftar, dfrightar, on='key')

dfcartae['matchratio'] = dfcartae.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbe['matchratio'] = dfcartbe.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbu['matchratio'] = dfcartbu.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbo['matchratio'] = dfcartbo.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartba['matchratio'] = dfcartba.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbl['matchratio'] = dfcartbl.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartcu['matchratio'] = dfcartcu.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartet['matchratio'] = dfcartet.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartfi['matchratio'] = dfcartfi.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartgm['matchratio'] = dfcartgm.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartha['matchratio'] = dfcartha.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartid['matchratio'] = dfcartid.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartei['matchratio'] = dfcartei.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartjm['matchratio'] = dfcartjm.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartkz['matchratio'] = dfcartkz.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartkg['matchratio'] = dfcartkg.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartzi['matchratio'] = dfcartzi.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartza['matchratio'] = dfcartza.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcarttw['matchratio'] = dfcarttw.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcarttu['matchratio'] = dfcarttu.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartsz['matchratio'] = dfcartsz.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartsp['matchratio'] = dfcartsp.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartsf['matchratio'] = dfcartsf.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartsa['matchratio'] = dfcartsa.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartpl['matchratio'] = dfcartpl.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartno['matchratio'] = dfcartno.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartke['matchratio'] = dfcartke.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartit['matchratio'] = dfcartit.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartis['matchratio'] = dfcartis.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartgr['matchratio'] = dfcartgr.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfcartgh['matchratio'] = dfcartgh.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartda['matchratio'] = dfcartda.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartcs['matchratio'] = dfcartcs.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbh['matchratio'] = dfcartbh.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbc['matchratio'] = dfcartbc.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartbb['matchratio'] = dfcartbb.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartba['matchratio'] = dfcartba.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartau['matchratio'] = dfcartau.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)
dfcartar['matchratio'] = dfcartar.apply(lambda s: fuzz.partial_ratio(s['matchname_x'], s['matchname_y']), axis=1)

dfconcat = dfcartae
dfconcat = pd.concat([dfconcat,dfcartbe],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbu],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbo],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartba],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbl],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartcu],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartet],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartfi],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartgm],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartha],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartid],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartei],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartjm],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartkz],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartkg],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartzi],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartza],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcarttw],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcarttu],ignore_index=True)

dfconcat = pd.concat([dfconcat,dfcartsz],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartsp],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartsf],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartsa],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartpl],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartno],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartke],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartit],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartis],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartgr],ignore_index=True)

dfconcat = pd.concat([dfconcat,dfcartgh],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartda],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartcs],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbh],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbc],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartbb],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartba],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartau],ignore_index=True)
dfconcat = pd.concat([dfconcat,dfcartar],ignore_index=True)


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


with pd.ExcelWriter('N:\\eApp\\Liaison\\LiaisonExternalOrgs\\matchesR4.xlsx') as writer:  # doctest: +SKIP
    matches95.to_excel(writer, sheet_name='95_and_up')
    matches9094.to_excel(writer, sheet_name='90_to_94')







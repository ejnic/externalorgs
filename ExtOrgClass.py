class Orgs:

    # also called constructor
    def __init__(self, df, system):
        self.df = df
        self.system = system

    def CleanName(self):
        self.df.replace({r'[^\x00-\x7F]+': ''}, regex=True, inplace=True)

        self.df['orgname'] = self.df['orgname'].str.upper()
        self.df['orgtype'] = ''
        self.df['orgmatch'] = self.df['orgname']

        self.df.loc[df['orgname'].str.contains(r'UNIV'), 'orgtype'] = 'UNIVERSITY'
        self.df['orgname'] = self.df['orgname'].str.replace("\sUNIVERSITY", "")
        self.df.loc[self.df['orgname'].str.contains(r'COLL'), 'orgtype'] = 'COLLEGE'
        self.df['orgname'] = self.df['orgname'].str.replace("COLLEGE", "")
        self.df.loc[self.df['orgname'].str.contains(r'SCHOOL'), 'orgtype'] = 'SCHOOL'
        self.df['orgname'] = self.df['orgname'].str.replace("\sSCHOOL", "")
        self.df.loc[self.df['orgname'].str.contains(r'INSTITUTE'), 'orgtype'] = 'INSTITUTE'
        self.df['orgname'] = self.df['orgname'].str.replace('INSTITUTE', '')
        self.df.loc[self.df['orgname'].str.contains(r'ACADEMY'), 'orgtype'] = 'ACADEMY'
        self.df['orgname'] = self.df['orgname'].str.replace('ACADEMY', '')
        self.df.loc[self.df['orgname'].str.contains(r'CONSERVATORY'), 'orgtype'] = 'CONSERVATORY'
        self.df['orgname'] = self.df['orgname'].str.replace('CONSERVATORY', ' ')

        self.df['orgname'] = self.df['orgname'].str.replace(" OF ", " ")
        self.df['orgname'] = self.df['orgname'].str.replace("OF ", " ")
        self.df['orgname'] = self.df['orgname'].str.replace(" AND ", " ")
        self.df['orgname'] = self.df['orgname'].str.replace(" & ", " ")
        self.df['orgname'] = self.df['orgname'].str.replace(" THE ", " ")
        self.df['orgname'] = self.df['orgname'].str.replace("THE ", "")
        self.df['orgname'] = self.df['orgname'].str.replace(".", "")
        self.df['orgname'] = self.df['orgname'].str.replace(",", "")
        self.df['orgname'] = self.df['orgname'].str.replace("'", "")
        self.df['orgtype'] = self.df['orgtype'].replace(np.nan, '')

        self.df['neworgname']=self.df['orgname'] + ' ' + self.df['orgtype']

        self.df['system'] = system
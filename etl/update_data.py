import os 
import requests
from urllib.request import urlretrieve
from glob import glob
from datetime import date
import numpy as np
import pandas as pd
from datetime import date
from io import BytesIO
import re
import time
from zipfile import ZipFile
import ssl
import warnings
warnings.filterwarnings('ignore')
ssl._create_default_https_context = ssl._create_unverified_context

"""
OBJECTIVE:
Contributions and expenditures, travel, loans, debts
Grouped by individual, sorted by amount and date
"""

# Cols of interest
contribs_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'contributionDt', 'contributionAmount', 
            'contributorPersentTypeCd', 'contributorNameOrganization', 
            'contributorNameLast', 'contributorNameFirst', 'contributorEmployer', 'contributorStreetCity',
            'contributorStreetPostalCode', 'contributorStreetStateCd', 'contributorStreetCountryCd']
expend_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'expendDt', 'expendAmount', 
            'expendDescr', 'expendCatCd', 'politicalExpendCd',
            'payeePersentTypeCd', 'payeeNameOrganization', 'payeeNameLast', 'payeeNameFirst',
            'payeeStreetCity', 'payeeStreetPostalCode', 'payeeStreetStateCd', 'payeeStreetCountryCd']
# travel_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'parentDt', 'parentType', 'parentId', 
#             'transportationTypeCd', 'transportationTypeDescr', 'departureCity', 'arrivalCity', 'departureDt', 'arrivalDt',
#             'travelPurpose']
loans_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'loanInfoId', 'loanDt', 'loanAmount',
            'lenderPersentTypeCd', 'lenderNameOrganization', 'lenderNameLast', 'lenderNameFirst', 'lenderEmployer',
            'lenderStreetCity', 'lenderStreetPostalCode', 'lenderStreetStateCd', 'lenderStreetCountryCd']
# debts_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'loanInfoId', 
#             'lenderPersentTypeCd', 'lenderNameOrganization', 'lenderNameLast', 'lenderNameFirst']
#             # 'lenderStreetPostalCode', 'lenderStreetStateCd', 'lenderStreetCountryCd']
cover_cols = ['filerIdent', 'periodStartDt', 'periodEndDt', 
        'unitemizedContribAmount', 'totalContribAmount', # positive
        'unitemizedExpendAmount', 'totalExpendAmount', # negative
        'loanBalanceAmount', # negative
        'contribsMaintainedAmount', # positive
        'unitemizedLoanAmount', # negative
        'totalInterestEarnedAmount'] # positive
colsort_dict = {'status': 7, 'type': 6, 'employer': 5, 'name_organization': 4,  'name': 3, 'ident': 2, 'record': 1, 'report': 0}




def sorted_cols(el):
    for kw in colsort_dict:
        if kw in el:
            return colsort_dict[kw]
    return 6



def make_sorted_cols(data):
    cols = list(data.columns)
    cols.sort(key=sorted_cols)
    data = data[cols]
    return data



def clean_filer_data(file):
    filers = pd.read_csv(file, dtype=str, parse_dates=['filerEffStartDt', 'filerEffStopDt'])\
        [['filerIdent', 'filerTypeCd', 'filerPersentTypeCd', 'filerName', 'filerFilerpersStatusCd', 'filerHoldOfficeCd',  # Removed 'filerStreetPostalCode', 'filerStreetStateCd', 'filerStreetCountryCd'
        'filerHoldOfficeDistrict', 'contestSeekOfficeCd', 'contestSeekOfficeDistrict', 'filerEffStartDt', 'filerEffStopDt']]\
        .sort_values('filerEffStartDt')\
        .fillna('')

    return filers



def clean_and_export(var, zf, filers, filenames, cols, datecols):
    """
    Given series of filenames or filename patterns for a var (e.g. contributions), 
    reads and merges each df with filers df, concatenates dfs, and exports last five years of data for that var.
    """

    print(f'Cleaning and exporting {var}', " "*80)
    
    # Retrieving filenames and concatenating dfs
    dfs = []
    try:
        date_filter = datecols[1]
    except:
        date_filter = False

    date_parser = lambda date: pd.to_datetime(date, errors='coerce')

    for filename in filenames:
        if '*' in filename:
            start, end = filename.split('*')
            files = [file for file in zf.namelist() if file.startswith(start) and file.endswith(end)]
            for file in files:
                print('\tLoading', file.split('/')[-1], " "*80, end='\r')
                df = pd.read_csv(zf.open(file), usecols=cols, dtype=str, parse_dates=datecols, date_parser=date_parser)
                try:
                    df = df[df[date_filter] >= pd.Timestamp.now().normalize() - pd.DateOffset(years=5)]
                except:
                    print('\t**Could not filter last 5 years of data**')
                df = df.merge(filers, how='left')
                dfs.append(df)
        else:
            print('\tLoading', filename.split('/')[-1], " "*80, end='\r')
            df = pd.read_csv(zf.open(filename), usecols=cols, dtype=str, parse_dates=datecols, date_parser=date_parser)
            try:
                df = df[df[date_filter] >= pd.Timestamp.now().normalize() - pd.DateOffset(years=5)]
            except:
                print('\t**Could not filter last 5 years of data**')
            df = df.merge(filers, how='left')
            dfs.append(df)
    print('\tConcatenating files', " "*80, end='\r')
    data = pd.concat(dfs, ignore_index=True)


    # Filling NAs and cleaning strs
    print('\tFilling NAs and cleaning strs', " "*80, end='\r')
    obj_cols = [col for col in data.columns if 'Amount' not in col and 'Id' not in col and 'Dt' not in col]
    for col in obj_cols:
        data[col] = data[col].fillna('')
        if 'Type' not in col:
            data[col] = data[col].apply(lambda x: x.title().replace(r'\r+|\n+|\t+','').replace(r'[^A-Za-z0-9 ]+', '').strip())

    # Consolidating name cols
    print('\tConsolidating name cols', " "*80, end='\r')
    namecolprefixls = [col.split('NameFirst')[0] for col in data.columns if 'NameFirst' in col]
    for prefix in namecolprefixls:
        data[prefix + 'Name'] = np.where(
            (data[prefix + 'NameLast'] != '') & (data[prefix + 'NameFirst'] != '') &
            (data[prefix + 'NameLast'] + data[prefix + 'NameFirst'] != np.nan),
            data[prefix + 'NameLast'] + ', ' + data[prefix + 'NameFirst'],
            np.nan)
        data[prefix + 'Name'] = data[prefix + 'Name'].str.strip()
        data.drop(columns=[prefix + 'NameLast', prefix + 'NameFirst'], inplace=True)
        data[prefix + 'Name'] = np.where(data[prefix + 'NameOrganization'] != '', data[prefix + 'NameOrganization'], data[prefix + 'Name'])
        data.drop(columns=[prefix + 'NameOrganization'], inplace=True)

    # Cleaning col names
    print('\tCleaning col names', " "*80, end='\r')
    data.columns = [re.sub( '(?<!^)(?=[A-Z])', '_', col.replace('Cd', '')).lower() for col in data.columns]
    data = make_sorted_cols(data)

    # Downloading data
    for letter, group in data.groupby(data.filer_name.str[:3]):
        group.to_csv(f'{os.getcwd()}/data/processed/{var}/{var}_{letter}.csv', index=False)
        print('\tDonwloaded', letter, end='\r')
    
    return data



def clean_cover(zf):
    cover = pd.read_csv(zf.open('filers.csv'), usecols=cover_cols)
    for col in ['periodStartDt', 'periodEndDt']:
        cover[col] = pd.to_datetime(cover[col], errors='coerce')
    cover['balance'] = cover[['unitemizedContribAmount', 'totalContribAmount', 'contribsMaintainedAmount', 'totalInterestEarnedAmount']].fillna(0).sum(axis=1) - \
        cover[['unitemizedExpendAmount', 'totalExpendAmount', 'loanBalanceAmount', 'unitemizedLoanAmount']].fillna(0).sum(axis=1)

    cover = cover[['filerIdent', 'periodStartDt', 'periodEndDt', 'balance']]
    cover.columns = [re.sub( '(?<!^)(?=[A-Z])', '_', col).lower() for col in cover.columns]
    
    cover.to_csv(f'{os.getcwd()}/data/processed/cover.csv')


def main():

    startTime = time.time()

    # Downloading and extracting file
    print('Updating data')
    zipfile = BytesIO(requests.get('https://www.ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip').content)
    zf = ZipFile(zipfile)
    # zf = ZipFile(f'{os.getcwd()}/data/source/TEC_CF.zip')
                
    # Loading filer data
    filers = make_sorted_cols(clean_filer_data(zf.open('filers.csv')))
    filers.to_csv(f'{os.getcwd()}/data/processed/filers.csv', index=False)

    # Processing and downloading data
    clean_and_export('contribs', zf, filers, [f'contribs_{n}*.csv' for n in range(3, 9)] + ['cont_ss.csv', 'cont_t.csv'], contribs_cols, ['receivedDt', 'contributionDt']) # Contributions
    clean_and_export('expend', zf, filers, ['expend_*.csv', 'expn_t.csv'], expend_cols, ['receivedDt', 'expendDt']) # Expenditures
    clean_and_export('loans', zf, filers, ['loans.csv'], loans_cols, ['receivedDt', 'loanDt']) # Loans

    # Cleaning and downloading cover data

    with open(f'{os.getcwd()}/data/documentation/last_update.txt', 'w') as f:
        f.write(date.today().strftime(format='%b %d, %Y'))

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


if __name__ == '__main__':
    main()
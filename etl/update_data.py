import os 
import requests
from urllib.request import urlretrieve
from glob import glob
from datetime import date
import numpy as np
import pandas as pd
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
            'payeePersentTypeCd', 'payeeNameOrganization', 'payeeNameLast', 'payeeNameFirst']
            # 'payeeStreetPostalCode', 'payeeStreetStateCd', 'payeeStreetCountryCd']
travel_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'parentDt', 'parentType', 'parentId', 
            'transportationTypeCd', 'transportationTypeDescr', 'departureCity', 'arrivalCity', 'departureDt', 'arrivalDt',
            'travelPurpose']
loans_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'loanInfoId', 'loanDt', 'loanAmount',
            'lenderPersentTypeCd', 'lenderNameOrganization', 'lenderNameLast', 'lenderNameFirst', 'lenderEmployer']
            # 'lenderStreetPostalCode', 'lenderStreetStateCd', 'lenderStreetCountryCd']
debts_cols = ['recordType', 'reportInfoIdent', 'filerIdent', 'receivedDt', 'loanInfoId', 
            'lenderPersentTypeCd', 'lenderNameOrganization', 'lenderNameLast', 'lenderNameFirst']
            # 'lenderStreetPostalCode', 'lenderStreetStateCd', 'lenderStreetCountryCd']
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

    # office_cols = [col for col in filers.columns if 'Office' in col]
    # filers['filerStatus'] = np.where((filers.filerHoldOfficeCd != '') | (filers.contestSeekOfficeCd != ''), 
    #                                 filers.filerFilerpersStatusCd + '/ ' + filers[office_cols].apply(lambda row: ' '.join(row.values.astype(str)).strip(), axis=1),
    #                                 filers.filerFilerpersStatusCd
    #                                 )
    # filers.drop(columns=office_cols, inplace=True)
    # filers.drop(columns=['filerFilerpersStatusCd'], inplace=True)

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

    print(f'\tExporting {var} ({len(data)} rows)', " "*80, end='\r')
    # data.to_csv(f'{os.getcwd()}/data/processed/{var}_5y.csv.zip', index=False, compression='zip')


    for letter, group in data.groupby(data.filer_name.str[:3]):
        group.to_csv(f'{os.getcwd()}/data/processed/{var}_{letter}.csv', index=False)
        print('\tDonwloaded', letter, end='\r')
    
    return data



def make_monthly_table(var, data, keyword, prefix):
    """
    Given a var, its corresponding data, the individual type keyword 
    and the prefix for amount and date columns, groups and pivots monthly 
    totals. Exports full datawrapper table and summary table to data/processed.
    """

    print(f'Making monthly table for {var} by {keyword}', " "*80)
    
    # Removing zero rows
    print('\tRemoving zero rows', " "*80, end='\r')
    data[prefix + '_amount'] = data[prefix + '_amount'].astype(float)
    data = data[data[prefix + '_amount'] > 0]
    
    # Getting col lists
    print('\tGetting col lists', " "*80, end='\r')
    key_cols = [col for col in data.columns if col.startswith(keyword)]
    key_groupcols = key_cols.copy()
    key_groupcols.append(pd.Grouper(key=prefix + '_dt', freq='m'))
    
    # Grouping data
    print('\tGrouping data', " "*80, end='\r')
    grouped = data.groupby(key_groupcols)[prefix + '_amount'].sum().reset_index()
    grouped.rename(columns={prefix + '_dt': prefix + '_month'}, inplace=True)
    
    # Creating 5-year total column
    print('\tCreating 5-year total col', " "*80, end='\r')
    grouped[prefix + '_5-year_total'] = grouped.groupby(key_cols)[prefix + '_amount'].transform('sum')

    key_cols.append(prefix + '_5-year_total')
    
    # Pivoting on months
    print('\tPivoting on months', " "*80, end='\r')
    pivoted = grouped.pivot(index=key_cols, values=prefix + '_amount', columns=prefix + '_month')
    pivoted.fillna(0, inplace=True)
    pivoted.columns = [col.strftime(format='%b %Y') for col in pivoted.columns]
    pivoted[f'max_{prefix}_month'] = pivoted.idxmax(axis=1).astype(str)
    pivoted[f'max_{prefix}_monthly_total'] = pivoted.max(axis=1)
    pivoted[f'{prefix}_1-month_difference'] = pivoted.iloc[:, -3] - pivoted.iloc[:, -4]

    # Exporting dw table
    print('\tExporting dw table', " "*80, end='\r')
    pivoted_dw = pivoted.copy().reset_index()
    pivoted_dw[f'max_{prefix}_monthly_total'] = pivoted_dw[f'max_{prefix}_monthly_total'].apply(lambda x: '{:,}'.format(round(x, 2))
    ).astype(str) + ' ^' + pivoted_dw[f'max_{prefix}_month'] + '^'
    pivoted_dw.drop(columns=[f'max_{prefix}_month', f'max_{prefix}_monthly_total'])
    pivoted_dw.columns.name = 'index'
    pivoted_dw.sort_values([pivoted_dw.columns[-4], prefix + '_5-year_total'], ascending=False, inplace=True)
    pivoted_dw = pivoted_dw.head(10000) # Filtering top 10000 by last month's amount and 5-year total
    pivoted_dw.columns = pivoted_dw.columns.str.replace('_', ' ').str.title()
    pivoted_dw.to_csv(f'{os.getcwd()}/data/processed/dw/{var}_monthly_table_by_{keyword}_dw.csv', index=False)
    
    # Exporting summary table
    print('\tExporting summary table', " "*80, end='\r')
    pivoted = pivoted.iloc[:, -5:]
    pivoted.reset_index(inplace=True)
    pivoted.columns.name = 'index'
    pivoted.sort_values([pivoted.columns[-4], prefix + '_5-year_total'], ascending=False, inplace=True)
    pivoted = pivoted.head(10000) # Filtering top 10000 by last month's amount and 5-year total
    pivoted.columns = pivoted.columns.str.replace('_', ' ').str.title()
    pivoted.to_csv(f'{os.getcwd()}/data/processed/{var}_monthly_table_by_{keyword}.csv', index=False)

    del pivoted



def main():

    startTime = time.time()

    # Downloading and extracting file
    print('Updating data')
    # zipfile = requests.get('https://www.ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip').content
    # with open(f'{os.getcwd()}/data/source/TEC_CF.zip', 'wb') as f:
    #     f.write(zipfile)
    # zf = ZipFile(zipfile)
    zf = ZipFile(f'{os.getcwd()}/data/source/TEC_CF.zip')
                
    # Loading filer data
    filers = make_sorted_cols(clean_filer_data(zf.open('filers.csv')))
    filers.to_csv(f'{os.getcwd()}/data/processed/filers.csv', index=False)

    # Combine, clean and export contributions and expenditures
    contribs = clean_and_export('contribs', zf, filers, [f'contribs_{n}*.csv' for n in range(3, 9)] + ['cont_ss.csv', 'cont_t.csv'], contribs_cols, ['receivedDt', 'contributionDt']) # Contributions
    # expend = clean_and_export('expend', zf, filers, ['expend_*.csv', 'expn_t.csv'], expend_cols, ['receivedDt', 'expendDt']) # Expenditures
    # clean_and_export('travel', zf, filers, ['travel.csv'], travel_cols, ['receivedDt', 'parentDt', 'departureDt', 'arrivalDt']) # Travel
    # loans = clean_and_export('loans', zf, filers, ['loans.csv'], loans_cols, ['receivedDt', 'loanDt']) # Loans
    # clean_and_export('debts', zf, filers, ['debts.csv'], debts_cols, ['receivedDt']) # Debts

    # Making monthly tables
    # make_monthly_table('contribs', contribs, 'filer', 'contribution')
    # make_monthly_table('contribs', contribs, 'contributor', 'contribution')
    # make_monthly_table('expend', expend, 'filer', 'expend')
    # make_monthly_table('expend', expend, 'payee', 'expend')
    # make_monthly_table('loans', loans, 'filer', 'loan')
    # make_monthly_table('loans', loans, 'lender', 'loan')

    executionTime = (time.time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))


if __name__ == '__main__':
    main()
import pandas as pd
import numpy as np
import re
import os
from zipfile import ZipFile
from csv import reader
import altair as alt
import streamlit as st
import warnings
from urllib import request
warnings.filterwarnings('ignore')
import time

# Page settings
st.set_page_config(
        
        layout='wide',
        initial_sidebar_state="expanded"
    )
st.image('https://upload.wikimedia.org/wikipedia/commons/d/d9/Austin_American-Statesman_%282019-10-31%29.svg', width=300)
m = st.markdown("""
<style>
div.stButton > button:first-child {
    background-color: #ff4c4c;
    color: white;
}
</style>""", unsafe_allow_html=True)

footer="""<style>
a:link , a:visited{
color: blue;
background-color: transparent;
text-decoration: underline;
}

a:hover,  a:active {
color: red;
background-color: transparent;
text-decoration: underline;
}

.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: white;
color: black;
text-align: center;
}
</style>
<div class="footer">
<p>Developed by <a href="mailto: cghisolfi@gannett.com" target="_blank">Caroline Ghisolfi</a></p>
</div>
"""


st.title('Campaign Finance Data Tool')

with request.urlopen('https://data-statesman.s3.amazonaws.com/tec-campaign-finance/documentation/last_update.txt') as f:
    last_update = f.read().decode('utf-8')

st.markdown(f'##### Last update: {last_update}')
st.markdown("""
This application processes and visualizes the **last five years** of campaign finance data released by the Texas Ethics Commission. 
The raw data is available for download [here](https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt). According to your selection of filers, 
the application will display contribution, expenditure and loan data for each filer
and then compare the filers you selected and their contributors, payees and lenders.

To begin, select one or more filers by **TYPE** and **NAME**. 

""")


@st.cache
def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')


@st.cache
def load_filers():
    filers = pd.read_csv('https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/filers.csv', dtype={'filer_ident': str})
    filers.columns = [re.sub( '(?<!^)(?=[A-Z])', ' ', col.replace('Cd', '')).title() for col in filers.columns]
    filers = filers[(filers['Filer Name'].str.lower().str.contains('use, do not|not to be use|do not') == False)]
    filers['Filer Filerpers Status'] = filers['Filer Filerpers Status'].str.replace('_', ' ')

    return filers


def filter_data(filername, dtype):

    # Load dtype vars
    var, prefix, var_short = dtype[0], dtype[1], dtype[2]

    # Filter data
    filer_initials = filername[:3]
    if var == 'Expenditure':
        vardt = 'expend'
    else:
        vardt = var
    try:
        data = pd.read_csv(f'https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/{var_short}/{var_short}_{filer_initials}.csv', low_memory=False, parse_dates=[f'{vardt.lower()}_dt', 'received_dt'])
        data.columns = data.columns.str.replace('_', ' ').str.title().str.replace('Expend', 'Expenditure')
        data[[f'{prefix} Street City', f'{prefix} Street State', f'{prefix} Street Postal Code', f'{prefix} Street Country']] = \
            data[[f'{prefix} Street City', f'{prefix} Street State', f'{prefix} Street Postal Code', f'{prefix} Street Country']].fillna('').astype(str)
        data[f'{prefix} Location'] = data[f'{prefix} Street City'] + ', ' + data[f'{prefix} Street State'] + ' ' + \
            data[f'{prefix} Street Postal Code'] + ', ' + data[f'{prefix} Street Country']
        filtered_data = data[data['Filer Name'].str.lower() == filername.lower()]
        filtered_data['year'] = filtered_data[f'{var} Dt'].dt.year
        return filtered_data
    except:
        return []


def display_filertable(filertable):
    with st.expander('Filer Information'):
            st.dataframe(filertable.fillna(''))


def display_stats(dtype):

    # Load dtype vars
    var, prefix, varshort, filtered_data = dtype[0], dtype[1], dtype[2], dtype[3]

    if len(filtered_data) > 0:

        # Get stats
        this_year = filtered_data.year.max()

        if this_year - 1 in filtered_data.year.unique():
            monthly = filtered_data.groupby([pd.Grouper(key=f'{var} Dt', freq='M'), 'year'])\
                            .agg(amount = (f'{var} Amount', 'sum'), count = (f'{var} Amount', 'count')).reset_index().fillna(0)

            avg_amount_this_year = round(monthly[monthly.year == this_year]['amount'].mean())
            avg_amount_last_year = round(monthly[monthly.year == this_year - 1]['amount'].mean())
            avg_amount_diff = round(avg_amount_this_year - avg_amount_last_year)

            avg_count_this_year = round(monthly[monthly.year == this_year]['count'].mean())
            avg_count_last_year = round(monthly[monthly.year == this_year - 1]['count'].mean())
            avg_count_diff = round(avg_count_this_year - avg_count_last_year)

            top_contrib_this_year = filtered_data[filtered_data.year == this_year].sort_values(f'{var} Amount', ascending=False).iloc[0]
            top_contrib_this_year_name = top_contrib_this_year[f'{prefix} Name']
            top_contrib_this_year_info = 'Located at zipcode ' + str(top_contrib_this_year[f'{prefix} Street Postal Code']) + ' in ' + str(top_contrib_this_year[f'{prefix} Street City']) + ', ' + str(top_contrib_this_year[f'{prefix} Street State'].upper())
            try:
                if top_contrib_this_year[f'{prefix} Employer'] != '':
                    top_contrib_this_year_info = 'Employed by ' + str(top_contrib_this_year[f'{prefix} Employer']) + ' in ' + str(top_contrib_this_year[f'{prefix} Street City']) + ', ' + str(top_contrib_this_year[f'{prefix} Street State'].upper())
            except:
                pass

            # Display stats
            col1, col2, col3 = st.columns(3)
            col1.metric(label=f"Avg Monthly {var} Amount ({this_year})", value='${:,}'.format(avg_amount_this_year), delta='${:,}'.format(avg_amount_diff))
            col2.metric(label=f"Avg Monthly {var} Count ({this_year})", value='{:,}'.format(avg_count_this_year), delta='{:,}'.format(avg_count_diff))
            col3.metric(label=f'Top {prefix} ({this_year})', value=top_contrib_this_year_name, delta=top_contrib_this_year_info, delta_color='off')


def display_data(dtype, filername):

    # Load dtype vars
    var, prefix, var_short, filtered_data = dtype[0], dtype[1], dtype[2], dtype[3]

    # Display data table
    if len(filtered_data) > 0:
        date_min, date_max = filtered_data[f'{var} Dt'].min().strftime(format='%b %d, %Y'), filtered_data[f'{var} Dt'].max().strftime(format='%b %d, %Y')
        for col in [f'{var} Dt', 'Received Dt']:
            filtered_data[col] = pd.to_datetime(filtered_data[col]).apply(lambda x: x.strftime(format='%Y-%m-%d'))
        with st.expander(f'{var}s'):
            st.markdown(f'**Date range**: {date_min} - {date_max}')
            data_csv = convert_df(filtered_data.drop(columns='year'))
            st.download_button(
                label=f"Download {var.lower()} data",
                data=data_csv,
                file_name=f"data_{var.lower()}_{filername}.csv",
                mime='text/csv'
            )
            st.dataframe(filtered_data.fillna(''))

    else:
        st.write(f'Insufficient {var.lower()} data to display')


def get_common(concat_dfs, year, dtype):

    var, prefix = dtype[0], dtype[1]

    if year != 'All':
        concat_dfs = concat_dfs[concat_dfs.year == year]
    grouped = concat_dfs.fillna('').groupby([col for col in concat_dfs.columns if prefix.lower() in col.lower()]).agg(
        contrib_amount = (f'{var} Amount', 'sum'), 
        filers_count = ('Filer Name', 'nunique'), 
        Filers = ('Filer Name', lambda x: ', '.join(x.unique()))).reset_index()

    common = grouped[grouped.filers_count > 1]\
    .rename(columns={'contrib_amount': f'{var}s Total'})\
    [[f'{prefix} Name', f'{prefix} Persent Type', f'{prefix} Location', f'{var}s Total', 'Filers']].reset_index(drop=True)

    for col in common.columns:
        if list(common[col].unique()) == ['']:
            common.drop(columns=[col], inplace=True)

    return common


def display_common(common, dtype, names):

    var, prefix = dtype[0], dtype[1]

    if len(common) > 0:
        common_csv = convert_df(common)
        names_forfile = '_'.join(names)
        st.download_button(
            label=f"Download shared {prefix.lower()}s data",
            data=common_csv,
            file_name=f"shared_{prefix.lower()}_{names_forfile}.csv",
            mime='text/csv'
        )
        st.dataframe(common)
    else:
        st.write('No data to display')


@st.cache
def make_chart(concat_dfs, var):

    concat_dfs[f'{var} Dt'] = pd.to_datetime(concat_dfs[f'{var} Dt'])
    grouped = concat_dfs.groupby(['Filer Name', pd.Grouper(key=f'{var} Dt', freq='m')])[f'{var} Amount'].sum().reset_index()
    grouped['Filer Name'] = grouped['Filer Name'].apply(lambda x: re.sub("([\(\[]).*?([\)\]])", "", x))
    chart = alt.Chart(grouped).mark_line().encode(
                                    x=alt.X(f'{var} Dt', axis=alt.Axis(labelAngle=0), title=''), 
                                      y=alt.Y(f'{var} Amount', title='', ), 
                                      color=alt.Color('Filer Name', legend=alt.Legend(title='', labelFontSize=15)),
                                      tooltip=alt.Tooltip(f'{var} Amount',format=",.2f")
                                     ).properties(width=1000)
    return chart



def display_chart(concat_dfs, dtype):

    var, prefix = dtype[0], dtype[1]

    st.markdown(f'**{var} Monthly Totals ($)**')
    st.altair_chart(make_chart(concat_dfs, var), use_container_width=True)
    st.markdown(f'**Shared {prefix}s**')

#____________________________________________________________ GET FILER DATA ____________________________________________________________


def get_filer_data():
    # Load filers
    filers = load_filers()

    # Display filters
    st.caption('This is a string that explains something above.')
    filertypeW = st.radio('Select a filer type', ('INDIVIDUAL', 'ENTITY'))
    filernameW = st.multiselect(options=list(filers[filers['Filer Persent Type'] == filertypeW]['Filer Name'].unique()), label='Select one or more filers by name')

    data = []

    # Display filertable
    for filername in filernameW:

        st.markdown(f'### {filername}')

        # Redefining dtypes
        contribs=['Contribution', 'Contributor', 'contribs']
        expend = ['Expenditure', 'Payee', 'expend']
        loans = ['Loan', 'Lender', 'loans']
        dtypes = [contribs, expend, loans]

        # Filter data
        ### *************************ADD LOAD WIDGET
        for dtype in dtypes:
            filtered_data = filter_data(filername, dtype)
            dtype.append(filtered_data) ## dtype format now [var, prefix, varshort, filtered_data]
                
        # Display stats for each dtype
        for dtype in dtypes:
            if len(dtype[3]) > 0: # filtered_data is dataframe
                data.append([dtype[0], dtype[3]])
                display_stats(dtype)

        # Display filertable
        filertable = filers[filers['Filer Name'] == filername].reset_index(drop=True).dropna(how='all', axis=1)
        display_filertable(filertable)

        # Display data
        for dtype in dtypes:
            display_data(dtype, filername)
    
    return filernameW, data

#____________________________________________________________ COMPARE FILERS ____________________________________________________________

# Defining dtypes
contribs=['Contribution', 'Contributor', 'contribs']
expend = ['Expenditure', 'Payee', 'expend']
loans = ['Loan', 'Lender', 'loans']
dtypes = [contribs, expend, loans]


def compare_filers(filernameW, data):
    if len(filernameW) > 1:

        st.markdown("""---""")
        st.markdown('<p style="background-color:  #ff4c4c; text-align: center; color:white; font-size: 20px; width:100%;"><b>FILER COMPARISON</b></p>', unsafe_allow_html=True)

        for dtype in dtypes:

            dfs = [el[1] for el in data if el[0] == dtype[0]]
            

            if len(dfs) > 0:

                concat_dfs = pd.concat(dfs)

                if len(concat_dfs) >= 10 and concat_dfs['Filer Name'].nunique() > 1:

                # Making main vars
                    names = [name.split(',')[0] for name in list(concat_dfs['Filer Name'].unique())]

                    # Get years
                    years_options =['All']
                    years_options.extend(sorted(concat_dfs.year.unique()))

                    with st.expander(f'Compare {dtype[0]}s'):
                        # Display chart
                        display_chart(concat_dfs, dtype)

                        # Display common
                        common = get_common(concat_dfs, 'All', dtype)
                        if len(common) > 0:
                            year = st.selectbox(f'Select a year to view common {dtype[0].lower()}s', years_options)
                            common = get_common(concat_dfs, year, dtype)
                            display_common(common, dtype, names)
                else:
                    st.write(f'Insufficient data to compare {dtype[0].lower()}s')
            else:
                st.write(f'Insufficient data to compare {dtype[0].lower()}s')



def main():

    filernameW, els = get_filer_data()
    compare_filers(filernameW, els)
    st.markdown(footer,unsafe_allow_html=True)


main()
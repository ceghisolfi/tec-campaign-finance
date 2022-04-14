import pandas as pd
import numpy as np
import re
import os
from zipfile import ZipFile
from csv import reader
import altair as alt
import streamlit as st



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

# with open('https://data-statesman.s3.amazonaws.com/tec-campaign-finance/documentation/last_update.txt') as f:
#     last_update = f.read()

last_update = 'Apr 14, 2022'

st.markdown(f'##### Last update: {last_update}')
st.markdown("""
This application processes and visualizes the **last five years** of campaign finance data released by the Texas Ethics Commission. 
The raw data is available for download [here](https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt). According to your selection of filers, 
the application will display contribution, expenditure and loan data for each filer
and then compare the filers you selected and their contributors, payees and lenders.

To begin, select one or more filers by **TYPE** and **NAME**. 

""")

#------------ FUNCTIONS ----------------

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


def filter_data(filername, var, prefix, var_short):
    filer_initials = filername[:3]
    data = pd.read_csv(f'https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/{var_short}_{filer_initials}.csv', low_memory=False, parse_dates=[f'{var.lower()}_dt', 'received_dt'])
    data.columns = data.columns.str.replace('_', ' ').str.title().str.replace('Expend', 'Expenditure')
    data[[f'{prefix} Street City', f'{prefix} Street State', f'{prefix} Street Postal Code', f'{prefix} Street Country']] = \
        data[[f'{prefix} Street City', f'{prefix} Street State', f'{prefix} Street Postal Code', f'{prefix} Street Country']].fillna('').astype(str)
    data[f'{prefix} Location'] = data[f'{prefix} Street City'] + ', ' + data[f'{prefix} Street State'] + ' ' + \
        data[f'{prefix} Street Postal Code'] + ', ' + data[f'{prefix} Street Country']
    filtered_data = data[data['Filer Name'].str.lower() == filername.lower()]
    filtered_data['year'] = filtered_data[f'{var} Dt'].dt.year
    return filtered_data



def get_stats(monthly, filtered_data, this_year, var, prefix):

    avg_amount_this_year = round(monthly[monthly.year == this_year]['amount'].mean())
    avg_amount_last_year = round(monthly[monthly.year == this_year - 1]['amount'].mean())
    avg_amount_diff = round(avg_amount_this_year - avg_amount_last_year)

    avg_count_this_year = round(monthly[monthly.year == this_year]['count'].mean())
    avg_count_last_year = round(monthly[monthly.year == this_year - 1]['count'].mean())
    avg_count_diff = round(avg_count_this_year - avg_count_last_year)

    top_contrib_this_year = filtered_data[filtered_data.year == this_year].sort_values(f'{var} Amount', ascending=False).iloc[0]
    top_contrib_this_year_name = top_contrib_this_year[f'{prefix} Name']
    top_contrib_this_year_info = 'Located at ' + str(top_contrib_this_year[f'{prefix} Street Postal Code']) + ' in ' + str(top_contrib_this_year[f'{prefix} Street City']) + ', ' + str(top_contrib_this_year[f'{prefix} Street State'].upper())
    if top_contrib_this_year[f'{prefix} Employer'] != '':
        top_contrib_this_year_info = 'Employed by ' + str(top_contrib_this_year[f'{prefix} Employer']) + ' in ' + str(top_contrib_this_year[f'{prefix} Street City']) + ', ' + str(top_contrib_this_year[f'{prefix} Street State'].upper())

    return avg_amount_this_year, avg_amount_diff, avg_count_this_year, avg_count_diff, top_contrib_this_year_name, top_contrib_this_year_info



def display_data(filtered_data, var, returndf, filername):
    # Display data table
    if len(filtered_data) > 0:
        date_min, date_max = filtered_data[f'{var} Dt'].min().strftime(format='%b %d, %Y'), filtered_data[f'{var} Dt'].max().strftime(format='%b %d, %Y')
        for col in [f'{var} Dt', 'Received Dt']:
            filtered_data[col] = pd.to_datetime(filtered_data[col]).apply(lambda x: x.strftime(format='%Y-%m-%d'))
        with st.expander(f'{var}s'):
            st.markdown(f'**Date range**: {date_min} - {date_max}')
            data_csv = convert_df(returndf.drop(columns='year'))
            st.download_button(
                label=f"Download {var.lower()} data",
                data=data_csv,
                file_name=f"data_{var.lower()}_{filername}.csv",
                mime='text/csv'
            )
            st.dataframe(filtered_data.fillna(''))

            return returndf
    else:
        st.write('No data to display')



def make_chart(concat_dfs, var):
    grouped = concat_dfs.groupby(['Filer Name', pd.Grouper(key=f'{var} Dt', freq='m')])[f'{var} Amount'].sum().reset_index()
    grouped['Filer Name'] = grouped['Filer Name'].apply(lambda x: re.sub("([\(\[]).*?([\)\]])", "", x))
    chart = alt.Chart(grouped).mark_line().encode(
                                    x=alt.X(f'{var} Dt', axis=alt.Axis(labelAngle=0), title=''), 
                                      y=alt.Y(f'{var} Amount', title='', ), 
                                      color=alt.Color('Filer Name', legend=alt.Legend(title='', labelFontSize=15)),
                                      tooltip=alt.Tooltip(f'{var} Amount',format=",.2f")
                                     ).properties(width=1000)
    return chart



def get_common(concat_dfs, year, var, prefix):

    if year != 'All':
        concat_dfs = concat_dfs[concat_dfs.year == year]
    grouped = concat_dfs.fillna('').groupby([col for col in concat_dfs.columns if 'contributor' in col.lower()]).agg(
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



def process_data(filername, var, prefix, var_short): # Contribution, Contributor

    filers = load_filers()
    
    if filername:
        
        # Filter and format data
        filtered_data = filter_data(filername, var, prefix, var_short)
        
        returndf = filtered_data.copy()
        this_year = filtered_data.year.max()
        monthly = filtered_data.groupby([pd.Grouper(key=f'{var} Dt', freq='M'), 'year'])\
            .agg(amount = (f'{var} Amount', 'sum'), count = (f'{var} Amount', 'count')).reset_index().fillna(0)

        st.markdown(f'### {filername}')

        if (this_year - 1) in list(monthly.year):

            avg_amount_this_year, avg_amount_diff, avg_count_this_year, avg_count_diff, top_contrib_this_year_name, top_contrib_this_year_info = \
                get_stats(monthly, filtered_data, this_year, var, prefix)

            col1, col2, col3 = st.columns(3)
            col1.metric(label=f"Avg Monthly {var} Amount ({this_year})", value='${:,}'.format(avg_amount_this_year), delta='${:,}'.format(avg_amount_diff))
            col2.metric(label=f"Avg Monthly {var} Count ({this_year})", value='{:,}'.format(avg_count_this_year), delta='{:,}'.format(avg_count_diff))
            col3.metric(label=f'Top {prefix} ({this_year})', value=top_contrib_this_year_name, delta=top_contrib_this_year_info, delta_color='off')
            

        filtered_data = filtered_data[[col for col in filtered_data.columns if col not in list(filers.columns)]]\
            .sort_values(f'{var} Dt', ascending=False).reset_index(drop=True)
        filtered_data.drop(columns=['year', f'{prefix} Street City', f'{prefix} Street State', f'{prefix} Street Postal Code', f'{prefix} Street Country'], inplace=True)

        # Display filer table
        filertable = filers[filers['Filer Name'] == filername].reset_index(drop=True)
        filertable.dropna(how='all', axis=1, inplace=True)
        with st.expander('Filer Information'):
            st.dataframe(filertable.fillna(''))

        # Display data table
        returndf = display_data(filtered_data, var, returndf, filername)

        return returndf



def compare_filers(dfs, var, prefix):
    if len(dfs) > 1:
        concat_dfs = pd.concat(dfs)
        names = [name.split(',')[0] for name in list(concat_dfs['Filer Name'].unique())]
        compare_title = ' vs. '.join(names)
        st.markdown("""---""")
        st.markdown('<p style="color:#ff4c4c; font-size: 20px;"><b>FILER COMPARISON</b></p>', unsafe_allow_html=True)
        st.markdown(f'**{compare_title}**')
        with st.expander(f'Compare {var}s'):
            st.markdown(f'**{var} Monthly Totals ($) Chart**')
            st.altair_chart(make_chart(concat_dfs, var), use_container_width=True)
            st.markdown(f'**Comm {prefix}s**')
            years =['All']
            years.extend(sorted(concat_dfs.year.unique()))
            year = st.selectbox('Select a year to view common contributors', years)
            common = get_common(concat_dfs, year, var, prefix)
            if len(common) > 0:
                common_csv = convert_df(common)
                names_forfile = '_'.join(names)
                st.download_button(
                    label=f"Download {var.lower()} data",
                    data=common_csv,
                    file_name=f"common_{var.lower()}_{names_forfile}.csv",
                    mime='text/csv'
                )
                st.dataframe(common)
            else:
                st.write('No data to display')



vardicts = [
    {'var':'Contribution', 'prefix': 'Contributor', 'var_short': 'contribs'},
    {'var': 'Expenditure', 'prefix': 'Payee', 'var_short': 'expend'},
    {'var': 'Loan', 'prefix': 'Lender', 'var_short': 'loan'}
    ]


with st.container():

    filers = load_filers()

    filertypeW = st.radio('Filer Type', ('INDIVIDUAL', 'ENTITY'))
    filernameW = st.multiselect(options=list(filers[filers['Filer Persent Type'] == filertypeW]['Filer Name'].unique()), label='Filer Name')


    dfs = []
    for filername in filernameW:   
        for d in vardicts:
            df = process_data(filername, d['var'], d['prefix'], d['var_short'])
            dfs.append(df)

    for d in vardicts:
        compare_filers(dfs, d['var'], d['prefix'])


    st.markdown(footer,unsafe_allow_html=True)

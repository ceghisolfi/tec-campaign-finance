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
st.markdown("""
This application processes and visualizes the **last five years** of campaign finance data released by the Texas Ethics Commission. 
The raw data is available for download [here](https://www.ethics.state.tx.us/data/search/cf/CFS-ReadMe.txt). Based on your selection of filers, 
the application will display contribution, expenditure and loan data for each filer
and then compare the filers you selected and their contributors, payees and lenders.



To begin, select one or more filers by **TYPE** and **NAME**. 

""")


@st.cache
def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')


#------------ FUNCTIONS ----------------

@st.cache
def load_filers():
    filers = pd.read_csv('https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/filers.csv', dtype={'filer_ident': str})
    filers.columns = [re.sub( '(?<!^)(?=[A-Z])', ' ', col.replace('Cd', '')).title() for col in filers.columns]
    filers = filers[(filers['Filer Name'].str.lower().str.contains('use, do not|not to be use|do not') == False)]
    filers['Filer Filerpers Status'] = filers['Filer Filerpers Status'].str.replace('_', ' ')

    return filers


def display_data(filername):

    filers = load_filers()
    
    if filername:
        
        # Filter and format data
        filer_initials = filername[:3]
        contribs = pd.read_csv(f'https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/contribs_{filer_initials}.csv', low_memory=False, parse_dates=['contribution_dt', 'received_dt'])
        contribs.columns = contribs.columns.str.replace('_', ' ').str.title()
        contribs[['Contributor Street City', 'Contributor Street State', 'Contributor Street Postal Code', 'Contributor Street Country']] = \
            contribs[['Contributor Street City', 'Contributor Street State', 'Contributor Street Postal Code', 'Contributor Street Country']].fillna('').astype(str)
        contribs['Contributor Location'] = contribs['Contributor Street City'] + ', ' + contribs['Contributor Street State'] + ' ' + \
            contribs['Contributor Street Postal Code'] + ', ' + contribs['Contributor Street Country']
        filtered_contribs = contribs[contribs['Filer Name'].str.lower() == filername.lower()]
        filtered_contribs['year'] = filtered_contribs['Contribution Dt'].dt.year
        returndf = filtered_contribs.copy()
        this_year = filtered_contribs.year.max()
        monthly = filtered_contribs.groupby([pd.Grouper(key='Contribution Dt', freq='M'), 'year'])\
            .agg(amount = ('Contribution Amount', 'sum'), count = ('Contribution Amount', 'count')).reset_index().fillna(0)

        st.markdown(f'### {filername}')

        if (this_year - 1) in list(monthly.year):

            avg_amount_this_year = round(monthly[monthly.year == this_year]['amount'].mean())
            avg_amount_last_year = round(monthly[monthly.year == this_year - 1]['amount'].mean())
            avg_amount_diff = round(avg_amount_this_year - avg_amount_last_year)

            avg_count_this_year = round(monthly[monthly.year == this_year]['count'].mean())
            avg_count_last_year = round(monthly[monthly.year == this_year - 1]['count'].mean())
            avg_count_diff = round(avg_count_this_year - avg_count_last_year)

            top_contrib_this_year = filtered_contribs[filtered_contribs.year == this_year].sort_values('Contribution Amount', ascending=False).iloc[0]
            top_contrib_this_year_name = top_contrib_this_year['Contributor Name']
            top_contrib_this_year_info = 'Located at ' + str(top_contrib_this_year['Contributor Street Postal Code']) + ' in ' + str(top_contrib_this_year['Contributor Street City']) + ', ' + str(top_contrib_this_year['Contributor Street State'].upper())
            if top_contrib_this_year['Contributor Employer'] != '':
                top_contrib_this_year_info = 'Employed by ' + str(top_contrib_this_year['Contributor Employer']) + ' in ' + str(top_contrib_this_year['Contributor Street City']) + ', ' + str(top_contrib_this_year['Contributor Street State'].upper())

            col1, col2, col3 = st.columns(3)
            col1.metric(label=f"Avg Monthly Contribution Amount ({this_year})", value='${:,}'.format(avg_amount_this_year), delta='${:,}'.format(avg_amount_diff))
            col2.metric(label=f"Avg Monthly Contribution Count ({this_year})", value='{:,}'.format(avg_count_this_year), delta='{:,}'.format(avg_count_diff))
            col3.metric(label=f'Top Contributor ({this_year})', value=top_contrib_this_year_name, delta=top_contrib_this_year_info, delta_color='off')
            

        filtered_contribs = filtered_contribs[[col for col in filtered_contribs.columns if col not in list(filers.columns)]]\
            .sort_values('Contribution Dt', ascending=False).reset_index(drop=True)
        filtered_contribs.drop(columns=['year', 'Contributor Street City', 'Contributor Street State', 'Contributor Street Postal Code', 'Contributor Street Country'], inplace=True)

        # Display filer table
        filertable = filers[filers['Filer Name'] == filername].reset_index(drop=True)
        filertable.dropna(how='all', axis=1, inplace=True)
        with st.expander('Filer Information'):
            st.dataframe(filertable.fillna(''))

        # Display data table
        if len(filtered_contribs) > 0:
            date_min, date_max = filtered_contribs['Contribution Dt'].min().strftime(format='%b %d, %Y'), filtered_contribs['Contribution Dt'].max().strftime(format='%b %d, %Y')
            for col in ['Contribution Dt', 'Received Dt']:
                filtered_contribs[col] = pd.to_datetime(filtered_contribs[col]).apply(lambda x: x.strftime(format='%Y-%m-%d'))
            with st.expander('Contributions'):
                st.markdown(f'**Date range**: {date_min} - {date_max}')
                contribs_csv = convert_df(returndf.drop(columns='year'))
                st.download_button(
                    label="Download contribution data",
                    data=contribs_csv,
                    file_name=f"contribs_{filername}.csv",
                    mime='text/csv'
                )
                st.dataframe(filtered_contribs.fillna(''))

                return returndf
        else:
            st.write('No data to display')



def make_chart(concat_dfs):
    grouped = concat_dfs.groupby(['Filer Name', pd.Grouper(key='Contribution Dt', freq='m')])['Contribution Amount'].sum().reset_index()
    grouped['Filer Name'] = grouped['Filer Name'].apply(lambda x: re.sub("([\(\[]).*?([\)\]])", "", x))
    chart = alt.Chart(grouped).mark_line().encode(
                                    x=alt.X('Contribution Dt', axis=alt.Axis(labelAngle=0), title=''), 
                                      y=alt.Y('Contribution Amount', title='', ), 
                                      color=alt.Color('Filer Name', legend=alt.Legend(title='', labelFontSize=15)),
                                      tooltip=alt.Tooltip('Contribution Amount',format=",.2f")
                                     ).properties(width=1000)
    return chart


def get_common(concat_dfs, year):

    if year != 'All':
        concat_dfs = concat_dfs[concat_dfs.year == year]
    grouped = concat_dfs.fillna('').groupby([col for col in concat_dfs.columns if 'contributor' in col.lower()]).agg(
        contrib_amount = ('Contribution Amount', 'sum'), 
        filers_count = ('Filer Name', 'nunique'), 
        Filers = ('Filer Name', lambda x: ', '.join(x.unique()))).reset_index()

    common = grouped[grouped.filers_count > 1]\
    .rename(columns={'contrib_amount': 'Contributions Total'})\
    [['Contributor Name', 'Contributor Persent Type', 'Contributor Location', 'Contributions Total', 'Filers']].reset_index(drop=True)

    for col in common.columns:
        if list(common[col].unique()) == ['']:
            common.drop(columns=[col], inplace=True)

    return common


def compare_filers(dfs):
    if len(dfs) > 1:
        concat_dfs = pd.concat(dfs)
        names = ' vs. '.join([name.split(',')[0] for name in list(concat_dfs['Filer Name'].unique())])
        st.markdown("""---""")
        st.markdown('## Filer Comparison')
        st.markdown(f'### {names}')
        with st.expander('Contribution Monthly Totals ($) Chart'):
            st.altair_chart(make_chart(concat_dfs), use_container_width=True)

        with st.expander('Common Contributors'):
            years =['All']
            years.extend(sorted(concat_dfs.year.unique()))
            year = st.selectbox('Select a year to view common contributors', years)
            common = get_common(concat_dfs, year)
            if len(common) > 0:
                st.dataframe(common)
            else:
                st.write('No data to display')



with st.container():

    filers = load_filers()

    filertypeW = st.radio('Filer Type', ('INDIVIDUAL', 'ENTITY'))
    filernameW = st.multiselect(options=list(filers[filers['Filer Persent Type'] == filertypeW]['Filer Name'].unique()), label='Filer Name')


    dfs = []
    for filername in filernameW:    
        df = display_data(filername)
        dfs.append(df)

    compare_filers(dfs)


    st.markdown(footer,unsafe_allow_html=True)

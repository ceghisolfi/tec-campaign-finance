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



# Defining dtypes
contribs=['Contribution', 'Contributor', 'contribs']
expend = ['Expenditure', 'Payee', 'expend']
loans = ['Loan', 'Lender', 'loans']
dtypes = [contribs, expend, loans]



def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')



def display_download_button(data, label, file_name):
    data_csv = convert_df(data)
    st.download_button(
        label=label,
        data=data_csv,
        file_name=file_name,
        mime='text/csv'
    )



@st.cache
def load_filers():
    filers = pd.read_csv('https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/filers.csv', dtype={'filerIdent': str})
    filers.columns = [re.sub( '(?<!^)(?=[A-Z])', ' ', col.replace('Cd', '')).title() for col in filers.columns]
    filers['Filer Filerpers Status'] = filers['Filer Filerpers Status'].str.replace('_', ' ')

    return filers



def filter_balance(ids):
    balance = pd.concat([pd.read_csv(f'https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/balance/balance_{id}.csv', dtype={'filer_ident': str}, parse_dates=['received_dt']) for id in ids])
    balance.columns = [col.replace('_', ' ').title() for col in balance.columns]

    return balance



def filter_data(ids, filername, dtype):

    # Load dtype vars
    var, prefix, var_short = dtype[0], dtype[1], dtype[2]

    # Filter data
    if var == 'Expenditure':
        vardt = 'expend'
    else:
        vardt = var
    try:
        data = pd.concat([
            pd.read_csv(f'https://data-statesman.s3.amazonaws.com/tec-campaign-finance/processed/{var_short}/{var_short}_{id}.csv', 
            low_memory=False, parse_dates=[f'{vardt.lower()}_dt', 'received_dt']) for id in ids
        ])
        data.columns = data.columns.str.replace('_', ' ').str.title().str.replace('Expend', 'Expenditure')
        filtered_data = data[data['Filer Name'].str.lower() == filername.lower()]
        filtered_data['year'] = filtered_data[f'{var} Dt'].dt.year
        filtered_data = filtered_data[[
            col for col in filtered_data.columns if ('Filer' not in col or col in ['Filer Ident', 'Filer Name']) and 'Office' not in col
            ]]
        return filtered_data
    except:
        return []



def display_filertable(filertable):
    if filertable['Filer Ident'].nunique() > 1:
        name = filertable['Filer Name'].iloc[0]
        st.warning(f'More than one filer ID found for {name}. Please consult the filer information box below and verify that all filer records listed match your search.')
    with st.expander('Filer Information'):
            st.dataframe(filertable.fillna(''))



def display_balance_stats(filtered_balance):
    if len(filtered_balance) > 0:
        # Display balance stats
        last_balance_amount = round(filtered_balance.iloc[0]['Balance'])
        prev_balance_amount = round(filtered_balance.iloc[1]['Balance'])
        balance_diff = round(last_balance_amount - prev_balance_amount)
        last_balance_date = filtered_balance.iloc[0]['Received Dt'].strftime('%b %d, %Y')
        
        # Display balance stats
        st.metric(label=f"Latest Balance (filed on {last_balance_date})", value='${:,}'.format(last_balance_amount), delta='{:,}'.format(balance_diff))



def display_balance_data(filername, filtered_balance):
    if len(filtered_balance) > 0:
        with st.expander(f'Balance'):
            st.info('Balance is calculated as: (Total Contributions + Total Unitemized Contributions + Total Contributions Mantained + Total Interest & Income Earned) \
            - (Total Expenditures + Total Unitemized Expenditures + Total Outstanding Loans + Total Unitemized Loans)')
            display_download_button(filtered_balance, f"Download balance data", f"balance_{filername}.csv")
            st.dataframe(filtered_balance)



def display_stats(dtype):

    # Load dtype vars
    var, prefix, filtered_data = dtype[0], dtype[1], dtype[3]

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
            top_contrib_this_year_info = 'Located in ' + str(top_contrib_this_year[f'{prefix} Location'])
            try:
                if top_contrib_this_year[f'{prefix} Employer'] != '':
                    top_contrib_this_year_info = 'Employed by ' + str(top_contrib_this_year[f'{prefix} Employer']) + ' in ' + str(top_contrib_this_year[f'{prefix} Location'])
            except:
                pass

            # Display stats
            col1, col2, col3 = st.columns(3)
            col1.metric(label=f"Avg Monthly {var} Amount ({this_year})", value='${:,}'.format(avg_amount_this_year), delta='{:,}'.format(avg_amount_diff))
            col2.metric(label=f"Avg Monthly {var} Count ({this_year})", value='{:,}'.format(avg_count_this_year), delta='{:,}'.format(avg_count_diff))
            col3.metric(label=f'Top {prefix} ({this_year})', value=top_contrib_this_year_name)
            # col3.markdown(top_contrib_this_year_info)
            col3.markdown(f'<p style="position: relative; top:-20px; color:grey">{top_contrib_this_year_info}</p>', unsafe_allow_html=True)



def display_data(dtype, filername):

    # Load dtype vars
    var, prefix, var_short, filtered_data = dtype[0], dtype[1], dtype[2], dtype[3]

    # Display data table
    if len(filtered_data) > 0:
        count = len(filtered_data)
        count_str = '{:,}'.format(count)
        date_min, date_max = filtered_data[f'{var} Dt'].min().strftime(format='%b %d, %Y'), filtered_data[f'{var} Dt'].max().strftime(format='%b %d, %Y')
        for col in [f'{var} Dt', 'Received Dt']:
            filtered_data[col] = pd.to_datetime(filtered_data[col]).apply(lambda x: x.strftime(format='%Y-%m-%d'))
        with st.expander(f'{var}s'):
            if date_min != date_max:
                st.markdown(f'**{count_str} {var.lower()}s between {date_min} and {date_max}**')
            elif count == 1:
                st.markdown(f'**{count_str} {var.lower()} on {date_min}**')
            else:
                st.markdown(f'**{count_str} {var.lower()}s on {date_min}**')
            display_download_button(filtered_data.drop(columns=['year']), f"Download {var.lower()} data", f"data_{var.lower()}_{filername}.csv")
            st.dataframe(filtered_data.fillna('').drop(columns=['year']))

    else:
        st.warning(f'No {var.lower()} data to display')



def get_common(concat_dfs, dtype):

    var, prefix = dtype[0], dtype[1]

    grouped = concat_dfs.fillna('').groupby([col for col in concat_dfs.columns if prefix.lower() in col.lower() or col == 'Filer Name' or col == 'year'])[f'{var} Amount'].sum().reset_index()

    name_cols = list(grouped['Filer Name'].unique())

    common = grouped.pivot(index=[col for col in concat_dfs.columns if prefix.lower() in col.lower() or col == 'year'], columns='Filer Name', values=f'{var} Amount').reset_index()
    common.rename(columns={'year': 'Year'}, inplace=True)

    for col in name_cols:
        common = common[common[col].isna() == False]

    for col in common.columns:
        if list(common[col].unique()) == ['']:
            common.drop(columns=[col], inplace=True)

    common.reset_index(drop=True, inplace=True)
    common.sort_values('Year', ascending=False, inplace=True)

    return common



def display_common(concat_dfs, dtype, names):

    var, prefix = dtype[0], dtype[1]

    st.markdown(f'**Shared {prefix}s**')
    common = get_common(concat_dfs, dtype)
    if len(common) > 0:
        names_forfile = '_'.join(names)
        display_download_button(common, f"Download shared {prefix.lower()}s data", f"shared_{prefix.lower()}s_{names_forfile}.csv")
        st.dataframe(common)
    else:
        st.write(f'No shared {prefix.lower()}s to display')



def make_line_chart(data):

    name_col = data.columns[0]
    date_col = data.columns[1]
    value_col = data.columns[2]
    data[value_col] = data[value_col].astype(float).round(1)
    data[name_col] = data[name_col].apply(lambda x: x.replace(' ', ' ').replace(',', '').replace('.', ' ')).apply(lambda x: re.sub("([\(\[]).*?([\)\]])", "", x))

    base = alt.Chart(data).encode(
        x=alt.X(date_col, axis=alt.Axis(format = ("%b %Y")))
        )

    names = sorted(data[name_col].unique())
    tooltips = [alt.Tooltip(c, type='quantitative', title=c, format="$,.2f") for c in names]
    tooltips.insert(0, alt.Tooltip(date_col, title=date_col))
    selection = alt.selection_single(
        fields=[date_col], nearest=True, on='mouseover', empty='none', clear='mouseout'
    )

    lines = base.mark_line(interpolate='catmull-rom').encode(
        y=alt.Y(value_col, title='', axis=alt.Axis(format="$~s")), 
        color=alt.Color(name_col, legend=alt.Legend()))
    points = lines.mark_point().transform_filter(selection)

    rule = base.transform_pivot(
        pivot=name_col, value=value_col, groupby=[date_col]
    ).mark_rule().encode(
        opacity=alt.condition(selection, alt.value(0.3), alt.value(0)),
        tooltip=tooltips
    ).add_selection(selection)

    chart = lines + points + rule
    
    return chart



def get_var_totals(concat_dfs, dtype):

    var = dtype[0]

    concat_dfs[f'{var} Dt'] = pd.to_datetime(concat_dfs[f'{var} Dt'])
    grouped = concat_dfs.groupby(['Filer Name', pd.Grouper(key=f'{var} Dt', freq='m')])[f'{var} Amount'].sum().reset_index()
    grouped[f'{var} Dt'] = pd.to_datetime(grouped[f'{var} Dt'])
    for_download = grouped.pivot(index=f'{var} Dt', columns='Filer Name', values=f'{var} Amount').reset_index()

    return grouped, for_download



def display_var_totals_chart(dtype, concat_dfs, names):

    var = dtype[0]
    names_forfile = '_'.join(names)
    chart_data, for_download = get_var_totals(concat_dfs, dtype)
    chart = make_line_chart(chart_data)

    st.markdown(f'**{var} Monthly Totals**')
    display_download_button(for_download, f"Download {var.lower()} monthly totals data", f"monthly_totals_{var.lower()}_{names_forfile}.csv")
    st.altair_chart(chart, use_container_width=True)


#____________________________________________________________ GET FILER DATA ____________________________________________________________


def get_filer_data():
    # Load filers
    filers = load_filers()

    # Display filters

    filertypeW = st.radio('Select a filer type', ('INDIVIDUAL', 'ENTITY'))
    if filertypeW == 'INDIVIDUAL':
        name_help = "Begin typing the filer's LAST NAME to view options"
    else:
        name_help = "Begin typing the entity's name to view options"
    filernameW = st.multiselect(options=list(filers[filers['Filer Persent Type'] == filertypeW]['Filer Name'].unique()), 
    label='Select one or more filers by name', help=name_help)

    if len(filernameW) == 1:
        st.info('Add another filer to generate comparison data.')

    data = []

    # Display filertable
    for filername in filernameW:
        try:
            ids = list(filers[filers['Filer Name'] == filername]['Filer Ident'].unique())

            # Redefining dtypes
            contribs=['Contribution', 'Contributor', 'contribs']
            expend = ['Expenditure', 'Payee', 'expend']
            loans = ['Loan', 'Lender', 'loans']
            dtypes = [contribs, expend, loans]

            # Filter data
            with st.spinner('Loading data...'):
                for dtype in dtypes:
                    filtered_data = filter_data(ids, filername, dtype)
                    dtype.append(filtered_data) ## dtype format now [var, prefix, varshort, filtered_data]

            
            # Balance data
            filtered_balance = filter_balance(ids)

            st.markdown(f'### {filername}')

            # Display balance stats
            display_balance_stats(filtered_balance)
                    
            # Display stats for each dtype
            for dtype in dtypes:
                if len(dtype[3]) > 0: # filtered_data is dataframe
                    data.append([dtype[0], dtype[3]])
                    display_stats(dtype)

            # Display filertable
            filertable = filers[filers['Filer Name'] == filername].reset_index(drop=True).dropna(how='all', axis=1)
            display_filertable(filertable)

            # Display balance data
            display_balance_data(filername, filtered_balance)

            # Display data
            for dtype in dtypes:
                display_data(dtype, filername)
        except:
            st.warning(f'No data found for {filername}')

    
    compare_filers(filernameW, data, filers)

#____________________________________________________________ COMPARE FILERS ____________________________________________________________


def compare_filers(filernameW, data, filers):
    if len(filernameW) > 1:

        st.markdown("""---""")
        st.markdown('<p style="background-color:  #ff4c4c; text-align: center; color:white; font-size: 20px; width:100%;"><b>FILER COMPARISON</b></p>', unsafe_allow_html=True)


        for dtype in dtypes:

            dfs = [el[1] for el in data if el[0] == dtype[0]]
            

            if len(dfs) > 0:

                concat_dfs = pd.concat(dfs)

                if len(concat_dfs) >= 10 and concat_dfs['Filer Name'].nunique() > 1:

                    # Making main vars
                    names = [re.sub("([\(\[]).*?([\)\]])", "", name.replace(',', '').replace('.', '').replace(' ', '-')).strip() for name in list(concat_dfs['Filer Name'].unique())]

                    with st.expander(f'Compare {dtype[0]}s'):
                        # Display chart
                        display_var_totals_chart(dtype, concat_dfs, names)

                        # Display common
                        display_common(concat_dfs, dtype, names)
                else:
                    st.warning(f'Insufficient data to compare {dtype[0].lower()}s')
            else:
                st.warning(f'Insufficient data to compare {dtype[0].lower()}s')


def main():
    st.image('https://upload.wikimedia.org/wikipedia/commons/d/d9/Austin_American-Statesman_%282019-10-31%29.svg', width=300)
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
    get_filer_data()
    st.markdown(footer,unsafe_allow_html=True)


if __name__ == '__main__':
    main()
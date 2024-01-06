from perekrestok_parser import PerekrestokScraper
from magnit_parser import MagnitScraper
import pandas as pd

import multiprocessing
def scrape_perekrestok():
    perekrestok_scraper = PerekrestokScraper()
    perekrestok_df = perekrestok_scraper.scrape_data()
    perekrestok_df.to_parquet('perekrestok.parquet')

def scrape_magnit():
    magnit_scraper = MagnitScraper()
    res_magnit = magnit_scraper.scrape_data()
    res_magnit.to_parquet('res_magnit.parquet')

#if __name__ == '__main__':
 #   perekrestok_process = multiprocessing.Process(target=scrape_perekrestok)
  #  magnit_process = multiprocessing.Process(target=scrape_magnit)

   # perekrestok_process.start()
    #magnit_process.start()

    #perekrestok_process.join()
    #magnit_process.join()
magnit_prod = pd.read_parquet('res_magnit.parquet')
magnit_cat = pd.read_parquet('magnit_categories.parquet')
perekrestok_prod = pd.read_parquet('perekrestok.parquet')

m1 = magnit_cat.iloc[:, :3].drop_duplicates()
m1.columns = ['cat_id', 'cat_name',  'cat_code']
m2 = magnit_cat.iloc[:, 3:6].drop_duplicates()
m2.columns = ['cat_id', 'cat_name',  'cat_code']
m3 = magnit_cat.iloc[:, 6:9].drop_duplicates()  
m3.columns = ['cat_id', 'cat_name',  'cat_code']
magnit_catgs = pd.concat([m1,m2,m3], ignore_index=True)
prod_magnit = pd.json_normalize(magnit_prod.explode('offers').to_dict(orient='records'))[['categories', 'code', 'grammar', 'id', 'isForAdults', 'name',
       'unitValue', 'offers.price',
       'offers.quantity']].explode('categories')
prod_magnit['offers.price'] = prod_magnit['offers.price'].str.replace(',','.').astype(float)
prod_cat_magnit = prod_magnit.merge(
    magnit_catgs[magnit_catgs.cat_name.notna()],
    how='left',
    left_on='categories',
    right_on='cat_id'
)
magnit_cat_avg = prod_cat_magnit.groupby('cat_name').agg({'offers.price':'mean'}).reset_index()
magnit_cat_avg.columns = ['category', 'avg_price']
perekrestok_prod['product_price'] = perekrestok_prod['product_price'].str.replace(',','.').str.split('₽').str[0].str.replace(' ','').astype(float)
perekrestok_cat_avg = perekrestok_prod.groupby('cat_title').agg({'product_price':'mean'}).reset_index()
perekrestok_cat_avg.columns = ['category', 'avg_price']
all_cats = perekrestok_cat_avg.merge(magnit_cat_avg, on='category', how='outer', suffixes=('_perekrestok', '_magnit'))
cats_mapped = all_cats[(all_cats.avg_price_perekrestok.isna()==False)&(all_cats.avg_price_magnit.isna()==False)]
perekrestok_unmapped = perekrestok_cat_avg[~perekrestok_cat_avg.category.isin(cats_mapped.category.values)]
magnit_unmapped = magnit_cat_avg[~magnit_cat_avg.category.isin(cats_mapped.category.values)]
from fuzzywuzzy import fuzz
#fuzz.partial_ratio('Автомасла', perekrestok_unmapped.category.values)

mapped_cats = []
for target_word in magnit_unmapped.category.values:
    target = 70

    best_match = None
    best_partial_ratio = 0
    best_matchs = {}
    for word in perekrestok_unmapped.category.values:
        #print(word)
        partial_ratio = fuzz.token_set_ratio(target_word, word)
        if partial_ratio > best_partial_ratio:
            #print(target_word, word,partial_ratio)
            best_partial_ratio = partial_ratio
            best_match = word
            #best_matchs[word] = best_partial_ratio

    if best_partial_ratio>=target:
        best_matchs['perekrestok'] = best_match
        best_matchs['magnit'] = target_word
    else:
        best_matchs['perekrestok'] = None
        best_matchs['magnit'] = target_word
    mapped_cats.append(best_matchs)

magnit_mapped = pd.DataFrame(mapped_cats).merge(magnit_unmapped, left_on='magnit', right_on='category', how='outer').rename({'avg_price':'avg_price_magnit'}, axis=1)
all_unmapped_mapped = magnit_mapped.merge(perekrestok_unmapped, left_on='perekrestok', right_on='category', how='outer').rename({'avg_price':'avg_price_perekrestok'}, axis=1)
#all_unmapped_mapped[(all_unmapped_mapped.avg_price_perekrestok.isna()==False)&(all_unmapped_mapped.avg_price_magnit.isna()==False)]

all_unmapped_mapped = all_unmapped_mapped[['magnit','avg_price_magnit','category_y','avg_price_perekrestok']].rename({'category_y':'perekrestok'}, axis=1)

cats_mapped['magnit'] = cats_mapped.category
cats_mapped['perekrestok'] = cats_mapped.category
cats_mapped = cats_mapped.drop('category', axis=1)

import numpy as np
all_mapped_categories = pd.concat([all_unmapped_mapped, cats_mapped])
all_mapped_categories['difference'] = np.where(all_mapped_categories['avg_price_magnit'].isna() | all_mapped_categories['avg_price_perekrestok'].isna(), 0, all_mapped_categories['avg_price_magnit'] - all_mapped_categories['avg_price_perekrestok'])
all_mapped_categories = all_mapped_categories.sort_values('difference', ascending=False).reset_index(drop=True)


import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import pandas as pd


df = all_mapped_categories

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(
        id='magnit-category-dropdown',
        options=[{'label': category, 'value': category} for category in df['magnit'].dropna().unique()],
        multi=True,
        placeholder='Select magnit categories'
    ),
    html.Br(),
    dcc.Dropdown(
        id='perekrestok-category-dropdown',
        options=[{'label': category, 'value': category} for category in df['perekrestok'].dropna().unique()],
        multi=True,
        placeholder='Select perekrestok categories'
    ),
    html.Br(),
    html.Div([
        dcc.Input(
            id='avg-price-magnit-filter',
            type='number',
            placeholder='Filter by avg price (magnit)',
            debounce=True
        ),
        dcc.Dropdown(
            id='avg-price-magnit-operator',
            options=[
                {'label': '=', 'value': 'eq'},
                {'label': '>', 'value': 'gt'},
                {'label': '<', 'value': 'lt'},
            ],
            value='eq',
            clearable=False,
        ),
    ]),
    html.Br(),
    html.Div([
        dcc.Input(
            id='avg-price-perekrestok-filter',
            type='number',
            placeholder='Filter by avg price (perekrestok)',
            debounce=True
        ),
        dcc.Dropdown(
            id='avg-price-perekrestok-operator',
            options=[
                {'label': '=', 'value': 'eq'},
                {'label': '>', 'value': 'gt'},
                {'label': '<', 'value': 'lt'},
            ],
            value='eq',
            clearable=False,
        ),
    ]),
    html.Br(),
    html.Div([
        dcc.Input(
            id='difference-filter',
            type='number',
            placeholder='Filter by difference',
            debounce=True
        ),
        dcc.Dropdown(
            id='difference-operator',
            options=[
                {'label': '=', 'value': 'eq'},
                {'label': '>', 'value': 'gt'},
                {'label': '<', 'value': 'lt'},
            ],
            value='eq',
            clearable=False,
        ),
    ]),
    html.Br(),
    html.Button('Scrape', id='scrape-button', n_clicks=0),  # Add a Scrape button
    html.Br(),
    html.Table(id='filtered-table'),
])


@app.callback(
    Output('filtered-table', 'children'),
    Input('magnit-category-dropdown', 'value'),
    Input('perekrestok-category-dropdown', 'value'),
    Input('avg-price-magnit-filter', 'value'),
    Input('avg-price-magnit-operator', 'value'),
    Input('avg-price-perekrestok-filter', 'value'),
    Input('avg-price-perekrestok-operator', 'value'),
    Input('difference-filter', 'value'),
    Input('difference-operator', 'value'),
    Input('scrape-button', 'n_clicks')  # Add a callback input for the Scrape button
)
def update_table(magnit_selected_categories, perekrestok_selected_categories, avg_price_magnit_filter,
                 avg_price_magnit_operator, avg_price_perekrestok_filter, avg_price_perekrestok_operator, difference_filter,
                 difference_operator, n_clicks):
    filtered_df = df.copy()

    if n_clicks > 0:  # Check if the Scrape button has been clicked
        perekrestok_process = multiprocessing.Process(target=scrape_perekrestok)
        magnit_process = multiprocessing.Process(target=scrape_magnit)

        perekrestok_process.start()
        magnit_process.start()

        perekrestok_process.join()
        magnit_process.join()

    if magnit_selected_categories:
        filtered_df = filtered_df[filtered_df['magnit'].isin(magnit_selected_categories)]
    if perekrestok_selected_categories:
        filtered_df = filtered_df[filtered_df['perekrestok'].isin(perekrestok_selected_categories)]
    if avg_price_magnit_filter is not None:
        if avg_price_magnit_operator == 'eq':
            filtered_df = filtered_df[filtered_df['avg_price_magnit'] == avg_price_magnit_filter]
        elif avg_price_magnit_operator == 'gt':
            filtered_df = filtered_df[filtered_df['avg_price_magnit'] > avg_price_magnit_filter]
        elif avg_price_magnit_operator == 'lt':
            filtered_df = filtered_df[filtered_df['avg_price_magnit'] < avg_price_magnit_filter]
    if avg_price_perekrestok_filter is not None:
        if avg_price_perekrestok_operator == 'eq':
            filtered_df = filtered_df[filtered_df['avg_price_perekrestok'] == avg_price_perekrestok_filter]
        elif avg_price_perekrestok_operator == 'gt':
            filtered_df = filtered_df[filtered_df['avg_price_perekrestok'] > avg_price_perekrestok_filter]
        elif avg_price_perekrestok_operator == 'lt':
            filtered_df = filtered_df[filtered_df['avg_price_perekrestok'] < avg_price_perekrestok_filter]
    if difference_filter is not None:
        if difference_operator == 'eq':
            filtered_df = filtered_df[filtered_df['difference'] == difference_filter]
        elif difference_operator == 'gt':
            filtered_df = filtered_df[filtered_df['difference'] > difference_filter]
        elif difference_operator == 'lt':
            filtered_df = filtered_df[filtered_df['difference'] < difference_filter]

    return [
        html.Tr([html.Th(col) for col in filtered_df.columns])] + [
        html.Tr([html.Td(filtered_df.iloc[i][col]) for col in filtered_df.columns]) for i in range(len(filtered_df))
    ]

if __name__ == '__main__':
    app.run_server(debug=True, port=1313)

#проблемы: не учитывается вес, размер, количество юнитов, нужно приводить к единому измерению (930мл превращать в 1000мл итд)


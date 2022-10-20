import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import json
import re
from ast import literal_eval

from github import Github
import requests
import os
from zipfile import ZipFile
from io import BytesIO

from dash import Dash, html, dcc, Input, Output, State, MATCH, ALL, callback_context
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import utils

#from memory_profiler import profile

local = True

#####################################################################################################################################
# Create Dash-App
    
mpya_purple = "rgb(155, 24, 137)"
mpya_light_blue = "rgb(82, 198, 226)"
mpya_yellow = "rgb(254, 203, 0)"
mpya_grey = "rgb(224, 225, 221)"
mpya_dark_blue = "rgb(0, 45, 89)"
mpya_font = "Helvetica Neue", "Helvetica", "Arial", "Lucida Grande", "sans-serif"

mpya_style_background = {'font-family': mpya_font, 'background-color': mpya_dark_blue, 'color': mpya_grey}
mpya_style_cards = {'font-family': mpya_font, 'background-color': utils.rgb_adder(mpya_dark_blue, 0.1), 'color': mpya_grey}
mpya_style_act_tab = {'font-family': mpya_font, 'background': utils.rgb_adder(mpya_dark_blue, 0.5), 'color': mpya_grey}
mpya_style_pas_tab = {'font-family': mpya_font, 'background': mpya_grey, 'color': mpya_dark_blue}

#utils.pull_repo()
    
#profiles, company_info_map, ignore_competences, ignore_titles, cities = utils.get_gh_resources()
profiles, company_info_map, ignore_competences, ignore_titles, cities = utils.get_local_resources()
dfs = {}
#dfs['Current'] = utils.get_gh_dataframe(profiles["Yellow Submarine"]["Data Science"], cities.keys())
dfs['Current'] = utils.get_local_dataframe(profiles["Yellow Submarine"]["Data Science"], cities.keys())

teams = list(profiles.keys())

app = Dash(__name__, external_stylesheets=[dbc.themes.SLATE])
server = app.server
app.title = "MPYA-Dashboard"
app._favicon = ('MPYASci&Tech_logo_original.png')

@app.callback([
        Output(component_id='example-graph', component_property='figure'),
        Output(component_id='ratio-graph', component_property='figure'),
        Output('checklist-input-1', 'options'),
        Output('checklist-input-1', 'value'),
        Output('checklist-input-2', 'options'),
        Output('checklist-input-2', 'value'),
        Output('checklist-input-3', 'options'),
        Output('checklist-input-3', 'value'),
        Output('location-dropdown', 'options'),],
        [
        Input('team-dropdown', 'value'),
        Input('kategori-dropdown', 'value'),
        Input('location-dropdown', 'value'),
        Input('group-by-value', 'value'),]
)
def update_ads(team, cat, loc, g_b_v):
    if loc == []:
        return {}, {}, [], [], [], [], [], [], dfs['Current'].locs
    if dfs['Current'].name != cat:
        #dfs['Current'] = utils.get_gh_dataframe(profiles[team][cat], cities)
        dfs['Current'] = None
        dfs['Current'] = utils.get_local_dataframe(profiles[team][cat], cities)
        if dfs['Current'].empty:
            return {}, {}, [], [], [], [], [], [], dfs['Current'].locs
    cat_df = dfs['Current']
    if type(loc) != list:
        loc = [loc]
    loc_df = cat_df[(cat_df.location.isin(loc))]
    g_b_val = list(np.unique([g_b_v, "year"]))
    trends = loc_df.groupby(g_b_val).agg(companies=('company', 'unique'), 
                                     nr_of_ads=('location', 'size')).reset_index()
    trends['nr_of_companies'] = trends.companies.apply(lambda x: len(x))
    trends['ratio'] = trends.nr_of_ads / trends.nr_of_companies
    fig1 = make_subplots(rows=1, cols=2, shared_yaxes=False, subplot_titles=['Annonser', 'Företag'])
    fig1.update_layout({'font_family': mpya_font[-1], 'font_color': mpya_grey, 'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)'})
    col = mpya_purple
    for year in np.sort(trends.year.unique()):
        sub = trends[trends.year == year]
        fig1.add_trace(go.Bar(x=sub[g_b_v], y=sub.nr_of_companies, marker=dict(color=col), showlegend=False, name=str(year)), 1, 2)
        fig1.add_trace(go.Bar(x=sub[g_b_v], y=sub.nr_of_ads, marker=dict(color=col), showlegend=False, name=str(year)), 1, 1)
    fig1.update_xaxes(dtick=1, title_text = utils.first_to_upper(g_b_v))
    trends['year_'] = trends['year'].astype(str)
    cdm={y: mpya_purple for y in trends['year_'].unique()}
    fig2 = px.bar(trends, x=g_b_v, y="ratio", color="year_", color_discrete_map=cdm, barmode="group", labels={
                    g_b_v: utils.first_to_upper(g_b_v),
                     "year_": "Year",
                    "ratio": "Ratio"
                }, title="Ratio (annonser/företag)")
    fig2.update_xaxes(showgrid=False)
    fig2.update_layout({'font_family': mpya_font[-1], 'font_color': mpya_grey, 'paper_bgcolor': 'rgba(0,0,0,0)', 'plot_bgcolor': 'rgba(0,0,0,0)'})
    fig2.update_xaxes(dtick=1, title_text=utils.first_to_upper(g_b_v))
    fig2.update_layout(showlegend=False)
    options = [{"label": str(y), "value": y} for y in reversed(dfs['Current'].years)]
    close = [c for c in cities if (cities[c] == cities[loc[0]]) and (c != loc[0])]
    locs =  np.array([l for l in cat_df.locs if l in close] + [l for l in cat_df.locs if l not in close])
    return fig1, fig2, options, [cat_df.years[-1]], options, [cat_df.years[-1]], options, [cat_df.years[-1]], locs

@app.callback(
    [Output('kategori-dropdown', 'options'),
     Output('kategori-dropdown', 'value')],
    [Input('team-dropdown', 'value'),]
)
def update_cat_dropdown(team):
    return list(profiles[team].keys()), list(profiles[team].keys())[0]

@app.callback(
    Output('profile-output', 'children'),
    [Input('team-dropdown', 'value'),
    Input('kategori-dropdown', 'value')],
)
def update_profile_output(team, cat):
    kws = profiles[team][cat]['keywords']
    
    return html.H5('Keywords för '+cat+'-profilen:'), html.H6(', '.join([utils.first_to_upper(kw) for kw in kws]))

@app.callback(
    Output('card-body-2', 'children'),
    [
        Input('location-dropdown', 'value'),
        Input('checklist-input-2', 'value'),
    ]
)
def update_card2(loc, checked):
    if checked != []:
        cat_df = dfs['Current']
        if type(loc) != list:
            loc = [loc]
        trends = cat_df[(cat_df.location.isin(loc)) & (cat_df.year.isin(checked))]
        if trends.empty:
            return
        top_ten = pd.Series(np.hstack(trends.Jobbtitlar.values))[~pd.Series(np.hstack(trends.Jobbtitlar.values)).isin(ignore_titles)].value_counts()[:10]
        return dbc.Table.from_dataframe(top_ten.to_frame().reset_index().set_axis(['Jobbtitel', 'Antal'], axis=1, inplace=False), striped=False)

@app.callback(
    Output('card-body-1', 'children'),
    [
        Input('location-dropdown', 'value'),
        Input('checklist-input-1', 'value'),
    ]
)
def update_card1(loc, checked):
    if checked != []:
        cat_df = dfs['Current']
        if type(loc) != list:
            loc = [loc]
        trends = cat_df[(cat_df.location.isin(loc)) & (cat_df.year.isin(checked))]
        if trends.empty:
            return
        top_ten = pd.Series(np.hstack(trends.Kompetenser.values))[~pd.Series(np.hstack(trends.Kompetenser.values)).isin(ignore_competences)].value_counts()[:10]
        return dbc.Table.from_dataframe(top_ten.to_frame().reset_index().set_axis(['Kompetens', 'Antal'], axis=1, inplace=False), striped=False)
    
@app.callback(
    Output('card-body-3', 'children'),
    [
        Input('location-dropdown', 'value'),
        Input('checklist-input-3', 'value'),
    ]
)
def update_card3(loc, checked):
    if checked != []:
        cat_df = dfs['Current']
        if type(loc) != list:
            loc = [loc]
        trends = cat_df[(cat_df.location.isin(loc)) & (cat_df.year.isin(checked)) & (cat_df.company != 'Nan')]
        if trends.empty:
            return
        table_df = trends.company.value_counts().reset_index().set_axis(['Företag', 'Antal'], axis=1, inplace=False)
        #Setup table
        table = dbc.Table(
                    # Header
                    [html.Thead([html.Tr([html.Th(col) for col in table_df.columns]+[html.Th("Expandera")])])] +

                    # Body
            #create_title_button(i, table_df.iloc[i], trends)
                    [html.Tbody([html.Tr([html.Td(html.A(table_df.iloc[i][col], id = ""+col+"_"+str(i)))
                        for col in table_df.columns]+[html.Div(html.Button('+', id={"type": "open-modal", "index": table_df.iloc[i]["Företag"]}, n_clicks=0))])
                        for i in range(len(table_df))])]
                    ,
                    hover = True,
                    striped=False,
                    id="table3"
                )
        return [table]+[dbc.Tooltip(html.A([html.Small(html.P(company_info_map[table_df['Företag'].values[i]][0])), html.Small(html.Small(html.P(company_info_map[table_df['Företag'].values[i]][1])))], href=company_info_map[table_df['Företag'].values[i]][2], target="_blank"), autohide=False, target="Företag_"+str(i)) for i in range(len(table_df))]

@app.callback(
    [Output("title-modal", "is_open"),
     Output("title-list", "children"),
     Output("header-modal", "children")],
    Input({'type': 'open-modal', 'index': ALL}, "n_clicks"),
    [State("title-modal", "is_open"),
     State('location-dropdown', 'value'),
     State('checklist-input-3', 'value')],
)
def toggle_modal(n1, is_open, loc, checked):
    cat_df = dfs['Current']
    if type(loc) != list:
        loc = [loc]
    trends = cat_df[(cat_df.location.isin(loc)) & (cat_df.year.isin(checked)) & (cat_df.company != 'Nan')]
    if (len(callback_context.triggered_prop_ids.keys()) == 1) and (sum(n1)):
        comp = callback_context.triggered_prop_ids[list(callback_context.triggered_prop_ids.keys())[0]]['index']
        table = dbc.Table.from_dataframe(utils.get_titles_from_comp(trends, comp).to_frame().reset_index().set_axis(['Titel', 'Antal'], axis=1))
        return not is_open, table, comp
    return is_open, "", ""

card_1 = dbc.Card(
    [
        dbc.Checklist(
            options=[{"label": str(y), "value": y} for y in reversed(dfs['Current'].years)],
            value=[dfs['Current'].years[-1]],
            id="checklist-input-1",
            inline=True,
        ),
        dcc.Loading(dbc.CardBody(id='card-body-1')),
    ],
    id='card-1', style=mpya_style_cards
)
card_2 = dbc.Card(
    [
        dbc.Checklist(
            options=[{"label": str(y), "value": y} for y in reversed(dfs['Current'].years)],
            value=[dfs['Current'].years[-1]],
            id="checklist-input-2",
            inline=True,
        ),
        dcc.Loading(dbc.CardBody( id='card-body-2')),
    ],
    id='card-2', style=mpya_style_cards
)
card_3 = dbc.Card(
    [
        dbc.Checklist(
            options=[{"label": str(y), "value": y} for y in reversed(dfs['Current'].years)],
            value=[dfs['Current'].years[-1]],
            id="checklist-input-3",
            inline=True,
        ),
        dcc.Loading(dbc.CardBody(id='card-body-3')),
    ],
    id='card-3', style=mpya_style_cards
)

graph_1 = dbc.Card(
    dbc.CardBody(
        [
            dbc.RadioItems(
            options=[{"label": utils.first_to_upper(y), "value": y} for y in ["year", "quarter", "month"]],
            value="year",
            id="group-by-value",
            inline=True,
        ),
            dcc.Loading(dcc.Graph(
            id='example-graph'
            ))
        ]
        ), style=mpya_style_cards
)

graph_2 = dbc.Card(
    dbc.CardBody(
        [
            dcc.Loading(dcc.Graph(
            id='ratio-graph'
            ))
        ], style=mpya_style_cards
        )
)

tab1_content = dbc.Card(dbc.CardBody([card_1, card_2], className="mt-3", style=mpya_style_cards), style=mpya_style_cards)

tab2_content = dbc.Card(dbc.CardBody([card_3] ,className="mt-3", style=mpya_style_cards), style=mpya_style_cards)

tabs = dbc.Tabs(
    [
        dbc.Tab(tab2_content, label="Företag", label_style=mpya_style_pas_tab, active_label_style={'font-weight': 'bold'}),
        dbc.Tab(tab1_content, label="Titlar/Kompetenser", label_style=mpya_style_pas_tab, active_label_style={'font-weight': 'bold'}),
    ]
)

modal = dbc.Modal([
    dbc.ModalHeader(dbc.ModalTitle(id='header-modal')),
    html.Div("Placeholder", id='title-list'),
], id="title-modal", size="lg",is_open=False)

app.layout = html.Div(children=[    
    html.Div([
        dbc.Row([
            dbc.Col([html.Img(src=r'assets/MPYASci&Tech_logo_original.png', alt='image', style={'width': '25%', 'margin': '1%'})], width=11),
            #dbc.Col([edit], width=1),
        ], style={'margin-left': '1%', 'margin-right': '1%'}),
        dbc.Row([
            dbc.Col([html.H3(children='Team:'), dcc.Dropdown(teams, "Yellow Submarine", id='team-dropdown', clearable=False, style=mpya_style_pas_tab)], width=4),
            dbc.Col([html.H3(children='Profil:'), dcc.Dropdown(list(profiles["Yellow Submarine"].keys()), "Data Science", id='kategori-dropdown', clearable=False, style=mpya_style_pas_tab), html.Div(id='profile-output')], width=4),
            dbc.Col([html.H3(children='Ort:'), dcc.Dropdown(dfs['Current'].locs, "Göteborg", id='location-dropdown', clearable=False, style=mpya_style_pas_tab, multi=True)], width=4),
        ], style={'margin': '1%'}),
        dbc.Row([
        dbc.Col([tabs], width=4), dbc.Col([graph_1, graph_2], width=8)
        ], style={'margin-left': '1%', 'margin-right': '1%'}),
        modal,
    ])
], style=mpya_style_background)


#####################################################################################################################################
# Run Dash-App
if local:
    debug = True
    app.run_server(debug=debug)
    app.suppress_callback_exceptions=True
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 29 16:33:12 2020

@author: wanxinye
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import dash_daq as daq

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df_ebal = pd.read_csv('ebal.csv')
df_unfcc = pd.read_csv('unfcc.csv')

e_country = df_ebal['REF_AREA'].unique()
e_commodity = df_ebal['COMMODITY'].unique()
e_transaction = df_ebal['TRANSACTION'].unique()

app.layout = html.Div(style={'backgroundColor': '#191919', 'height': 2000}, children=[
    # header and banner
    html.Div([

        html.H1('World Energy Consumption Report by Team Infinity', style={'color': '#FFFFFF'},
                className='gs-header gs-text-header padded')

    ]),
    # basic stats
    html.Div(
        id="quick-stats",
        className="row",
        children=[
            html.Div(
                id="card-1",
                children=[
                    html.P("Total Oil Consumption", style={'color': '#d6d6d6'}),
                    daq.LEDDisplay(
                        id="oil-led",
                        value="300",
                        color="#92e0d3",
                        backgroundColor="#1e2130",
                        size=30,
                    ),
                ], style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(
                id="card-2",
                children=[
                    html.P("Total natural gas consumption", style={'color': '#d6d6d6'}),
                    daq.LEDDisplay(
                        id="gas-led",
                        value="200",
                        color="#ffd500",
                        backgroundColor="#1e2130",
                        size=30,
                    ),
                ], style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(
                id="card-3",
                children=[
                    html.P("Total coal & peat products consumption", style={'color': '#d6d6d6'}),
                    daq.LEDDisplay(
                        id="coal-led",
                        value="170",
                        color="#002aff",
                        backgroundColor="#1e2130",
                        size=30,
                    ),
                ], style={'width': '20%', 'display': 'inline-block'}
            ),
            html.Div(
                id="card-4",
                children=[
                    html.P("Total electricity consumption", style={'color': '#d6d6d6'}),
                    daq.LEDDisplay(
                        id="electricity-led",
                        value="1500",
                        color="#ff002a",
                        backgroundColor="#1e2130",
                        size=30,
                    ),
                ], style={'width': '20%', 'display': 'inline-block'}
            )

        ]
    ),

    html.Div([

        html.Div([
            html.H4('Consumption level between countries', style={'color': '#FFFFFF'}, className='eight columns'),
            dcc.Dropdown(
                id='crossfilter_1',
                options=[{'label': i, 'value': i} for i in e_commodity],
                value='Primary coal and peat'
            )
        ],
            style={'width': '49%', 'display': 'inline-block', 'border': '0px solid black'}),
        html.Div([
            html.H4('Consumption trend by Country', style={'color': '#FFFFFF'}, className='eight columns'),
            dcc.Dropdown(
                id='crossfilter_2',
                options=[{'label': i, 'value': i} for i in e_transaction],
                value='Primary production'
            )
        ],
            style={'width': '49%', 'float': 'right', 'display': 'inline-block'})
    ],
        style={
            'borderBottom': 'thin lightgrey solid',
            'backgroundColor': 'rgb(0, 0, 0)',
            'padding': '10px 5px'
        }),
    html.Div([
        dcc.Graph(
            id='crossfilter-indicator-scatter',
            hoverData={'points': [{'customdata': 'AFG'}]}
        )
    ],
        style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Graph(id='COM_TRAN')

    ], style={'display': 'inline-block', 'width': '49%', 'float': 'right'}),

    html.Div(dcc.Slider(
        id='crossfilter-year--slider',
        min=df_ebal['TIME_PERIOD'].min(),
        max=df_ebal['TIME_PERIOD'].max(),
        value=df_ebal['TIME_PERIOD'].max(),
        marks={str(year): str(year) for year in df_ebal['TIME_PERIOD'].unique()},
        step=None
    ), style={'width': '49%', 'padding': '0px 20px 20px 20px'})

])


@app.callback(
    dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
    [dash.dependencies.Input('crossfilter_1', 'value'),
     dash.dependencies.Input('crossfilter_2', 'value'),
     dash.dependencies.Input('crossfilter-year--slider', 'value')])
def update_graph(filter_1,
                 filter_2,
                 year_value):
    dff = df_ebal[(df_ebal['TIME_PERIOD'] == year_value) & (df_ebal['TRANSACTION'] == filter_2)]

    fig = px.scatter(
        y=dff[dff['COMMODITY'] == filter_1]['value'],
        hover_name=dff[dff['COMMODITY'] == filter_1]['REF_AREA']
    )

    fig.update_traces(customdata=dff[dff['COMMODITY'] == filter_1]['REF_AREA'])

    fig.update_layout(margin={'l': 40, 'b': 40, 't': 10, 'r': 0}, hovermode='closest', paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)')

    return fig


def create_time_series(dff, title):
    fig = px.scatter(dff, x='TIME_PERIOD', y='value')

    fig.update_traces(mode='lines+markers')

    fig.add_annotation(x=0, y=0.85, xanchor='left', yanchor='bottom',
                       xref='paper', yref='paper', showarrow=False, align='left',
                       bgcolor='rgba(0, 0, 0, 0.1)', text=title)

    fig.update_layout(margin={'l': 20, 'b': 30, 'r': 10, 't': 10}, paper_bgcolor='rgba(0,0,0,0)',
                      plot_bgcolor='rgba(0,0,0,0)')

    return fig


@app.callback(
    dash.dependencies.Output('COM_TRAN', 'figure'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
     dash.dependencies.Input('crossfilter_1', 'value'),
     dash.dependencies.Input('crossfilter_2', 'value')])
def update_y_timeseries(hoverData, filter_1, filter_2):
    country_name = hoverData['points'][0]['customdata']
    dff = df_ebal[df_ebal['REF_AREA'] == country_name]
    dff = dff[(dff['COMMODITY'] == filter_1) & (dff['TRANSACTION'] == filter_2)]
    title = '<b>{}</b><br>{}'.format(country_name, filter_1)
    return create_time_series(dff, title)


if __name__ == '__main__':
    app.run_server(debug=True)

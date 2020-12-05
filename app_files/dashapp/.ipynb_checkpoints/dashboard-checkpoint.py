import pandas as pd
import numpy as np
import pymongo
import sklearn as sk
from matplotlib import pyplot as plt
import plotly as pl
from tqdm.auto import tqdm
from xml.etree import ElementTree
import plotly.graph_objects as go
import urllib
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from sklearn.linear_model import Ridge
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import PolynomialFeatures

from .layout import html_layout


def init_callbacks(app):
    # Dropdowns - World map
    @app.callback(
        Output("WORLD_MAP", 'figure'),
        Input("TRANSACTION_DPDN", 'value'),
        Input("COMMODITY_DPDN", 'value'),
    )
    def plot_world_map(trans, comm):

        #        plot_df = df_ebal.query("(TRANSACTION == @trans) and (COMMODITY == @comm)")
        plot_df = pd.DataFrame(col_ebal.aggregate([{'$match': {'TRANSACTION': trans, 'COMMODITY': comm}},
                                                   {"$project": {"_id": 0}},
                                                   {'$group': {'_id': '$REF_AREA', 'avg_value': {'$avg': '$value'}}}]
                                                  )
                               ).rename(columns={"_id": "REF_AREA"}).set_index('REF_AREA').iloc[:, 0]
        if plot_df.shape[0] == 0:
            return
        #        plot_df = plot_df.groupby(["REF_AREA"])['value'].mean()
        fig = go.Figure()
        fig.add_trace(
            go.Choropleth(
                locations=plot_df.index,
                z=plot_df.values,
                # colorscale="Viridis",
                colorscale=[
                    [0, "#519fd0"],
                    [0.2, "#7bb2d4"],
                    [0.4, "#fedfe1"],
                    [0.6, "#e7a5a5"],
                    [0.8, "#ce4c69"],
                    [1, "#993439"],
                ],
                marker_line_color='white',
                marker_line_width=0.5,
            )
        )
        fig.update_layout(
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            dragmode=False,
            #     range=[-90, 50],
            geo={
                'showframe': False,
                'showocean': True,
                'oceancolor': palette['ocean'],
                'showlakes': True,
                'lakecolor': palette['lake'],
                'showcoastlines': False,
                'landcolor': palette['background'],
            },
            plot_bgcolor=palette['background'],
            paper_bgcolor=palette['background'],
            font_color=palette['text'],
            height=580,
        )
        fig.update_traces(
            colorbar={'x': 0.93}, selector=dict(type='choropleth')
        )
        return fig

    @app.callback(
        Output("COUNTRY_SUMMARY", 'figure'),
        Input("TRANSACTION_DPDN", 'value'),
        Input("COMMODITY_DPDN", 'value'),
        Input("WORLD_MAP", 'clickData')
    )
    def plot_country_summary(trans, comm, country):
        """
        :param trans: The transaction currently selected
        :param comm: The commodity currently selected
        :param country: The country that was just clicked on
            has format like:
            {'points': [{'curveNumber': 0, 'pointNumber': 36, 'pointIndex': 36, 'location': 'CHN', 'z': 271769.93042857136}]}
        """

        # Change code below
        if country is None:
            country = 'USA'
        else:
            country = country['points'][0]['location']

        # plot_df_ebal = df_ebal.query("(TRANSACTION == @trans) and (COMMODITY == @comm) and (REF_AREA == @country)")
        plot_df_ebal = pd.DataFrame(col_ebal.find({'TRANSACTION': trans, 'COMMODITY': comm, 'REF_AREA': country}))
        if country in col_unfcc.distinct('REF_AREA'):
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            plot_df_unfcc = pd.DataFrame(
                col_unfcc.find({'INDICATOR': 'EN_ATM_CO2E_XLULUCF',
                                'REF_AREA': country})
            )

            fig.add_trace(
                go.Bar(
                    name='CO2 Emission',
                    x=plot_df_unfcc['TIME_PERIOD'],
                    y=plot_df_unfcc['value']
                ),
                secondary_y=True
            )

            fig.update_traces(
                marker_color='rgb(158,202,225)',
                marker_line_color='rgb(8,48,107)',
                marker_line_width=1,
                opacity=0.2)

            fig.add_trace(
                go.Scatter(
                    name=comm,
                    mode='lines+markers',
                    x=plot_df_ebal['TIME_PERIOD'],
                    y=plot_df_ebal['value']
                ),
                secondary_y=False
            )

            fig.update_layout(
                title={'text': "Quantity of " + trans + " in " + country + " from UNdata, 1990 to 2017",
                       'y': 0.9,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01),
                xaxis_title="Year",
                font=dict(size=12),
                hovermode="x unified",
                plot_bgcolor=palette['background'],
                paper_bgcolor=palette['background'],
                font_color=palette['text']
            )

            # Set y-axes titles
            fig.update_yaxes(
                title_text="Quantity in HSO",
                secondary_y=False)
            fig.update_yaxes(
                title_text="CO2 Emission in kilotonne CO2 equivalent",
                range=[min(plot_df_unfcc['value']) - 10000, max(plot_df_unfcc['value']) + 10000],
                secondary_y=True)
            return fig

        else:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    name=comm,
                    mode='lines+markers',
                    x=plot_df_ebal['TIME_PERIOD'],
                    y=plot_df_ebal['value']
                ))
            fig.update_layout(
                title={'text': "Quantity of " + trans + " in " + country + " from UNdata, 1990 to 2017",
                       'y': 0.9,
                       'x': 0.5,
                       'xanchor': 'center',
                       'yanchor': 'top'},
                xaxis_title="Year",
                yaxis_title="Quantity in HSO",
                font=dict(size=12),
                hovermode="x unified",
                plot_bgcolor=palette['background'],
                paper_bgcolor=palette['background'],
                font_color=palette['text']
            )
            return fig

    @app.callback(
        Output("PIE_CHART", 'figure'),
        Output("BAR_CHART", 'figure'),
        Output("TRANCOMM_LINE", 'children'),
        Input("TRANSACTION_DPDN", 'value'),
        Input("COMMODITY_DPDN", 'value'),
        Input("COUNTRY_SUMMARY", 'clickData'),
        Input("WORLD_MAP", 'clickData'),
    )
    def plot_pie_bar(trans, comm, year, country):
        """
        :param trans: The transaction currently selected
        :param comm: The commodity currently selected
        :param year: The year, currently unused
        :param country: The country that was just clicked on
            has format like:
            {'points': [{'curveNumber': 0, 'pointNumber': 36, 'pointIndex': 36, 'location': 'CHN', 'z': 271769.93042857136}]}
        """
        if year is None:
            year = 2012
        else:
            year = year['points'][0]['x']

        if country is None:
            country = 'USA'
        else:
            country = country['points'][0]['location']

        plot_df_pie = pd.DataFrame(col_ebal.find({'$and': [{'TRANSACTION': {'$in': transactions}},
                                                           {'COMMODITY': comm},
                                                           {'TIME_PERIOD': year},
                                                           {'REF_AREA': '{}'.format(country)}
                                                           ]
                                                  })
                                   )
        plot_df_bar = pd.DataFrame(col_ebal.find({'$and': [{'TRANSACTION': trans},
                                                           {'COMMODITY': {'$in': commodities}},
                                                           {'TIME_PERIOD': year},
                                                           {'REF_AREA': '{}'.format(country)}
                                                           ]
                                                  })
                                   )

        bar_fig = go.Figure()
        for transaction, group in plot_df_bar.groupby("TRANSACTION"):
            bar_fig.add_trace(go.Bar(
                x=group["COMMODITY"],
                y=group["value"],
                name=transaction,
            ))

        # bar_fig.update_xaxes(title_text="Commodity")
        bar_fig.update_yaxes(title_text="Energy (in TJ)")
        bar_fig.update_layout(
            # title="Distribution of {} across all Commodities in {} {}".format(trans, str(year), country),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            dragmode=False,
            plot_bgcolor=palette['background'],
            paper_bgcolor=palette['background'],
            font_color=palette['text'],
        )

        pie_fig = go.Figure()
        pie_fig.add_trace(go.Pie(labels=plot_df_pie['TRANSACTION'].unique(), values=plot_df_pie['value'],
                                 name=comm, showlegend=True, )
                                 )

        pie_fig.update_layout(
            legend=dict(
                yanchor="auto",
                # y=0.99,
                xanchor="auto",
                x=-0.5,
                bgcolor='rgba(0,0,0,0)'
            ),
            # title='Distribution of {} across different transactions in {} {}'.format(trans, str(year), country),
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            dragmode=False,
            plot_bgcolor=palette['background'],
            paper_bgcolor=palette['background'],
            font_color=palette['text'],
        )
        pie_fig.update_traces(hole=.2, hoverinfo="label+percent+name")

        line = [
            html.H3(
                children=[
                    "Transaction and Commodity Summary",
                    html.Br(),
                    "For {}".format(country),
                ],
                style={
                    'font-size': '6rem',
                    'text-align': 'center'
                }
            )
        ]
        return pie_fig, bar_fig, line

    @app.callback(
        Output("PRED_LINE", 'children'),
        Input("TRANSACTION_DPDN", 'value'),
        Input("COMMODITY_DPDN", 'value'),
        Input("WORLD_MAP", 'clickData'),
    )
    def gen_preds(trans, comm, country, window=6, model_func=Ridge):

        if country is None:
            country = 'USA'
        else:
            country = country['points'][0]['location']

        pred_df_ebal = pd.DataFrame(col_ebal.find({'TRANSACTION': trans, 'COMMODITY': comm}))
        pred_df_ebal = pred_df_ebal.groupby(["TIME_PERIOD", "REF_AREA"])["value"].sum().unstack(level=1)

        if pred_df_ebal.shape[0] != 0:

            nan_areas = pred_df_ebal.columns[pd.isna(pred_df_ebal).mean(axis=0) == 1]
            pred_df_ebal = pred_df_ebal.drop(nan_areas, axis=1)

            if country in pred_df_ebal.columns:

                im = SimpleImputer()
                train_data = np.transpose(im.fit_transform(pred_df_ebal), (1, 0))
                model = model_func()

                X = []
                y = []
                for i in range(train_data.shape[1] - window - 1):
                    X.append(train_data[:, i:(i + window)])
                    y.append(train_data[:, i + window])
                train_X = np.concatenate(X[:-1])
                train_y = np.concatenate(y[:-1])
                model.fit(train_X, train_y)

                pred = model.predict(pred_df_ebal.loc[:, country].values[-window:].reshape(1, -1))[0]

                mill_inds = ['', 'K', 'M', 'B', 'T']
                num_mill = int(np.floor(np.log10(abs(pred)))) // 3
                pred_string = "{:.1f}".format(pred / (10 ** (num_mill * 3))) + mill_inds[num_mill]

                return [
                    html.H3([
                        "Based on previous data, we predict",
                        ],
                        style={'font-size': '3rem', 'text-align': 'center'}
                    ),
                    html.H3([
                        trans,
                    ],
                        style={'font-size': '6rem', 'text-align': 'center'}
                    ),
                    html.H3([
                        "for",
                    ],
                        style={'font-size': '3rem', 'text-align': 'center'}
                    ),
                    html.H3([
                        comm,
                        " in ",
                        country,
                    ],
                        style={'font-size': '6rem', 'text-align': 'center'}
                    ),
                    html.H3([
                        "next year to be",
                    ],
                        style={'font-size': '3rem', 'text-align': 'center'}
                    ),
                    html.H3([pred_string], style={'font-size': '40rem', 'text-align': 'center'}),
                ]

        return [
            html.H3([
                "Due to lack of previous data, we are unable to predict next year's",
            ],
                style={'font-size': '3rem', 'text-align': 'center'}
            ),
            html.H3([
                trans,
            ],
                style={'font-size': '6rem', 'text-align': 'center'}
            ),
            html.H3([
                "for",
            ],
                style={'font-size': '3rem', 'text-align': 'center'}
            ),
            html.H3([
                comm,
                " in ",
                country,
            ],
                style={'font-size': '6rem', 'text-align': 'center'}
            ),
            html.H3([
                "at the moment.",
            ],
                style={'font-size': '3rem', 'text-align': 'center'}
            ),
        ]


# app.run_server(debug=False)

def init_dashboard(server):
    MONGO_URI = 'mongodb+srv://sayan:infinity@infinity.9hew3.mongodb.net/<dbname>?retryWrites=true&w=majority'
    # MONGO_URI='localhost:27017'
    client = pymongo.MongoClient(MONGO_URI)

    global col_ebal, col_unfcc
    db = client.UNSD
    col_ebal = db.ebal
    col_unfcc = db.unfcc

    global commodities
    global transactions
    global palette
    # global df_unfcc
    # global df_ebal
    # global df_unfcc_co2
    # global df_inner

    commodities = [
        'Oil Products',
        'Electricity',
        'Natural Gas',
        'Memo: Renewables',
        'Biofuels and waste',
    ]

    transactions = [
        'Primary production',
        'Total energy supply',
        'Final consumption',
        'Final Energy Consumption',
        'Exports',
        'Imports',
        'Other Consumption',
        'Transformation',
        'Manufacturing, construction and non-fuel mining industries',
        'Electricity, Heat and CHP plants',
    ]

    # df_unfcc = pd.DataFrame(col_unfcc.find()).drop("_id", axis=1)
    # df_ebal = pd.DataFrame(col_ebal.find()).drop("_id", axis=1)
    # df_ebal_small = df_ebal.query("(COMMODITY in @commodities) and (TRANSACTION in @transactions)").reset_index(drop=True)
    palette = {
        'background': 'rgba(0, 0, 0, 0)',
        'text': '#787c7a',
        'ocean': '#fcfaf2',
        'lake': '#fcfaf2',
        'land': '#fcfaf2'
    }
    # df_unfcc_co2 = df_unfcc[df_unfcc['INDICATOR'] == 'EN_ATM_CO2E_XLULUCF']
    # df_inner = pd.merge(df_unfcc_co2, df_ebal_small, on=['REF_AREA','TIME_PERIOD'], how='inner')

    blank_fig = go.Figure()
    blank_fig.add_trace(go.Choropleth())
    blank_fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        dragmode=False,
        geo={
            'showocean': True,
            'oceancolor': palette['ocean'],
            'showlakes': True,
            'lakecolor': palette['lake'],
            'showcoastlines': False,
            'landcolor': palette['background'],
        },
    )
    blank_fig.update_layout(
        plot_bgcolor=palette['background'],
        paper_bgcolor=palette['background'],
        font_color=palette['text'],
    )

    # external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    # app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.CYBORG])
    app = dash.Dash(__name__,
                    server=server,
                    routes_pathname_prefix="/dashapp/",
                    # assets_url_path="/static/dist/css/maps",
                    external_stylesheets=[
                        "/static/dist/css/maps/styles.css",
                        "https://fonts.googleapis.com/css?family=Montserrat:300,400,500,700|Open+Sans:300,300i,400,400i,700,700i"
                        "https://fonts.googleapis.com/css?family=Lato",
                        "/static/dist/css/maps/base.css",
                        "/static/dist/css/maps/main.css",
                        "/static/dist/css/maps/vendor.css",
                        "/static/dist/css/maps/fonts.css",
                    ],
                    )

    app.layout = html.Div(children=[

        # Page title
        html.Div(
            className='home-content-left',
            children=[
                html.H1(["Energy Data Dashboards"], style={
                    'color': '#0b1013', 'text-transform': 'capitalize', 'font-family': "'aqua grotesque', sans-serif"
                }),
                html.Div(
                    children=[
                        dcc.Dropdown(
                            id='TRANSACTION_DPDN',
                            options=[{'label': trans, 'value': trans} for trans in transactions],
                            value=transactions[1],
                        )
                    ],
                    style={"width": '20%', 'display': 'inline-block'},
                ),
                html.Div(
                    children=[
                        dcc.Dropdown(
                            id='COMMODITY_DPDN',
                            options=[{'label': comm, 'value': comm} for comm in commodities],
                            value=commodities[0],
                        ),
                    ],
                    style={"width": '20%', 'display': 'inline-block'},
                ),
            ],
            style={'width': '100%', 'background': '#d4c98e', 'text-align': 'center',
                   'font-size': '1.5rem', 'position': 'sticky', 'top': '0', 'padding': '50px', 'z-index': '100',
                   'text-transform': 'uppercase', 'font-family': "'aqua grotesque', sans-serif"}
        ),

        html.Div(
            style={'width': '100%', 'padding': '20px'}
        ),

        # Dropdowns
        html.Div(
            [],
            style={'display': 'block', 'width': '80%', 'margin-left': 'auto', 'margin-right': 'auto'}
        ),

        # World map
        html.Div(
            children=[
                dcc.Graph(
                    id="WORLD_MAP",
                    figure=blank_fig,
                )
            ],
            style={'display': 'inline-block', 'width': '95%', 'position': 'relative', 'right': '100px'}
        ),

        # Country Summary
        html.Div(children=[
            html.Div(
                children=[
                    dcc.Graph(
                        id="COUNTRY_SUMMARY",
                        figure=blank_fig,
                    )
                ],
                style={'display': 'inline-block', 'width': '60%'}
            ),

            html.Div(
                className='home-content-right',
                children=[
                    html.H3(["Country Summary"])
                ],
                style={'width': '40%', 'display': 'inline-block'},
            ),
            ],
            style={'width': '80%', 'display': 'block', 'margin-right': 'auto', 'margin-left': 'auto'}
        ),

        # Pie and bar
        html.Div([

            html.Div(
                id="TRANCOMM_LINE",
                className='home-content-left',
                children=[
                    html.H3(
                        children=[
                            "Transaction and Commodity Summary",
                            html.Br(),
                            "For USA"
                        ],
                        style={
                            'font-size': '6rem',
                            'text-align': 'center'
                        }
                    )
                ],
                style={'width': '100%', 'display': 'inline-block', 'margin-left': 'auto', 'margin-right': 'auto', }
            ),

            # Bar and Pie charts
            html.Div(
                children=[
                    html.Div([
                        dcc.Graph(
                            id="PIE_CHART",
                            figure=blank_fig,
                            style={'height': '550px'}
                        )],
                        style={'width': '40%', 'display': 'inline-block', 'margin-left': '0', 'margin-right': 'auto'}
                    ),
                    html.Div([],
                             style={'width': '15%', 'display': 'inline-block'}),
                    html.Div([
                        dcc.Graph(
                            id="BAR_CHART",
                            figure=blank_fig,
                            style={'height': '550px'}
                        )],
                        style={'width': '40%', 'display': 'inline-block', 'margin-right': '0', 'margin-left': 'auto'}
                    ),
                ],
                style={'display': 'inline-block', 'width': '100%'}
                ),
            ],
            style={'width': '80%', 'display': 'block', 'margin-right': 'auto', 'margin-left': 'auto'}
        ),

        # Predictor
        html.Div(
            id='PRED_LINE',
            className='home-content-left',
            children=[
                html.H3("Based on previous data, we predict the next year's {}")
            ],
            style={'display': 'inline-block', 'width': '100%'}
        ), ],
        # style={
        #     'backgroundColor': palette['background']
        # }
    )
    init_callbacks(app)

    return app.server

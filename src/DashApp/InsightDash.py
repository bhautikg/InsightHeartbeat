# -*-coding: utf-8 -*-
import time
import numpy as np
import dash
import dash_core_components as dcc
import dash_html_components as html
from data_util import DataUtil
import flask
from flask_caching import Cache

# TODO: Deploy with app with Heroku.


# Setup flask server
server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
})
TIMEOUT = 2

app.config['suppress_callback_exceptions'] = True


# Connect query object to database
#postgres_config_infile = '../../.config/postgres.config'
query_helper = DataUtil()

queries={'signames_ecg':[], 'signals':[]}

@cache.memoize(timeout=TIMEOUT)
def update():
    queries['signames_ecg'], queries['signals'] = query_helper.getLastestECGSamples(10)
    #queries['signames_hr'], queries['hrvariability'], queries['latesthr'] = query_helper.getHRSamples()
    # return (signames_ecg, signals, signames_hr, hrvariability, latesthr)

@app.callback(
    dash.dependencies.Output('ecg-output', 'children'),
    [dash.dependencies.Input('refresh', 'interval-component')])


def get_ecg_graph():
    """
    Plots ECG signals for each patient input.
    """
    titles = ['ecg1', 'ecg2', 'ecg3']
    colors = ['rgb(240,0,0)', 'rgb(0,240,0)', 'rgb(0,0,240)']
    update()
    signames_ecg = queries['signames_ecg']
    signals = queries['signals']
    return html.Div(className='ecg', children=[
        html.Div(style={'display': 'flex', 'height': '40vh'},
                 children=[dcc.Graph(
                     id=titles[i] + signame,
                     style={'width': '100%'},
                     figure={
                         'data': [
                             {'x': signals[signame]['time'],
                              'y': signals[signame][titles[i]],
                              'mode': 'line', 'name': signame, 'line': {'color':colors[i]}}
                         ],
                         'layout': {
                             'font': {'color':'#fff'},
                             'title': '{}-{}'.format(signame, titles[i]),
                             'xaxis': {'title': 'time', 'color': '#fff', 'showgrid': 'False'},
                             'yaxis': {'title': 'voltage (mv)', 'color': '#fff', 'showgrid': 'False', 'range': np.linspace(-2.5, 2.5, 10)},
                             'paper_bgcolor':'#000', 'plot_bgcolor':'#000'
                         }
                     }
                 ) for i in range(len(titles))]
                          
                 ) for signame in signames_ecg])


# app layout with separate tabs for ECG and HR graphs.
app.layout = html.Div(className='main-app', style={'fontFamily': 'Sans-Serif',
                                                   'backgroundColor':'black', 'color': 'white'},
                      children=[
                          html.H1(children='ECGdashboard For Monitored Patients', style={'margin-top':'0'}),
                          dcc.Tabs(className="tabs", children=[
                              dcc.Tab(label='ECG Signals', style={'backgroundColor':'black',
                              'color':'white'}, children=html.Div(id='ecg-output')),
                          ], style={
                              'width': '50vh',
                              'textAlign': 'left',
                              'fontSize': '12pt',
                          }),
                          dcc.Interval(id='refresh', interval=2 * 1000, n_intervals =0)
                        ]
                    )



if __name__ == '__main__':
    app.run_server(debug=True,dev_tools_hot_reload=True)
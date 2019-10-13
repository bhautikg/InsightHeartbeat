"""
Displays Table of latest latest ECG events, and whether they are normal.
You can selet an Event ID which is True for abnormal to view its raw signal 
Template:
sudo python run_app.py
"""

# IMPORTED LIBRARIES
from dash.dependencies import Input
from dash.dependencies import Output
from itertools import chain as flat
from textwrap import dedent as ded
from data_util import DataUtil

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dte
import os
import pandas as pd
import plotly.graph_objs as go

import flask
from flask_caching import Cache

# Setup flask server
server = flask.Flask(__name__)
app = dash.Dash("Charge_Tracker",
                external_stylesheets=[\
                        "https://codepen.io/chriddyp/pen/bWLwgP.css"])
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
})
TIMEOUT = 2
app.config['suppress_callback_exceptions'] = True

## GLOBAL DEFINITIONS 


query_helper = DataUtil()

queries_sig={'signames_ecg':[], 'signals':[]}
queries_name={'signames_ecg':[]}
queries_name={'signames_ecg':['101', '103']}
queries_id={'id':['1']}

evnt_df = query_helper.getAllEvents()
# sig_df = query_helper.getECGSignal('50')
# x=list(range(len(sig_df.get_value(0,'ecg'))))
# y_sig= sig_df.get_value(0,'ecg')
# trace = []
# trace.append(go.Scatter(x=x, y=y_sig, mode='lines',
#                 marker={'size': 8, "opacity": 0.6, "line": {'width': 0.5}}, ))
# print("FIRST TRACE")
# print(trace)
# layout =  go.Layout(title="ECG abnormal signal", colorway=['#fdae61', '#abd9e9', '#2c7bb6'],
#                                 yaxis={"title": "Voltage(mV)"}, xaxis={"title": "ECG Signal"})
# fig = go.Figure(data = trace, layout = layout)
# print("FIRST FIGURE")
# print (fig)


@cache.memoize(timeout=TIMEOUT)

def get_signames():
    queries_name['signames_ecg'] = query_helper.getSigNames()

# def get_id():
#     queries_name['signames_ecg'] = query_helper.getSigNames()

# Sets Table and dropdown options

table_order = ["id", "signal name", "timestamp", "abnormal"]


# Sets Dash application parameters
#server = app.server
app.layout = html.Div([
        html.Div([
                dcc.Markdown(ded("""
                **ECG Tracker: near real time ECG Monitoring**
                Choose a patient ID to see all latest abnormal events for them
                """)),
                # dcc.Graph(
                #         id="ECGGraph"),
                #         # figure = fig),
                dcc.Tab(label='ECG Signals', style={'backgroundColor':'black',
                'color':'white'}, children=html.Div(id='ecg-output')),
                dcc.Interval(
                        id="real_time_updates",
                        interval=10 * 1000,
                        n_intervals=0)],
                style={
                        "width": "100%",
                        "height": "auto",
                        "display": "scatter",
                        "padding-bottom": "75px"}
                ),
        html.Div([
                dcc.Markdown(ded("""
                **Table of all recent ECG events**
                """)),
                html.Div(
                        dcc.Input(
                            id="table_groups",
                            type = "text",
                            placeholder="enter a patient id..."
                            ),
                        style={
                                "width": "48%",
                                "display": "inline-block"}
                        ),
                html.Div(
                        dcc.Input(
                            id="ecg_id",
                            type = "text",
                            placeholder="enter ID number..."
                            ),
                        style={
                                "width": "48%",
                                "float": "right",
                                "display": "inline-block"}
                        ),
                dte.DataTable(
                        data=evnt_df.to_dict('records'),
                        columns =[{"name": i, "id": i} for i in evnt_df.columns],
                        id="group_detail")],
                        
                        
                style={
                        "width": "100%",
                        "height": "auto",
                        "display": "scatter",
                        "padding-bottom": "125px"}
                )
        ],
        style={
                "width": "99%",
                "height": "auto",}
        )

## FUNCTION DEFINITIONS

# def make_trace(df):
#     """
#     For selected group "c", creates Plotly scatter objects.
#     """
#     print("MAKE TRACE")
#     x=list(range(len(df.get_value(0,'ecg'))))
#     print(x)
#     print("y_sig")
#     y_sig= df.get_value(0,'ecg')
#     print(y_sig)
#     print("TRACE")
#     trace = []
#     trace.append(go.Scatter(x=x, y=y_sig, mode='lines',
#                 marker={'size': 8, "opacity": 0.6, "line": {'width': 0.5}}, ))
#     print(trace)

#     return trace


# Callback updates graph (OUTPUT) according to time interval (INPUT)
@app.callback(Output('ecg-output', 'children'),
              [Input('ecg_id', 'value')]
              )
def update_graph(ecg_id):
    """
    Queries for the event Id and and assembles results in Dash format.
    """

    if ecg_id is not None :

            if ecg_id != "":
                    ecg_id = "'{}'".format(ecg_id)
                    df = query_helper.getECGSignal(ecg_id)
                    # Creates all scatter data for real-time graph
                #     trace = make_trace(df)
                    # Sets layout 
                    x=list(range(len(df.at[0,'ecg'])))

                    y_sig= df.get_value(0,'ecg')


                    return html.Div(className='ecg', children=[
                        html.Div(style={'display': 'flex', 'height': '40vh'},
                                children=[dcc.Graph(
                                id=ecg_id,
                                style={'width': '100%'},
                                figure={
                                        'data': [
                                        {'x': x,
                                        'y': y_sig,
                                        'mode': 'line', 'name': ecg_id, 'line': {'color':'rgb(240,0,0)'},}
                                        ],
                                        'layout': {
                                        'font': {'color':'#fff'},
                                        'title': '{}'.format(ecg_id),
                                        'xaxis': {'title': 'time', 'color': '#fff', 'showgrid': 'False'},
                                        'yaxis': {'title': 'voltage (mv)', 'color': '#fff', 'showgrid': 'True',
                                        'paper_bgcolor':'#000', 'plot_bgcolor':'#000'}
                                        }
                                }
                                )]
                                        
                        )]) 
 


# Callback updates graph (OUTPUT) according to time interval (INPUT)
@app.callback(Output('group_detail', 'data'),
              [Input('table_groups', 'value'), 
              Input("real_time_updates", "n_intervals")]
              )
def update_table(group_name, interval, max_rows=50):
    """
    Queries table, analyzes data, and assembles results in Dash format.
    """
    # Pulls all data from TSDB and puts it in pandas dataframe
    
    if group_name is None:
            df = query_helper.getAllEvents()
    else:
            if len(group_name) != 3:
                   df = query_helper.getAllEvents()
            else:
                   group_name = "'{}'".format(group_name)
                   df = query_helper.getLatestEvents(group_name)


    return df.to_dict("records")


## MAIN MODULE
if __name__ == "__main__":
    # Sets formatting for retrieved database query
    # Starts Flask/Dash app
    #app.run_server(debug=True, dev_tools_hot_reload=True)
    app.run_server(host='0.0.0.0', port=80)


## END OF FILE
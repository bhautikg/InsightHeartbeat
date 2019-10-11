"""
Displays summary visual showing aggreagated battery data using Dash library.
Graph 
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
sig_df = query_helper.getECGSignal(1)
x=list(range(len(sig_df.get_value(0,'ecg'))))
y_sig= sig_df.get_value(0,'ecg')
trace = []
trace.append(go.Scatter(x=x, y=y_sig, mode='lines',
                marker={'size': 8, "opacity": 0.6, "line": {'width': 0.5}}, ))
layout =  go.Layout(title="ECG abnormal signal", colorway=['#fdae61', '#abd9e9', '#2c7bb6'],
                                yaxis={"title": "Voltage(mV)"}, xaxis={"title": "ECG Signal"})
fig = go.Figure(data = trace, layout = layout)


@cache.memoize(timeout=TIMEOUT)

def get_signames():
    queries_name['signames_ecg'] = query_helper.getSigNames()

# def get_id():
#     queries_name['signames_ecg'] = query_helper.getSigNames()

# Sets Table and dropdown options
all_groups = queries_name['signames_ecg']
all_ids = queries_id['id']
table_order = ["signal name", "timestamp", "abnormal"]
get_signames()

# Sets Dash application parameters
#server = app.server
app.layout = html.Div([
        html.Div([
                dcc.Markdown(ded("""
                **Charge Tracker: near real time battery monitoring**
                For each battery group, displays average energy (lines) and
                standard deviation (shaded area).
                """)),
                dcc.Graph(
                        id="ECG signal",
                        figure = fig),
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
                **Group deep drive**
                For given group and number of discharge cycles, identify
                whether batteries are representatives or outliers.
                
                Note: 100 % percent deviation indicates value is
                2 standard deviations away from the group's mean.
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

# def analyze_all_groups():
#     """
#     Aggregates queried Cassandra data by mean, std dev, counts, and error.
#     """
#     # Pulls all data from Cassandra into Pandas dataframe
#     df_all = query_cassandra("""
#                              SELECT
#                              group,
#                              cycle,
#                              double_sum(metric) AS metric
#                              FROM battery_metrics.discharge_energy;
#                              """)

#     # Calculates aggreates (mean, std dev, count, error, upper/lower limits)
#     pg = df_all.groupby(["group", "cycle"])
#     df = pd.DataFrame({"mean": pg["metric"].mean(),
#                        "stdev": pg["metric"].std(),
#                        "count": pg["metric"].count(),}).reset_index()
#     df["error"] = df["stdev"] * 100.0 / df["mean"]
#     return df

def make_trace(df):
    """
    For selected group "c", creates Plotly scatter objects.
    """
    x=list(range(len(df.get_value(0,'ecg'))))
    y_sig= df.get_value(0,'ecg')
    trace = []
    trace.append(go.Scatter(x=x, y=y_sig, mode='lines',
                marker={'size': 8, "opacity": 0.6, "line": {'width': 0.5}}, ))

    return trace


# Callback updates graph (OUTPUT) according to time interval (INPUT)
@app.callback(Output("ECG signal", "figure"),
              [Input('ecg_id', 'value')]
              )
def update_graph(ecg_id):
    """
    Queries table, analyzes data, and assembles results in Dash format.
    """

    if ecg_id is not None :

            ecg_id = "'{}'".format(ecg_id)
            df = query_helper.getECGSignal(ecg_id)            
    else:
            df = query_helper.getECGSignal(1)
    print ("UPDATE GRAPH")
    print(df)

    # Creates all scatter data for real-time graph
    trace = [make_trace(df)]

    # Sets layout 
    layout =  go.Layout(title="ECG abnormal signal", colorway=['#fdae61', '#abd9e9', '#2c7bb6'],
                        yaxis={"title": "Voltage(mV)"}, xaxis={"title": "ECG Signal"})
    fig = go.Figure(data = trace, layout = layout)

    return fig

# Callback updates graph (OUTPUT) according to time interval (INPUT)
@app.callback(Output('group_detail', 'data'),
              [Input('table_groups', 'value'), 
              Input("real_time_updates", "n_intervals")]
              )
def update_table(group_name, interval, max_rows=50):
    """
    Queries table, analyzes data, and assembles results in Dash format.
    """
    # Pulls all data from Cassandra into Pandas dataframe
    
    if group_name is None:
            df = query_helper.getAllEvents()
    else:

            group_name = "'{}'".format(group_name)
            df = query_helper.getLatestEvents(group_name)

    print ("UPDATE TABLE")
    print(df)
    queries_id['id'] = df['id'].tolist
    print(queries_id['id'])


    return df.to_dict("records")


## MAIN MODULE
if __name__ == "__main__":
    # Sets formatting for retrieved database query
    # Starts Flask/Dash app
    #app.run_server(debug=True, dev_tools_hot_reload=True)
    app.run_server(host='0.0.0.0', port=80)


## END OF FILE

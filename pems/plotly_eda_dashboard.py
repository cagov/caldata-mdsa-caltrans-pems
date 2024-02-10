# Import necessary libraries

import math
import os
import re

import dash
import dash_bootstrap_components as dbc
import dash_daq as daq
import ibis
import pandas as pd
import plotly.express as px
from dash import dcc, html
from dash.dependencies import Input, Output
from dotenv import load_dotenv

load_dotenv()
USERNAME = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")


print("connecting to snowflake...")


# Connect to Snowflake
ibis_con = ibis.snowflake.connect(
    user=USERNAME,
    password=PASSWORD,
    role="TRANSFORMER_DEV",
    # private_key=p_key,
    account="vsb79059-dse_caltrans_pems",
    database="RAW_DEV/CLEARINGHOUSE",
)


station_metadata = ibis_con.table(
    database="TRANSFORM_DEV", schema="DBT_MMARKS", name="TBL_MOST_RECENT_STATION_META"
)


################################################################################################
#### You can comment this part out if you aleady saved agg data to pandas dataframe locally ####
################################################################################################
pd_station_metadata = station_metadata.execute()
pd_station_metadata["ID"] = pd_station_metadata["ID"].astype("int64")
pd_station_metadata.set_index("ID", inplace=True)


station_agg_stats_by_day = ibis_con.table(
    database="TRANSFORM_DEV", schema="DBT_MMARKS", name="TBL_STATION_DAY_AGG_STATS_V2"
)
pd_station_agg_stats_by_day = station_agg_stats_by_day.execute()

expected_num_daily_observations = 2880

pd_station_agg_stats_by_day["FLOW_1_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["FLOW_1_COUNT"] / expected_num_daily_observations
)
pd_station_agg_stats_by_day["FLOW_2_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["FLOW_2_COUNT"] / expected_num_daily_observations
)
pd_station_agg_stats_by_day["FLOW_3_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["FLOW_3_COUNT"] / expected_num_daily_observations
)
pd_station_agg_stats_by_day["FLOW_4_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["FLOW_4_COUNT"] / expected_num_daily_observations
)
pd_station_agg_stats_by_day["OCCUPANCY_1_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["OCCUPANCY_1_COUNT"] / expected_num_daily_observations
)
pd_station_agg_stats_by_day["SPEED_1_Pct_Expected_Obs"] = (
    pd_station_agg_stats_by_day["SPEED_1_COUNT"] / expected_num_daily_observations
)


# Convert SAMPLE_DATE to datetime
pd_station_agg_stats_by_day["SAMPLE_DATE"] = pd.to_datetime(
    pd_station_agg_stats_by_day["SAMPLE_DATE"]
)

print(pd_station_agg_stats_by_day.dtypes)

float16_colnames = [
    "FLOW_1_AVG",
    "FLOW_2_AVG",
    "FLOW_3_AVG",
    "FLOW_4_AVG",
    "OCCUPANCY_1_AVG",
    "SPEED_1_AVG",
    "FLOW_1_Pct_Expected_Obs",
    "FLOW_2_Pct_Expected_Obs",
    "FLOW_3_Pct_Expected_Obs",
    "FLOW_4_Pct_Expected_Obs",
    "OCCUPANCY_1_Pct_Expected_Obs",
    "SPEED_1_Pct_Expected_Obs",
]
for col in float16_colnames:
    pd_station_agg_stats_by_day[col] = pd_station_agg_stats_by_day[col].astype(
        "float16"
    )


int16_colnames = [
    "FLOW_1_COUNT",
    "FLOW_2_COUNT",
    "FLOW_3_COUNT",
    "FLOW_4_COUNT",
    "OCCUPANCY_1_COUNT",
    "SPEED_1_COUNT",
]
for col in int16_colnames:
    pd_station_agg_stats_by_day[col] = pd_station_agg_stats_by_day[col].astype("int16")

pd_station_agg_stats_by_day["ID"] = pd_station_agg_stats_by_day["ID"].astype("int32")
print(pd_station_agg_stats_by_day.dtypes)

# Extract the list of unique IDs from pd_station_agg_stats_by_day
unique_ids = pd_station_agg_stats_by_day["ID"].unique()

# Filter pd_station_metadata to include only rows with IDs present in unique_ids
pd_station_metadata = pd_station_metadata[pd_station_metadata.index.isin(unique_ids)]
################################################################################################


######################################################################
####    Write aggregate pd dataframe to disk for future use      ####
#### Use this to prevent having to load from Snowflake each time ####
######################################################################
# # Save pd_station_metadata to a pickle file
# pd_station_metadata.to_pickle("pd_station_metadata.pkl")

# # Save pd_station_agg_stats_by_day to a pickle file
# pd_station_agg_stats_by_day.to_pickle("pd_station_agg_stats_by_day.pkl")

# print("loading local pandas dataframes...")
# # Load pd_station_metadata from the pickle file
# pd_station_metadata = pd.read_pickle("pd_station_metadata.pkl")

# # Load pd_station_agg_stats_by_day from the pickle file
# pd_station_agg_stats_by_day = pd.read_pickle("pd_station_agg_stats_by_day.pkl")
######################################################################


print("Data load complete. Starting app....")


def create_number_tile(number, description, p_txt_size=19, h_txt_size=45):
    """
    Create a number tile for Dash app.

    :param number: The number to display in the tile.
    :param description: The description text to display below the number.
    :return: A Dash HTML Div representing the number tile.
    """
    number_tile_div = html.Div(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H3(
                        number,
                        style={
                            "textAlign": "center",
                            "fontSize": h_txt_size,
                            "marginLeft": "10px",
                            "marginRight": "10px",
                            "marginTop": "6px",
                            "marginBottom": "6px",
                        },
                    ),
                    html.P(
                        description,
                        style={
                            "textAlign": "center",
                            "fontSize": p_txt_size,
                            "marginLeft": "10px",
                            "marginRight": "10px",
                            "marginTop": "3px",
                            "marginBottom": "6px",
                        },
                    ),
                ]
            ),
            style={
                "border": "1px solid lightgrey",
                "borderRadius": "5px",
                "padding": "1px",
                "textAlign": "center",
                "height": "94px",
                "marginLeft": "10px",
                "marginRight": "10px",
                "marginTop": "0px",
                "marginBottom": "0px",
            },
        ),
        style={},
    )
    return number_tile_div  # noqa: RET504


# Initialize the Dash app
app = dash.Dash(__name__)


app.layout = html.Div(
    [
        # html.H1("Caltrans"),
        # Container for the controls
        html.Div(
            [
                # Each control (and its label) is wrapped in its own Div
                html.Div(
                    [
                        html.H3("Date Range"),
                        dcc.DatePickerRange(
                            id="date-picker-range",
                            start_date="2023-01-01",
                            end_date="2023-02-01",
                            display_format="YYYY-MM-DD",
                            min_date_allowed="2022-01-01",
                            max_date_allowed="2023-12-31",
                        ),
                    ],
                    style={
                        "margin": "10px",
                        "display": "flex",
                        "flex-direction": "column",
                        "justify-content": "flex-end",
                    },
                ),  # Adjust margin as needed
                html.Div(
                    [
                        html.H3("Metric"),
                        dcc.Dropdown(
                            [
                                "FLOW_1_COUNT",
                                "FLOW_1_AVG",
                                "FLOW_1_Pct_Expected_Obs",
                                "FLOW_2_COUNT",
                                "FLOW_2_AVG",
                                "FLOW_2_Pct_Expected_Obs",
                                "FLOW_3_COUNT",
                                "FLOW_3_AVG",
                                "FLOW_3_Pct_Expected_Obs",
                                "FLOW_4_COUNT",
                                "FLOW_4_AVG",
                                "FLOW_4_Pct_Expected_Obs",
                                "OCCUPANCY_1_COUNT",
                                "OCCUPANCY_1_AVG",
                                "OCCUPANCY_1_Pct_Expected_Obs",
                                "SPEED_1_COUNT",
                                "SPEED_1_AVG",
                                "SPEED_1_Pct_Expected_Obs",
                            ],
                            "FLOW_1_AVG",
                            id="plot-value",
                        ),
                    ],
                    style={
                        "margin": "10px",
                        "width": "200px",
                        "display": "flex",
                        "flex-direction": "column",
                        "justify-content": "flex-end",
                    },
                ),
                html.Div(
                    [
                        dcc.Dropdown(
                            id="null-toggle",
                            options=[
                                {
                                    "label": "Show Only Null Values",
                                    "value": "SHOW_NULL",
                                },
                                {
                                    "label": "Exclude Null Values",
                                    "value": "EXCLUDE_NULL",
                                },
                                {"label": "Show All", "value": "SHOW_ALL"},
                            ],
                            value="SHOW_ALL",  # Default value
                        )
                    ],
                    style={"margin": "10px", "width": "200px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Min Map Value:"),
                                dcc.Input(
                                    id="min_value_input",
                                    type="number",
                                    value=0,
                                    style={"marginRight": "10px", "width": "60px"},
                                ),
                            ],
                            style={"display": "inline-block", "width": "120px"},
                        ),
                        html.Div(
                            [
                                html.Label("Max Map Value:"),
                                dcc.Input(
                                    id="max_value_input",
                                    type="number",
                                    value=100,
                                    style={"width": "60px"},
                                ),
                            ],
                            style={"display": "inline-block", "width": "120px"},
                        ),
                    ],
                    style={"padding": 20, "width": "120px", "marginRight": "60px"},
                ),
                html.Div(
                    [
                        # html.Div(id='average_value', style={'margin': '10px', 'fontSize': 20}),  # Adjust styling as needed
                        # html.Div(id='null_percentage', style={'margin': '10px', 'fontSize': 20}),  # Adjust styling as needed
                        html.Div(id="tile1-placeholder"),
                        html.Div(id="tile2-placeholder"),
                        html.Div(id="tile3-placeholder"),
                        html.Div(id="tile4-placeholder"),
                        html.Div(id="tile5-placeholder"),
                    ],
                    style={
                        "marginLeft": "60px",
                        "marginRight": "5px",
                        "marginTop": "0px",
                        "marginBottom": "0px",
                        "display": "flex",
                        "justify-content": "flex-end",
                        "height": "100px",
                    },
                ),
            ],
            style={
                "display": "flex",
                "justify-content": "start",
                "align-items": "flex-end",
            },
        ),  # This line makes the layout horizontal
        html.Div(
            [
                dcc.Graph(id="overall_map"),
                dcc.Graph(id="time-series-plot"),
                dcc.Graph(id="indiv_obs_time_series_plot"),
            ],
            style={
                "display": "flex",
                "justify-content": "start",
                "align-items": "flex-start",
            },
        ),
        html.Div(
            [
                html.H3("Color Scale Max Percentile"),
                dcc.Input(
                    id="color_range_max_percentile", type="number", value=0.95, size="3"
                ),
            ],
            style={
                "margin": "10px",
                "width": "200px",
                "display": "flex",
                "flex-direction": "column",
                "justify-content": "flex-end",
                "marginTop": "200px",
            },
        ),
        html.Div(
            [
                html.Label("Min Color:"),
                daq.ColorPicker(
                    id="min-color-picker", value={"hex": "#FFC639"}, size=140
                ),
                html.Label("Max Color:"),
                daq.ColorPicker(
                    id="max-color-picker", value={"hex": "#0079D4"}, size=140
                ),  # ,
                # html.Label('Null Color:'),
                # daq.ColorPicker(id='null-color-picker', value={'hex': '#888888'}),
            ],
            style={
                "margin": "10px",
                "display": "flex",
                "justify-content": "start",
                "align-items": "flex-end",
            },
        ),
    ]
)


# Callback for updating the map based on selected date range
@app.callback(
    Output("overall_map", "figure"),
    Input("date-picker-range", "start_date"),
    Input("date-picker-range", "end_date"),
    Input("plot-value", "value"),
    Input("color_range_max_percentile", "value"),
    Input("min-color-picker", "value"),
    Input("max-color-picker", "value"),
    Input("null-toggle", "value"),
    Input("min_value_input", "value"),
    Input("max_value_input", "value"),
)
def update_graph(  # noqa: D417
    start_date,
    end_date,
    agg_col,
    color_range_max_percentile,
    min_color,
    max_color,
    null_toggle,
    min_value,
    max_value,
):
    """
    Update the map with the given parameters.

    Parameters
    ----------
    start_date (str): The start date for filtering the data.
    end_date (str): The end date for filtering the data.
    agg_col (str): The column to be aggregated.
    color_range_max_percentile (float): The percentile value for determining the maximum color range.
    min_color (dict): The dictionary containing the minimum color value in hexadecimal format.
    max_color (dict): The dictionary containing the maximum color value in hexadecimal format.
    null_toggle (str): The toggle option for handling null values.
    min_value (float): The minimum value for filtering the aggregated data.
    max_value (float): The maximum value for filtering the aggregated data.

    Returns
    -------
    fig (plotly.graph_objs._figure.Figure): The updated plotly figure.

    """
    print(f"updating plot... start_date = {start_date},end_date = {end_date} ")

    filtered_df = pd_station_agg_stats_by_day.query(
        "`SAMPLE_DATE` >= @start_date and `SAMPLE_DATE` <= @end_date"
    )

    # Group by ID and aggregate multiple columns
    aggregated_df = filtered_df.groupby("ID").agg({agg_col: "mean"}).reset_index()

    aggregated_df_filtered = aggregated_df[
        ((aggregated_df[agg_col] >= min_value) & (aggregated_df[agg_col] <= max_value))
        | (aggregated_df[agg_col].isnull())
    ]

    # I do this for the subsequent join. Perhaps it would be more performant to join on the column instead of index?
    aggregated_df_filtered.set_index("ID", inplace=True)

    aggregated_df_filtered[agg_col] = (
        aggregated_df_filtered[agg_col].astype("float32").round(2).astype("float16")
    )

    pd_station_metadata_w_agg_results = pd_station_metadata.join(
        aggregated_df_filtered, how="inner", rsuffix="r"
    ).reset_index()

    # print(pd_station_metadata_w_agg_results.head)

    # Handle null value display based on the dropdown selection
    if null_toggle == "SHOW_NULL":
        pd_station_metadata_w_agg_results = pd_station_metadata_w_agg_results[
            pd_station_metadata_w_agg_results[agg_col].isnull()
        ]
    elif null_toggle == "EXCLUDE_NULL":
        pd_station_metadata_w_agg_results = pd_station_metadata_w_agg_results[
            pd_station_metadata_w_agg_results[agg_col].notnull()
        ]

    # Calculate the 95th percentile for the selected metric
    color_range_max_value = aggregated_df[agg_col].quantile(color_range_max_percentile)

    # Create the figure
    fig = px.scatter_mapbox(
        pd_station_metadata_w_agg_results,
        lat="LATITUDE",
        lon="LONGITUDE",
        hover_name="NAME",
        hover_data={
            "FWY": True,
            "DIR": True,
            "ID": True,
            agg_col: ":.2f",
            "TYPE": True,
            "LANES": True,
        },
        custom_data="ID",
        color=agg_col,
        zoom=6,
        height=900,
        width=780,
        color_continuous_scale=[(0, min_color["hex"]), (1, max_color["hex"])],
        range_color=[0, color_range_max_value],
    )

    fig.update_layout(mapbox_style="carto-positron")
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    fig.update_layout(transition_duration=500)
    print("plot update complete")

    return fig


@app.callback(
    [Output("min_value_input", "value"), Output("max_value_input", "value")],
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("plot-value", "value"),
    ],  # ,
    # prevent_initial_call=True  # Prevents the callback from firing upon initial load
)
def update_default_min_max(start_date, end_date, agg_col):  # noqa: D417
    """
    Update the default minimum and maximum values based on the selected date range and aggregation column.

    Parameters
    ----------
    start_date (str): The start date of the selected date range.
    end_date (str): The end date of the selected date range.
    agg_col (str): The aggregation column to calculate the minimum and maximum values.

    Returns
    -------
    tuple: A tuple containing the minimum and maximum values.

    """
    # Filter the DataFrame based on the selected date range
    filtered_df = pd_station_agg_stats_by_day.query(
        "`SAMPLE_DATE` >= @start_date and `SAMPLE_DATE` <= @end_date"
    )

    # Calculate the min and max values based on the selected aggregation column
    if agg_col in filtered_df:
        min_val = filtered_df[agg_col].min()
        max_val = filtered_df[agg_col].max()
        # round for display.
        min_val = math.floor(min_val * 10) / 10
        max_val = math.ceil(max_val * 10) / 10
    else:
        min_val = 0
        max_val = 10000

    return min_val, max_val


@app.callback(
    Output("time-series-plot", "figure"),
    [
        Input("overall_map", "selectedData"),
        Input("plot-value", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
    ],
)
def update_time_series(selected_data, agg_col, start_date, end_date):
    """
    Update the time series plot based on the selected data.

    Args:
    ----
        selected_data (dict): The selected data from the plotly graph.
        agg_col (str): The column to aggregate the data on.
        start_date (str): The start date for filtering the data.
        end_date (str): The end date for filtering the data.

    Returns:
    -------
        fig (plotly.graph_objs.Figure): The updated figure for the time series plot.
    """
    if selected_data is None or len(selected_data["points"]) == 0:
        # Display a message
        return {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": "No map data selected",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 20},
                    }
                ],
            },
        }

    max_stations_to_plot = 5
    if len(selected_data["points"]) >= max_stations_to_plot:
        # Display a message
        return {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": f"Select {max_stations_to_plot} or fewer sensors for plotting.",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 20},
                    }
                ],
            },
        }

    # Extract the IDs of selected points
    selected_ids = [
        point["customdata"][0] for point in selected_data["points"]
    ]  # Adjust according to your data structure
    print(selected_ids)
    # Filter the data for the selected IDs
    # filtered_data = pd_station_agg_stats_by_day[(pd_station_agg_stats_by_day['ID'].isin(selected_ids))
    #                                                  & (pd_station_agg_stats_by_day['SAMPLE_DATE'] >= start_date)
    #                                                  & (pd_station_agg_stats_by_day['SAMPLE_DATE'] <= end_date)]
    filtered_data = pd_station_agg_stats_by_day.query(
        "`ID` in @selected_ids and `SAMPLE_DATE` >= @start_date and `SAMPLE_DATE` <= @end_date"
    )

    # filtered_data.set_index('ID',inplace = True)

    # filtered_data_with_station_metadata = pd_station_metadata.join(filtered_data, how = "inner",rsuffix = "r").reset_index()

    # ensure the facets are plotted in the same order as selected_ids
    # Convert 'ID' to a categorical column with specified order
    filtered_data["ID"] = pd.Categorical(
        filtered_data["ID"], categories=selected_ids, ordered=True
    )

    # Sort the DataFrame by the 'ID' column
    filtered_data_sorted = filtered_data.sort_values(by="ID", ascending=False)
    SMALL_VALUE = 0.1

    filtered_data_sorted["Display_Value"] = filtered_data_sorted[agg_col].apply(
        lambda x: SMALL_VALUE if x == 0 else x
    )
    filtered_data_sorted["Day_of_Week"] = filtered_data_sorted[
        "SAMPLE_DATE"
    ].dt.day_name()

    # filtered_data_sorted['Hover_Text'] = filtered_data_sorted[agg_col].apply(lambda x: f"Value: {x}")

    # Create a facet plot for each selected ID
    fig = px.bar(
        filtered_data_sorted,
        x="SAMPLE_DATE",
        y="Display_Value",
        facet_col="ID",
        facet_col_wrap=1,
        height=700,
        width=600,
        title=f"Daily Aggregations - {start_date}→{end_date} ({agg_col})",
        template="plotly_white",
        hover_data={"Display_Value": False, agg_col: ":.2f", "Day_of_Week": True},
    )
    print([filtered_data_sorted[agg_col].max(), 5])
    print(min([filtered_data_sorted[agg_col].max(), 5]))
    fig.update_yaxes(
        range=[0, max([filtered_data_sorted[agg_col].max(), 5])], title_text=""
    )

    # Iterate over each facet and set a unique title
    for i, id in enumerate(selected_ids, start=1):
        id_metadata_row = pd_station_metadata.loc[id]
        name = id_metadata_row["NAME"]
        fwy = id_metadata_row["FWY"]
        direction = id_metadata_row["DIR"]
        title_str = name + " (" + fwy + " Fwy, " + direction + ", ID = " + str(id) + ")"
        fig["layout"]["annotations"][i - 1].update(text=title_str)

    # Adjust layout if necessary (e.g., to make the subplots more readable)
    fig.update_traces(marker_line_width=0, marker_color="#046B99")

    # fig.update_layout(transition_duration=500)
    print("time series update complete...")

    return fig


@app.callback(
    Output("indiv_obs_time_series_plot", "figure"),
    [
        Input("time-series-plot", "clickData"),
        Input("plot-value", "value"),
        Input("overall_map", "selectedData"),
    ],
    # Input('date-picker-range', 'start_date'),
    # Input('date-picker-range', 'end_date'),
)
def update_indiv_obs_time_series(click_data, agg_col, map_selected_data):
    """
    Update the individual observation time series plot based on the selected data.

    Args:
    ----
        click_data (dict): The clickData from the individual observation map.
        agg_col (str): The column to aggregate the data on.
        map_selected_data (pandas.DataFrame): The data selected on the map.

    Returns:
    -------
        dict: The updated figure for the individual observation time series plot.
    """
    if (
        click_data is None
        or len(click_data["points"]) == 0
        or map_selected_data is None
        or len(map_selected_data["points"]) == 0
    ):
        # Display a message
        return {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": "Select Day for Plotting on\nTime-Series Bar Graph",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 20},
                    }
                ],
            },
        }
    max_stations_to_plot = 5
    if len(map_selected_data["points"]) >= max_stations_to_plot:
        # Display a message
        return {
            "data": [],
            "layout": {
                "xaxis": {"visible": False},
                "yaxis": {"visible": False},
                "annotations": [
                    {
                        "text": f"Select {max_stations_to_plot} or fewer sensors for plotting.",
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                        "font": {"size": 20},
                    }
                ],
            },
        }
    # print(selected_data)
    # print(selected_data['range'])
    # x_key = next((key for key in selected_data['range'] if key.startswith('x')), None)
    # timestamp_strings = selected_data['range'][x_key]
    # date_strings = [ts.split(' ')[0] for ts in timestamp_strings]
    selected_ids = [
        str(point["customdata"][0]) for point in map_selected_data["points"]
    ]

    # print(date_strings)
    # print(selected_ids)
    string_id_list = ", ".join(selected_ids)
    print(string_id_list)
    print(click_data["points"][0])
    plot_date = click_data["points"][0]["x"]
    var_string_dict = {
        "FLOW_1_COUNT": "COUNT(FLOW_1) AS FLOW_1_COUNT",
        "FLOW_1_AVG": "AVG(FLOW_1) AS FLOW_1_AVG",
        "FLOW_1_Pct_Expected_Obs": "COUNT(FLOW_1) AS FLOW_1_Pct_Expected_Obs",
        "FLOW_2_COUNT": "COUNT(FLOW_2) AS FLOW_2_COUNT",
        "FLOW_2_AVG": "AVG(FLOW_2) AS FLOW_2_AVG",
        "FLOW_2_Pct_Expected_Obs": "COUNT(FLOW_2) AS FLOW_2_Pct_Expected_Obs",
        "FLOW_3_COUNT": "COUNT(FLOW_3) AS FLOW_3_COUNT",
        "FLOW_3_AVG": "AVG(FLOW_3) AS FLOW_3_AVG",
        "FLOW_3_Pct_Expected_Obs": "COUNT(FLOW_3) AS FLOW_3_Pct_Expected_Obs",
        "FLOW_4_COUNT": "COUNT(FLOW_4) AS FLOW_4_COUNT",
        "FLOW_4_AVG": "AVG(FLOW_4) AS FLOW_4_AVG",
        "FLOW_4_Pct_Expected_Obs": "COUNT(FLOW_4) AS FLOW_4_Pct_Expected_Obs",
        "OCCUPANCY_1_COUNT": "COUNT(OCCUPANCY_1) AS OCCUPANCY_1_COUNT",
        "OCCUPANCY_1_AVG": "AVG(OCCUPANCY_1) AS OCCUPANCY_1_AVG",
        "OCCUPANCY_1_Pct_Expected_Obs": "COUNT(OCCUPANCY_1) AS OCCUPANCY_1_Pct_Expected_Obs",
        "SPEED_1_COUNT": "COUNT(SPEED_1) AS SPEED_1_COUNT",
        "SPEED_1_AVG": "AVG(SPEED_1) AS SPEED_1_AVG",
        "SPEED_1_Pct_Expected_Obs": "COUNT(SPEED_1) AS SPEED_1_Pct_Expected_Obs",
    }
    print(var_string_dict[agg_col])
    # WHERE SAMPLE_DATE BETWEEN '{date_strings[0]}' AND '{date_strings[1]}' AND ID IN ({string_id_list})
    #
    sql_str = f"""SELECT
        DATEADD(SECOND, -MOD(SECOND(SAMPLE_TIMESTAMP), 30), SAMPLE_TIMESTAMP) AS rounded_timestamp,
        ID,
        {var_string_dict[agg_col]}
    FROM RAW_DEV.CLEARINGHOUSE.STATION_RAW
    WHERE SAMPLE_DATE = '{plot_date}' AND ID IN ({string_id_list})
    GROUP BY 1,2"""

    print(sql_str)

    result = ibis_con.sql(sql_str).execute()

    print(result.head())
    # df = result.fetchall()
    # print(df.head)
    # Create a facet plot for each selected ID

    # ensure the facets are plotted in the same order as selected_ids
    # Convert 'ID' to a categorical column with specified order
    result["ID"] = pd.Categorical(result["ID"], categories=selected_ids, ordered=True)

    # Sort the DataFrame by the 'ID' column
    result_sorted = result.sort_values(by="ID", ascending=False)

    SMALL_VALUE = 0.1
    result_sorted["Display_Value"] = result_sorted[agg_col.upper()].apply(
        lambda x: SMALL_VALUE if x == 0 else x
    )

    fig = px.bar(
        result_sorted,
        x="ROUNDED_TIMESTAMP",
        y=agg_col.upper(),
        facet_col="ID",
        facet_col_wrap=1,
        height=700,
        width=600,
        template="plotly_white",
        title=f"30 Second Period Aggregation - 24 hrs {plot_date} ({agg_col})",
        hover_data={"Display_Value": False, agg_col: ":.2f"},
    )
    x_axis_range = [plot_date + " 00:00:00", plot_date + " 23:59:59"]

    fig.update_xaxes(range=x_axis_range)
    fig.update_yaxes(range=[0, max([result[agg_col.upper()].max(), 5])], title_text="")

    # Iterate over each facet and set a unique title
    for i, id in enumerate(selected_ids, start=1):
        id_metadata_row = pd_station_metadata.loc[int(id)]
        name = id_metadata_row["NAME"]
        fwy = id_metadata_row["FWY"]
        direction = id_metadata_row["DIR"]
        title_str = name + " (" + fwy + " Fwy, " + direction + ", ID = " + str(id) + ")"
        fig["layout"]["annotations"][i - 1].update(text=title_str)

    fig.update_traces(marker_line_width=0, marker_color="#046B99")
    print("time series 2 update complete...")

    return fig


def format_number_for_dashboard(number):
    """
    Format a number for display in a dashboard.

    Args:
    ----
        number (int or float): The number to be formatted.

    Returns:
    -------
        str: The formatted number.

    """
    if number >= 1_000_000_000:  # Billions  # noqa: PLR2004
        return f"{number / 1_000_000_000:.1f} bil"
    elif number >= 1_000_000:  # Millions  # noqa: PLR2004
        return f"{number / 1_000_000:.1f} mil"
    elif number >= 1_000:  # Thousands  # noqa: PLR2004
        return f"{number / 1_000:.1f}k"
    else:
        return str(number)  # Handle numbers less than 1000


@app.callback(
    [
        Output("tile1-placeholder", "children"),
        Output("tile2-placeholder", "children"),
        Output("tile3-placeholder", "children"),
        Output("tile4-placeholder", "children"),
        Output("tile5-placeholder", "children"),
    ],
    [
        Input("plot-value", "value"),
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("null-toggle", "value"),
    ],
)
def update_tiles(agg_col, start_date, end_date, null_toggle):
    """
    Update the tiles on the EDA dashboard based on the selected parameters.

    Args:
    ----
        agg_col (str): The column name for which the tiles are updated.
        start_date (str): The start date of the selected date range.
        end_date (str): The end date of the selected date range.
        null_toggle (str): The null value display option.

    Returns:
    -------
        tuple: A tuple containing the layouts for the updated tiles.

    """
    # Filter the DataFrame for the selected date range
    filtered_df = pd_station_agg_stats_by_day[
        (pd_station_agg_stats_by_day["SAMPLE_DATE"] >= start_date)
        & (pd_station_agg_stats_by_day["SAMPLE_DATE"] <= end_date)
    ]

    aggregated_df = filtered_df.groupby("ID").agg({agg_col: "mean"}).reset_index()

    # Handle null value display based on the dropdown selection
    if null_toggle == "SHOW_NULL":
        filtered_df = filtered_df[filtered_df[agg_col].isnull()]
        aggregated_df = aggregated_df[aggregated_df[agg_col].isnull()]
    elif null_toggle == "EXCLUDE_NULL":
        filtered_df = filtered_df[filtered_df[agg_col].notnull()]
        aggregated_df = aggregated_df[aggregated_df[agg_col].notnull()]

    # Calculate the percentage of null values for the selected metric
    total_sensor_count = aggregated_df.shape[0]
    null_count = aggregated_df[agg_col].isnull().sum()
    null_percentage = (null_count / total_sensor_count) * 100

    # Calculate the average value for the selected metric
    average_value = filtered_df[agg_col].astype("float32").mean()

    # Calculate the percentage of all zero values for the selected metric
    all_zero_count = (aggregated_df[agg_col] == 0).sum()
    all_zero_percentage = (all_zero_count / total_sensor_count) * 100

    # calculate number of observations. Use the count column.
    pattern = r"\b([^\d_]+_\d+)"

    selected_field_count_column = re.match(pattern, agg_col).group(1) + "_COUNT"
    ttl_obs = format_number_for_dashboard(
        filtered_df[selected_field_count_column].sum()
    )

    sensor_count = format_number_for_dashboard(len(filtered_df["ID"].unique()))

    # Create the tile layouts
    tile1_layout = create_number_tile(sensor_count, "Sensor Count")
    tile2_layout = create_number_tile(ttl_obs, "Total Observations")
    tile3_layout = create_number_tile(
        f"{null_percentage:.1f}%", "Pct Sensors w/ No Data"
    )
    tile4_layout = create_number_tile(
        f"{all_zero_percentage:.1f}%", "Pct Sensors w/ All Zeros"
    )
    tile5_layout = create_number_tile(f"{average_value:.2f}", "Statewide Avg")
    return tile1_layout, tile2_layout, tile3_layout, tile4_layout, tile5_layout


# Run the app
if __name__ == "__main__":
    app.run(debug=True)

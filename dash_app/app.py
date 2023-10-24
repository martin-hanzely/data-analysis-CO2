from dash import Dash, html, dcc
import plotly.express as px

from data.loaders.local_csv_loader import LocalCSVLoader


app = Dash(__name__)

_l = LocalCSVLoader()
df = _l.retrieve_dataframe()

figure = px.line(df, x="year", y="value", title=f"CO2 in ppm (parts per million)")


app.layout = html.Div([
    html.H1(children="Data analysis CO2", style={"textAlign": "center"}),
    dcc.Graph(id="graph-content", figure=figure),
    html.P("""\
    SOURCE:
    Lan, X., J. W. Mund, A. M. Crotwell, M. J. Crotwell, E. Moglia,
    M. Madronich, D. Neff and K. W. Thoning (2023), Atmospheric Carbon Dioxide Dry
    Air Mole Fractions from the NOAA GML Carbon Cycle Cooperative Global
    Air Sampling Network, 1968-2022, Version: 2023-08-28, https://doi.org/10.15138/wkgj-f215
    """),
])

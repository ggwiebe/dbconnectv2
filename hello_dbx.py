# from dash import Dash, dash_table
# from databricks.connect.session import DatabricksSession as SparkSession
# from databricks.sdk.core import Config

# config = Config(profile="plotly", cluster_id="0324-195708-nlv2jrsc")
# spark = SparkSession.builder.sdkConfig(config).getOrCreate()
# df = spark.table("samples.nyctaxi.trips").limit(10).toPandas()

# app = Dash(__name__)
# app.layout = dash_table.DataTable(df.to_dict('records'), [{"name": i, "id": i} for i in df.columns])
# if __name__ == '__main__':
#     app.run_server(debug=True)


# 1. Imports & Session
from databricks.connect import DatabricksSession

# session = DatabricksSession.builder.getOrCreate()

# 2. 
SPARK_REMOTE="sc://adb-5854717212043428.8.azuredatabricks.net:443/;token=XXXX;x-databricks-cluster-id=<cluster id>"

# 3.
spark = DatabricksSession.builder.remote(
    host="adb-5854717212043428.8.azuredatabricks.net",
    cluster_id="0324-195708-nlv2jrsc",
    token="XXXX"
).getOrCreate()

# df = session.range(1, 10)
# df.show()

hosp_df = spark.table('ggw.covid.vaccinations_flat')
hosp_df.show(20)

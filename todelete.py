from influxdb_client_3 import InfluxDBClient3, Point

# Set up your client
client = InfluxDBClient3(
    host="localhost",
    port=8181,
    token="",    # Leave blank for no auth (default OSS dev)
    org="my-org" # Optional, default org if you have one
)

# Create a database (namespace) if needed
db_name = "testdb"

# Write a point
point = Point("temperature").tag("location", "office").field("value", 22.7)
client.write(database=db_name, record=point)

# Query (SQL)
result = client.query(
    f'SELECT * FROM "{db_name}"."temperature"'
)
for row in result:
    print(row)
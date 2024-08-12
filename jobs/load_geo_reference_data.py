"""Load geospatial reference datasets."""
from jobs.utils.geo import gdf_from_esri_feature_service
from jobs.utils.snowflake import gdf_to_snowflake, snowflake_connection_from_environment

SCHEMA = "GEO_REFERENCE"
FEATURE_SERVICES = {
    "SHN_LINES": (
        "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHhighway/SHN_Lines/FeatureServer/0/query"
    ),
    "DISTRICTS": (
        "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/District_Tiger_Lines/FeatureServer/0/query"
    ),
    "COUNTY_BOUNDARIES": (
        "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/County_Boundaries/FeatureServer/0/query"
    ),
}

if __name__ == "__main__":
    snowflake_conn = snowflake_connection_from_environment(schema=SCHEMA)
    snowflake_conn.cursor().execute(
        f'CREATE SCHEMA IF NOT EXISTS "{snowflake_conn.schema}"'.upper()
    )

    try:
        for name, url in FEATURE_SERVICES.items():
            print(f"Loading {name}")
            gdf = gdf_from_esri_feature_service(url)
            gdf_to_snowflake(gdf, snowflake_conn, name)
    finally:
        snowflake_conn.close()

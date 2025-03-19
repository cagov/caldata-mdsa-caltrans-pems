import json
import os

from snowflake import snowpark
from snowflake.snowpark import types


def unload_as_geojson(
    session: snowpark.Session, table_name: str, file_name: str, stage_path: str
) -> list:
    import geopandas
    import pandas
    import shapely

    dataframe = session.table(table_name)

    geography_columns = []
    date_columns = []
    for field in dataframe.schema:
        if isinstance(field.datatype, types.GeographyType):
            geography_columns.append(field.name)
        if isinstance(field.datatype, types.DateType):
            date_columns.append(field.name)
    assert len(geography_columns)  # Error if no geography columns found

    pdf = dataframe.to_pandas()

    # Convert to GeoDataFrame, ensure all geography columns are geometry types,
    # ensure we are using SRID 4326 (lat/lon)
    gdf = geopandas.GeoDataFrame(pdf)
    for column in geography_columns:
        gdf[column] = gdf[column].apply(
            lambda s: shapely.geometry.shape(json.loads(s)) if s else pandas.NA
        )
    for column in date_columns:
        gdf[column] = pandas.to_datetime(gdf[column])
    gdf = gdf.set_geometry(geography_columns[0]).set_crs("EPSG:4326")

    fname = f"/tmp/{file_name}.geojson"
    gdf.to_file(fname, driver="GeoJSON")
    return session.file.put(
        fname,
        stage_path,
        auto_compress=False,
        overwrite=True,
    )


def write_iceberg_metadata(session: snowpark.Session, database: str, stage: str):
    # Your code goes here, inside the "main" handler.
    iceberg_tables = (
        session.sql(
            f"""
                show iceberg tables in database {database}
            """
        )
        .collect_nowait()
        .to_df()
        .to_pandas()
    )

    def _get_meta(record):
        identifier = ".".join([database, record["schema_name"], record["name"]])
        result = (
            session.sql(
                f"""
            select system$get_iceberg_table_information('{identifier}')
        """
            )
            .collect_nowait()
            .result()
        )
        return json.loads(result[0][0])["metadataLocation"]

    iceberg_tables = iceberg_tables.assign(
        schema=iceberg_tables["schema_name"],
        metadata=iceberg_tables.apply(_get_meta, axis=1),
        path=iceberg_tables["base_location"],
    )[["name", "schema", "path", "metadata"]]
    fname = "/tmp/current_table_versions.json"
    iceberg_tables.to_json(fname, orient="records")
    return session.file.put(
        fname,
        f"{stage}/iceberg",
        auto_compress=False,
        overwrite=True,
    )


if __name__ == "__main__":
    from jobs.utils.snowflake import snowflake_connection_from_environment

    # Create the snowpark session
    conn = snowflake_connection_from_environment(
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="PUBLIC",
    )
    session = snowpark.Session.builder.configs({"connection": conn}).create()

    # Ensure an internal stage exists for storing our stored procedure.
    _ = session.sql(
        f"""CREATE STAGE IF NOT EXISTS {conn.database}.PUBLIC.PEMS_MARTS_INTERNAL"""
    ).collect()

    # Register the procedures!
    session.sproc.register(
        func=unload_as_geojson,
        name="unload_as_geojson",
        return_type=types.VariantType(),
        input_types=[types.StringType(), types.StringType(), types.StringType()],
        packages=["snowflake-snowpark-python", "geopandas==0.14.2", "shapely"],
        replace=True,
        source_code_display=True,
        is_permanent=True,
        stage_location="@PEMS_MARTS_INTERNAL",
    )

    session.sproc.register(
        func=write_iceberg_metadata,
        name="write_iceberg_metadata",
        return_type=types.VariantType(),
        input_types=[types.StringType(), types.StringType()],
        packages=["snowflake-snowpark-python"],
        replace=True,
        source_code_display=True,
        is_permanent=True,
        stage_location="@PEMS_MARTS_INTERNAL",
    )

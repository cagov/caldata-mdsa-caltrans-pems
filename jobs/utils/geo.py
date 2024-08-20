from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import geopandas
import pandas


def gdf_from_esri_feature_service(url):
    """
    Load an Esri Feature Service to a GeoDataFrame.

    Given a URL to an Esri Feature Service, download the features
    as GeoJSON, and put them into a GeoDataFrame.
    """
    parsed = urlparse(url)

    # Ensure we are using the query endpoint of the feature service
    if not parsed.path.endswith("/query"):
        parsed = parsed._replace(path=parsed.path + "/query")

    # Keep grabbing data using the resultOffset until there is no more left
    offset = 0
    gdfs = []
    while True:
        queries = dict(parse_qsl(parsed.query))
        queries.update(
            {
                "where": "1=1",  # Ensure all rows
                "f": "geojson",  # Ensure GeoJSON
                "outFields": "*",  # Ensure all columns
                "resultOffset": str(offset),  # offset the start
                "returnGeometry": "true",  # Yes we want geometries
            }
        )
        offset_url = urlunparse(parsed._replace(query=urlencode(queries)))

        gdf = geopandas.read_file(offset_url, driver="GeoJSON")
        if len(gdf) == 0:
            break

        gdfs.append(gdf)
        offset += len(gdf)
    return pandas.concat(gdfs).reset_index(drop=True)

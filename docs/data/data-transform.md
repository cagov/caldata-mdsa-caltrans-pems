# Data Transformation Process

This page details the data transformation processes for the PeMS Modernization
Project. Data from the sources identified below are transformed from their raw
format into calculated performance metrics that are loaded into tables and files
that can used for reporting, visualizations, and/or be downloaded by users.

There are three primary data sources that the PeMS Modernization Project
uses for analysis, reporting and visualizations:

1. PeMS receives field collected detector raw 30-second data from Intelligent
   Transportation System (ITS) Vehicle Detector Stations (VDS) relayed by the District TMC's
2. Caltrans Districts provide station configuration information primarily through manual
   uploads of XML files through the PeMS website.
   (i.e. Location, identification ID's, and other metadata for stations/detectors/controllers)
3. Freeway configuration information for visualizations is obtained from the Caltrans Linear Referencing
   System is obtained from the [Caltrans GIS Data Open Portal website](https://gisdata-caltrans.opendata.arcgis.com/)
   (i.e. district, county, route, postmiles, etc.)

Caltrans HQ staff are currently in the process of building data pipelines
to bring in other datasources including CHP incident and Lane Closure
System data.

## Transformation Processes

### Vehicle Detector Stations (VDS) Configuration Transformation:

District staff uploads station configuration data via XML files which are processed and transformed into
various tables located within the Oracle data warehouse schemas. A data pieline takes the station configuration data
from Oracle and loads them into our AWS/Snowflake data warehouse for further processing into staging data models.
The station configuration data is later combined with the raw 30-second data in the following section.

### 30-Second Raw Data Transformation

1. The 30-second raw data is received from district TMC's and transformed into tables that are divided by district. The raw
   data is received in a number of different formats including via SQLnet, XML feed over TCP, and in others raw controller
   packets via RPC.
2. A data pipeline takes the data from the Oracle data lake tables and places it in the Snowflake RAW database. Additional
   details and a flow diagram are included here: [data loading documentation](https://cagov.github.io/caldata-mdsa-caltrans-pems/data-loading/)
3. The raw data is then transformed in staging data models and remains aggregated at the station level. The VDS data is also
   transformed in staging data models.
4. The 30-second raw data is then aggregated to 5 minute samples per lane in our intermediate data model. For detectors that
   do not report sample data or where data is missing during a 5-minute interval. The goal of these data models is to capture
   5-minute intervals for all stations/detectors that are considered active on any given date for all 24 hours.
5. The 5-minute data aggregations are then processed in the following order:

    - If 10 or more samples are reported in a 5-minute timeframe the sum of the flow value is checked to ensure it is between 0
      and the 5-minute max capacity value. If the flow value meet this condition it is used. The average occupancy values are checked
      to ensure they are between 0 and 1. If the occupancy value meets this condition it is used. If a speed value is reported by the
      device the flow weighted speed value is calculated and used.
    - If between 1-9 samples are reported in a 5-minute timeframe the flow data is normalized as if 10 samples were received using the
      following formula: (reported flow ) \* (10 / number of samples reported). The resulting value for the normalized flow is used. The
      average of the reported occupancy values is used, no normalization is done for occupancy or speed. The flow weighted speed value is
      used based on the raw data provided, no normalization is done with speed data.
    - If the flow or occupancy values fail any of the checks above or if the detector is diagnosed as BAD based on the diagnostic tests,
      than the flow, occupancy and speed are imputed. The data models related to imputation are intermediate models contained in the
      IMPUTATION schema.

6. Once the data 5-minute data aggregations for flow, occupancy and speed are completely populated, performance metrics are calculated
   at the detector/lane level. The data is then aggregated to the station level with the following exception: the delay for a station is
   based on the aggregated speed and flow across all lanes. It is NOT the sum of the individual lane delay for a station.
7. For individual lanes and stations, the complete 5-minute data (observed and imputed) is aggregated across multiple temporal
   (hourly, daily, weekly, monthly) aggregations. The intermediate data models that contain the various temporal metrics are contained
   in the PERFORMANCE schema.
8. Detector diagnostic tests are performed on daily data based on the sample data received or not received. Additional details of
   the diagnostic tests performed can be found here: [detector health diagnostics](https://cagov.github.io/caldata-mdsa-caltrans-pems/data/detector-health/)
9. Once the data processing is completed in the intermediate data models, mart data models are built from the intermediate models so
   they can used for reporting on the web or in our visualization tools.The data models in the marts folder are calculated daily and are
   located in the ANALYTICS_PRD database and a copy is also saved to a public S3 bucket.
10. Please note that additioanl details for imputation, bottlenecks and other calculations are included in other sections of this website.

# Detector Health Diagnostics

Vehicle detectors are the main source of data in the system and provide the best source of real-time traffic data
available to Caltrans. Data from the detectors is relayed from the field to District TMC's which ultimatley
makes it's way to the system. The detector data is combined with equipment data
(i.e. detector id, district, county, route, postmile, description, etc.) in order to calculate performance
measures and aggregate them spatially and temporaly. Since the detector data serves as the source for most
of the performance calculations, the resulting performance measures are only as accurate as the underlying data.

The Detector Health Diagnostic checks performed in the system serve multiple purposes:

1.  Ensure detectors and other equipment are functioning properly
2.  Serves as a data quality check against data being reported by detectors
3.  Assists Caltrans staff in troubleshooting and maintaining detectors/stations/controllers
4.  Used as a data quality measure for imputation, bottleneck and other calculations

Caltrans has installed detectors to cover most urban freeways on the State Highway System. The data collected,
however, can contain missing values (also referred to as "holes") or “bad” (incorrect) values that require
data quality analysis to ensure the highest level of data quality is being used to produce reliable results.
The system contains various data quality checks to detect when data is "bad" to ensure the "bad" data is not used
to compute performance measures. When "bad" or missing data is encountered, the system imputes values to fill these
data "holes". The resulting database of complete and reliable data ensures that analyses produce meaningful results.
The system relies on extensive analysis of all of the detectors in the state to help approximate what the true values
would be had the faulty detectors been working properly and sending good data. The ability of the system to adjust
for missing data is a real advantage in using the system for performance measurements.

For every analysis performed in the system, confirming the data quality is essential. The basic idea of the diagnostic tests
are to identify "bad" detectors based on the observed measurements of volume and occupancy over an entire day. We do this
by computing summary statistics from the flow and occupancy measurements every day. The following tests have been tested,
adjusted, and re-adjusted to yield effective results. The algorithms based on the tests have their historical roots in a
paper by Chen, Kwon, Skabardonis and Varaiya (members of the the system original development team). This paper, "Detecting
errors and imputing missing data for single loop surveillance systems," is available on the Internet. The algorithms that
the system uses have been modified from the original version described in the paper as a result of Caltrans’ experience
operating the system for more than a decade.

| Test Number | Detector Types      | Condition                      | Description                                                                                                                                                                                                                                                                                                                         | Diagnostic Test                                                                                                                                                                                                  | Data Used | Diagnostic         |
| ----------- | ------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------------------ |
| 1           | All including Ramps | Never receive any data samples | The system breaks down this condition into five bins based on the communication infrastructure. The first bin indicates that none of the detectors in the same district as the selected detector are reporting data. When a district feed is down all of the data calculated in the system for that day in the district is imputed. | Number of samples received is equal to zero for all detectors in the district.                                                                                                                                   | 30-sec    | District Feed Down |
|             |                     |                                | The second bin indicates that none of the detectors attached to the same communication line as the selected detector are reporting data. Note that information about communication lines is not always available. In this case, this test is omitted.                                                                               | Number of samples received is equal to zero for all detectors attached to the same communication line.                                                                                                           | 30-sec    | Line Down          |
|             |                     |                                | The third bin indicates that none of the detectors attached to the same controller as the selected detector are reporting data. This probably indicates no power at this location or the communication link is broken.                                                                                                              | Number of samples received is equal to zero for all detectors attached to the controller. If communication line information is available, then at least one other controller on the same line is reporting data. | 30-sec    | Controller Down    |
|             |                     |                                | The fourth bin indicates that none of the detectors attached to the same station as the selected detector are reporting data.                                                                                                                                                                                                       | Number of samples received is equal to zero for all detectors attached to the same station.                                                                                                                      | 30-sec    | Station Down       |
|             |                     |                                | The fifth bin indicates that the individual detector is not reporting any data, but other detectors on the same controller are sending samples. This most likely indicates a software configuration error or bad wiring.                                                                                                            | Number of samples received is equal to zero, but other detectors on the same controller are reporting data.                                                                                                      | 30-sec    | No Data            |
| 2           | All including Ramps | Too few data samples           | The system received some samples but not enough to perform diagnostic tests. Other detectors reported more samples (so the data feed did not die).                                                                                                                                                                                  | \# of samples < 60% of the max collected samples during the test period.                                                                                                                                         | 30-sec    | Insufficient Data  |
| 3           | All including Ramps | Zero occupancy or flow         | There are too many samples with an occupancy (non-ramps only) or flow (Ramps only) of zero. The system suspects that the detector card (in the case of standard loop detectors) is off.                                                                                                                                             | Non-Ramps: # zero occ samples > % of the max collected samples during the test period.<br>Ramp: # zero flow samples > % of the max collected samples during the test<br>period.                                  | 30-sec    | Card Off           |
| 4           | All including Ramps | High values                    | There are too many samples with either occupancy above 0% (non-ramps only) or flow above veh/30-sec (Ramps only). The detector is probably stuck on.                                                                                                                                                                                | Non-Ramps: # high occ samples > % of the max collected samples during the test period.<br>Ramp: # high flow samples > % of the max collected samples during the test<br>period.                                  | 30-sec    | High Value         |
| 5           | All excluding Ramps | Flow-Occupancy mismatch        | There are too many samples where the flow is zero and the occupancy is non- zero. This could be caused by the detector hanging on.                                                                                                                                                                                                  | \# of flow-occupancy mismatch samples > % of the max collected samples during the test period.                                                                                                                   | 30-sec    | Intermittent       |
| 6           | All excluding Ramps | Occupancy is constant          | The detector is stuck at some value for some reason. The system knows that occupancy should have some variation over the day. the system count the number of times that the occupancy value is non-zero and repeated from the last sample (is exactly the same as the last sample).                                                 | \# repeated occupancy values > 5-min samples.                                                                                                                                                                    | 5-min     | Constant Occupancy |

The steps that are involved in actually implementing the complete diagnostic algorithm are as follows:

We compute the statistics needed for the above tests over the time period from 5am until 10pm (we don't want to capture the
time period when there are very little vehicles on the freeway anyways) every day. For each of the above tests, we check
the statistics against the predefined thresholds. The threshold values are located in the systems Seed files. If any of the
tests for a detector fails, then the detector is declared "bad" and we stop testing. We record that the detector is declared
as "bad" in the database. Users can subsequently view tables of "bad" detectors. Once a mainline or HOV detector is identified as
"bad", we impute data in order to fill in for the "bad" detector for the same day the detector is diagnosed as "bad".
Note that we do not impute for any ramp detectors. It is important to note that we do not identify individual data samples
as bad or good. We make this determination on a detector-by-detector basis each day.

We perform the diagnostic tests on all detectors in the system including on and off ramps. The types of tests that we
apply are different for each type of detector. For example, most mainline detectors report flow and occupancy whereas ramp
detectors usually only report flow. Hence for ramps we can only do a subset of the tests that we perform for mainline
detectors.

It is important to note that we are attempting to identify "bad" detectors, not bad data samples. The system performs a number
of simple filters on individual data samples just to make sure that they make sense as a measurement (e.g., no values less
than zero, certain values can not be null, flow values do not exceed a maximum values determined by statistical analysis, etc.)
The system does not perform real-time checks.

Detectors can go "bad" or malfunction for many reasons. This is typically an intermittent or recurrent problem that can
require different approaches to properly diagnose and fix. The system devotes a substantial amount of its computing resources
to identifying "bad" detectors and calculating health diagnostics to help users evaluate data quality and to help those
responsible for detector maintenance.

There are many potential points of failure in this process, which can include the physical devices themselves (i.e. hardware,
equipment, device, communications links, etc.) or can also involve non-physical, “human error,” such as making errors in the
configuration of the device such as associating the wrong county, route and postmile with a station, or assigning the wrong
identification number to a station. These human errors can be difficult to diagnose.

## Understanding Errors When No Data is Recieved

The systems detector health diagnostic approach is to assign a failure status to individual detectors. When the system
receives data samples, it can test the samples against the threshold values to determine a number of different types of
errors. But when the system recieves no data samples it's difficult to tell if the detector itself is "bad".
If we know the physical data collection infrastructure, which means knowing which detectors are connected to which stations,
or which stations are connected to which controllers we can focus our diagnostics to the equipment level.

For example, it could be that we aren't receiving any data samples from any detector simply because the FEP is down. For
situations like this we mark the detector as "bad" with a reason of district feed down. This status simply represents that
we didn't receive any data samples in the district for a given date. The reason we don't mark the detector as "good" is due
to how "good" detector data is used in other system calculations. For example, we only perform certain performance metrics such
as bottleneck and congestion analyses where we have "good" detectors. We do not believe it is appropriate to include
imputed detector data in these advanced metrics, especially since the imputation methods used when a district feed is down
are the least robust methods due to a lack of observed data.

As another example, if we know that all of the detectors that are connected to a single station are not sending any data samples,
then it's likely that the issue is related to the station. It could also mean that all of the detectors connected to the station
have issues but that is less likely. If some detectors are reporting samples to a station and other detectors are not reporting
samples then the detectors not reporting samples are the likely source of the issue. If a controller is not recieving any samples
from connected stations there could be no power at the controller, or it could have been damaged in an accident (or removed
during construction).

If we receive samples from some stations connected to a controller but not other stations, then the station is considered to be
down and the loops belonging to those controllers are marked as station down. If we receive samples from some detectors but not
others for a single station then the non-reporting detectors are considered to be down but not the station and are marked as no
data. It should be pointed out that we're assuming that the district feed, controller or stations are the problem if we don't
receive data samples from the detectors underneath them. But it could be that all of the individual loops are broken althought
this is considered unlikely.

In districts where we don't have the physical topology of the data collection infrastructure, or the data collection
infrastructure is just a star the detector staus checks are more limited. District 4 doesn't provide us with their
data collection infrastructure and District 10 has only wireless modems that are sending data back to a centralized point so
not all of the detector status checks are possible.

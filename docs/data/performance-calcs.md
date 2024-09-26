# Performance Measure Calculations

The PeMS Modernization project calculates the following performance measures for every single detector and station location:

Vehicle Miles Traveled (VMT): For a selected period of time and section of the freeway, this value is the sum of the miles of
freeway driven by each vehicle. For a selection of freeway of length L, the number of freeway miles driven is simply the flow
for a period of time multiplied by the length L. For example, the VMT for a single station is:

    VMT = Flow * Station Length

Vehicle Hours Traveled (VHT): For a selected period of time and section of freeway, this is the amount of time spent by all of the
vehicles on the freeway. For a section of freeway of length L, that has F number of cars passing over the detector/station, it is
the amount of time that these cars spent on that link. For example, the VMT for a single station is:

    VHT= Flow * Station Length / Actual Speed

Delay: The amount of additional time spent by the vehicles on a section of road due to congestion. This is the difference between
the travel time at a non-congestion speed (selected by the user) and the current speed. The congestion, or threshold, speed is
usually 35MPH but we have computed the delay for a number of different thresholds to accommodate different definitions of delay.
For example the delay for a single station is:
The formula for this is simply:

    Delay = Flow * (Station Length / Actual Speed - Station Length / Threshold Selected Speed)

It is important to note that delay can never be negative by definition. For delay, you have to specify a threshold which is
considered to be the users definition of congested traffic. While a speed of 35 MPH is commonly used to define congestion we have
calculated delay for threshold values of 35, 40, 45, 50, 55, and 60 MPH.

Q: The sum of the VMT in a spatial and temporal region divided by the sum of the VHT in the same region. For a single location
the interpretation of Q is the average speed although the value can be computed across many different temporal and spatial selections.

    Q = VMT / VHT

Travel Time Index (TTI): The ratio of the average travel time for all users across a section of freeway to the free-flow travel time.
The free-flow speed used in the PeMS Modernization project for this calculation is 60MPH. For our performance measures, the TTI
is simply the free-flow speed divided by Q:

    TTI = 60 / Q

Lost Productivity: The number of lane-mile-hours on the freeway lost due to operating under congested conditions instead of under free-flow
conditions. Similar to delay, the user can select different speed threshold values of 35, 40, 45, 50, 55, and 60 MPH. When the freeway
is in congestion, based on the user selected threshold, the calculation is the ratio between the measured flow and the capacity for this
location. This drop in capacity is due to the fact that the freeway is operating in congested conditions instead of in free-flow. We
then multiply one minus this ratio by the length of the segment to determine the number of equivalent lane-miles-hours of freeway which
this represents. For the capacity we 2076 v/l/h at each location. For example, the formula for Lost Productivity during a 5-minute interval
for a single station is:

    Lost Productivity = 1 - (5-minute Flow / 173 v/l/5-min) * Station Length

It is important to note that if the actual speed is above the user selected threshold a value of 0 will be returned. A Lost Productivity value
will only be calculated if the actual speed is less than the user selected threshold value.

A couple of notes about the calculation of these performance measures:

- All performance are computed at the 5-minute level for each lane and station.
- The measures VMT, VHT and Delay can be summed up temporally and spatially.
- The measures Q and TTI are just ratios of the base performance measures of VMT and VHT.

# G-Factor Speed

The PeMS system has the ability to compute speed for sensors that don't report speed, like single loop detectors. 
If the sensors are reporting speed, like with radar detectors deployed head-on, or double loop detectors, then we can use those speed measurement directly. 
Single loop detectos are prevalent for traffic measurement like occupancy and flow but do not directly measure speed, a critical variable for traffic control and information systems. 
By establishing a relationship between vehicle count, occupancy, and vehicle length, this model proposes a novel method to estimate speed indirectly.

## Methodology
**Statement**: Single loop detectors estimates vehicle speed by dynamically adjusting the assumed vehicle length based on observed traffic conditions, overcoming the limitations of fixed-length assumptions commonly used in other models.
1. **Traffic Data Representation**:
   - Let $N(d, t)$ represent the vehicle count ("flow") and $\rho(d, t)$ the occupancy measured at a loop detector on day $d$ during the time interval $t$.
   - Occupancy is defined as the fraction of time the detector is occupied by vehicles.

2. **Initial Speed Estimation Assumption**:
   - If vehicle speeds are uniform, the occupancy $\rho$ can be represented as:
     $$
     \rho = \frac{\sum_{i=1}^{N} L_i}{v}
     $$
     where $L_i$ is the length of the $i$-th vehicle and $v$ is the uniform speed of all vehicles.

3. **Revising Vehicle Length Assumption**:
   - Recognizing that vehicle lengths vary, they are treated as random variables with a mean $\mu$.
   - The mean vehicle length $\mu$ is not constant and varies with traffic conditions, specifically it is affected by the proportion of trucks to cars at different times.

4. **Estimating Mean Vehicle Length ($\mu_t$)**:
   - During low occupancy, it's assumed that all vehicles travel at a known free-flow speed $v_{FF}$, allowing for an estimation of $\mu_t$ based on observed occupancy and vehicle count:
     ```math
     \mu_t = \frac{v_{FF} \times \rho(d, t)}{N(d, t)}
     ```
   - This formula helps estimate the average vehicle length dynamically, factoring in varying traffic compositions throughout the day.

5. **Final Speed Estimation**:
 - The speed estimation formula adjusts the average vehicle length in the calculation:
   $$
   v \approx \frac{N(d, t) \times \mu_t}{\rho(d, t)}
   $$
   - This method provides a more accurate and less biased speed estimate, especially useful in variable traffic conditions.

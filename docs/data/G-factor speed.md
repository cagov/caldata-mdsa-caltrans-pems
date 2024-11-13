# G-Factor Speed

The PeMS system possesses the ability to compute speed for sensors that don't report speed, like single loop detectors. In fact, even many double loop detectors, which often fail to report speed, depend on estimations. 
Single loop detectos are prevalent for traffic measurement like occupancy and flow but do not directly measure speed. By establishing a relationship between vehicle count, occupancy, and vehicle length, this model proposes a method to estimate speed indirectly.

## Methodology
- **Statement**: Single loop detectors estimates vehicle speed by dynamically adjusting the assumed vehicle length based on observed traffic conditions, overcoming the limitations of fixed-length assumptions commonly used in other models. The estimated vehicle length is called **g-factor**.

- **Procedures**: The steps for estimation of speed from is available via [PeMS Documentation](https://pems.dot.ca.gov/?dnode=Help&content=help_calc#speeds), which we won't duplicate the documentation here.

## STL Decomposition for Comparing Speed Datasets

### Why Speed QC
- **Ambiguity**: We noted that even though the existing PeMS system claimed to follow the procedures of the orginal paper, they have made significant changes to smooth the speed calculation without additional detailed documentations that we can follow. It turns out difficult for us to generate the exact same logic to estimate the speed.
- **Availability**: Through investigations, it seems hard to obtain the datasets of other components, including g-factor and p-factor. They are key parameters to implement a kalman filter for speed smoothing. 
- **Accuracy**: Speed is a fundamental metric in PeMS that directly influences the assessment of downstream performance metrics, such as bottleneck identification, congestion levels and VHT. Before performance metrics QC, it is essential to guarantee the accuracy of speed calculation as a basis.
- **Quantity**: Visual assessments are insufficient to measure the difference.  To accurately identify and measure these variations and adjusted parameters at the detector level, it is essential to adopt quantitative methods.

![Daily Average Speed for Each GP Lane](https://github.com/user-attachments/assets/e96bf5ab-6f7a-4b4c-99c0-48da7be3411d)
![Daily Average Speed for Each GP Lane PeMS Modernization](https://github.com/user-attachments/assets/0c0e51d8-d145-4586-949b-aa34c07cfec4)

### Why Use STL Decomposition?

Seasonal-Trend Decomposition using Loess (STL) is a robust method for decomposing a time series into three additive components:

- **Trend**: The long-term progression of the series.
- **Seasonal**: The repeating short-term cycle in the series.
- **Residual**: The remaining variation after removing trend and seasonal components.

A speed time series $Y_t$ can be expressed as:
```math
Y_t = T_t + S_t + R_t
```
- $T_t$: Trend component.
- $S_t$: Seasonal component.
- $R_t$: Residual component.

```python
def decompose_lane(data, station_lane, dataset_name):
    # Filter data for the lane
    lane_data = data[data['station_lane'] == station_lane].copy()
    lane_data.set_index('timestamp', inplace=True)
    lane_data.sort_index(inplace=True)

    # Ensure sufficient data points for STL
    if len(lane_data) < 288: # Drop lane data if the sample size is less in a day
        return None

    # Set the seasonal period
    seasonal_period = 288 

    # Apply STL decomposition
    stl = STL(lane_data['speed'], period=seasonal_period, robust=True).fit()
    lane_data['trend'] = stl.trend
    lane_data['seasonal'] = stl.seasonal
    lane_data['resid'] = stl.resid

    # Add dataset identifier
    lane_data['dataset'] = dataset_name

    return lane_data
```

### [STL Algorithm](https://www.statsmodels.org/dev/examples/notebooks/generated/stl_decomposition.html)

STL decomposition involves the following steps:

1. **Estimation of the Trend Component**:
   - Smooth the time series data to capture the long-term trend.
2. **Estimation of the Seasonal Component**:
   - Extract repeating patterns by averaging over cycles.
3. **Calculation of the Residual Component**:
   - Subtract the trend and seasonal components from the original series.

STL uses locally weighted regression (Loess) for smoothing, providing flexibility in modeling non-linear patterns.

---

## Experimental Study

**Goal**: Compare speed calculations from an old PeMS system and multiple versions of a modernized PeMS system to determine which modernized version best matches the existing system. The adjusting parameters including:

- [X] **Baseline**: Fixed vehicle length vs dynamic vechile length
- [X] **Spatial**: G-factor aggregation: detector, station, corridor
- [X] **Temporal**: Moving average smoothing length: 2, 12

| Variation | Baseline | Spatial | Temporal |
| --------- | -------- | ------- | -------- |
| 1         |    ✔️    |         |          |
| 2         |          | detector|         |
| 3         |          | station |         |
| 4         |          | corridor |         |
| 5         |          | detector | 2       |
| 6         |          | station  | 2       |
| 7         |          | corridor  | 2       |
| 8         |          | detector  | 12       |
| 9         |          | station  | 12       |
| 10         |          | corridor  | 12       |

## Data Preparation: SR 91

### Datasets information:

1. **Time Window**: `10/4/2024 - 10/10/2024`
2. **Temporal Aggregation Level**: `5-minute`
3. **Spatial Aggregation Level**: Detector level (`803` unique statsions containing different numbers of lanes).
4. **Detector Status**: Choose only detectors which are diagnosed as `good`. Specifically, for existing PeMS speed, we only choose data if there are two consecutive days reporting good data.
5. **Sample Size**: Around `700000` records for each variation.
6. **Seasonality**: `288` Assume the speed has a daily pattern (12*24 = 288).

### Evaluation Metrics:

#### Statistical Tests

For each detector with 3 components (`trend`, `seasonal`, `resid`), we perform independent Two-sample T-Test:
- **$H_0$**: The means of components in existing speed are equal to the ones in modernized speed.
- **$H_a$**: Their means are different.

```python
def compare_components(lane, component, data_old, data_modern):

    data_old_comp = data_old.loc[data_old['station_lane'] == lane, component].copy()
    data_modern_comp = data_modern.loc[data_modern['station_lane'] == lane, component].copy()

    data_old_comp = data_old_comp.dropna()
    data_modern_comp = data_modern_comp.dropna()

    # Perform t-test
    t_stat, p_value = ttest_ind(
        data_old_comp,
        data_modern_comp,
        equal_var=False
    )
    return t_stat, p_value
```

#### Summary Tables
1. A component is significantly different if $p$ < 0.05.
2. Count the number of detectors with significant differences for each component and calculate the percentage relative to the total number of detectors analyzed.
3. Compare all modernized PeMS speed datasets to the existing PeMS dataset.
4. Identify the dataset with the lowest percetange of significant differences.

```python
# Summarize significant differences
significant_differences = component_tests_df[component_tests_df['p_value'] < 0.05]
total_station_lanes = component_tests_df['station_lane'].nunique()
significant_counts = significant_differences.groupby('component')['station_lane'].nunique().reset_index()
significant_counts.rename(columns={'station_lane': 'significant_station_lanes'}, inplace=True)
total_counts = component_tests_df.groupby('component')['station_lane'].nunique().reset_index()
total_counts.rename(columns={'station_lane': 'total_station_lanes'}, inplace=True)
summary_df = pd.merge(total_counts, significant_counts, on='component', how='left')
summary_df['significant_station_lanes'].fillna(0, inplace=True)
summary_df['percentage'] = (summary_df['significant_station_lanes'] / summary_df['total_station_lanes']) * 100
```

##### Comparison of Significant Differences Across Datasets (%)
| Model | seasonal | resid | trend |
| ----- | -------- | ----- | ----- |
| 1     | 2.24     | 49.30 | 98.32 |
| 2     | 1.68     | 62.75 | 98.88 |
| 3     | 1.68     | 61.06 | 98.60 |
| 4     | 1.68     | 62.75 | 98.88 |
| 5     | 0.84     | 55.74 | 96.64 |
| 6     | 1.68     | 59.10 | 98.60 |
| 7     | 1.68     | 61.06 | 98.88 |
| 8     | 1.12     | 51.54 | 95.51 |
| 9     | 1.68     | 57.14 | 97.48 |
| 10    | 2.52     | 59.10 | 98.60 |
 
Detector-level performance with moving average window seems to be the optimal chocie. But t-test is not appropriate for trend comparison given the nature of time series data. We need to explore alternative ways to deal with the presence of such non-linear patterns.

#### MSE, MAE, RMSE, and Spearman's Rank Correlation Coefficient

One way is to quantitatively measue the difference between two models using error metrics.

- **Mean Absolute Error**: MAE measures the average error magnitude that each error contibutes equally to the total average.
- **Root Mean Squared Error**: RMSE is sensitive to large errors.
- **Mean Absolute Percentage Error**: MAPE express error as a percentage of the actual values, which is useful when dealing with relative errors and percentages.
- **Spearman's Rank Correlation Coefficient**:  It measures the strength and direction of the monotonic relationship between two datasets, which is a non-parametric model that captures non-linear relationships.

```python
def compute_error_metrics(lane, data_old, data_modern, component='trend'):
    # Extract components for the lane
    comp_old = data_old.loc[data_old['station_lane'] == lane, component].copy()
    comp_modern = data_modern.loc[data_modern['station_lane'] == lane, component].copy()

 
    merged_data = pd.merge(
        comp_old.reset_index(),
        comp_modern.reset_index(),
        on='timestamp',
        how='inner',
        suffixes=('_old', '_modern')
    )

    # Drop NaNs
    merged_data.dropna(subset=[f'{component}_old', f'{component}_modern'], inplace=True)

    # Check if data is sufficient
    if len(merged_data) == 0:
        return None, None, None

    # Compute error metrics
    mae = np.mean(np.abs(merged_data[f'{component}_old'] - merged_data[f'{component}_modern']))
    rmse = np.sqrt(np.mean((merged_data[f'{component}_old'] - merged_data[f'{component}_modern']) ** 2))
    with np.errstate(divide='ignore', invalid='ignore'):
        mape = np.mean(np.abs((merged_data[f'{component}_old'] - merged_data[f'{component}_modern']) / merged_data[f'{component}_old'])) * 100
    return mae, rmse, mape

def compute_correlation(lane, data_old, data_modern, component='trend', method='spearman'):
    # Extract components for the lane
    comp_old = data_old.loc[data_old['station_lane'] == lane, component].copy()
    comp_modern = data_modern.loc[data_modern['station_lane'] == lane, component].copy()

    # Align data by timestamps
    merged_data = pd.merge(
        comp_old.reset_index(),
        comp_modern.reset_index(),
        on='timestamp',
        how='inner',
        suffixes=('_old', '_modern')
    )

    # Drop NaNs
    merged_data.dropna(subset=[f'{component}_old', f'{component}_modern'], inplace=True)

    # Check if data is sufficient
    if len(merged_data) < 10:
        return None, None

    # Compute Spearman's correlation
    if method == 'spearman':
        corr_coeff, p_value = spearmanr(merged_data[f'{component}_old'], merged_data[f'{component}_modern'])
    else:
        corr_coeff, p_value = pearsonr(merged_data[f'{component}_old'], merged_data[f'{component}_modern'])

    return corr_coeff, p_value

```





# G-Factor Speed

The PeMS system possesses the ability to compute speed for sensors that don't report speed, like single loop detectors. In fact, even many double loop detectors, which often fail to report speed, depend on estimations.
Single loop detectos are prevalent for traffic measurement like occupancy and flow but do not directly measure speed. By establishing a relationship between vehicle count, occupancy, and vehicle length, this model proposes a method to estimate speed indirectly.

## Methodology

- **Statement**: Single loop detectors estimates vehicle speed by dynamically adjusting the assumed vehicle length based on observed traffic conditions, overcoming the limitations of fixed-length assumptions commonly used in other models. The estimated vehicle length is called **g-factor**.

- **Procedures**: The steps for estimation of speed from is available via [PeMS Documentation](https://pems.dot.ca.gov/?dnode=Help&content=help_calc#speeds), which we won't duplicate the documentation here.

## STL Decomposition for Comparing Speed Datasets

### Why Speed QC

- **Ambiguity**: We noted that even though the existing PeMS system claimed to follow the procedures of the original paper, they have made significant changes to smooth the speed calculation without additional detailed documentations that we can follow. It turns out difficult for us to generate the exact same logic to estimate the speed.
- **Availability**: Through investigations, it seems hard to obtain the datasets of other components, including g-factor and p-factor. They are key parameters to implement a kalman filter for speed smoothing.
- **Accuracy**: Speed is a fundamental metric in PeMS that directly influences the assessment of downstream performance metrics, such as bottleneck identification, congestion levels and VHT. Before performance metrics QC, it is essential to guarantee the accuracy of speed calculation as a basis.
- **Quantity**: Visual assessments are insufficient to measure the difference. To accurately identify and measure these variations and adjusted parameters at the detector level, it is essential to adopt quantitative methods.

![Daily Average Speed for Each GP Lane](https://github.com/user-attachments/assets/e96bf5ab-6f7a-4b4c-99c0-48da7be3411d)
![Daily Average Speed for Each GP Lane PeMS Modernization](https://github.com/user-attachments/assets/0c0e51d8-d145-4586-949b-aa34c07cfec4)

### Why Use [STL Algorithm](https://www.statsmodels.org/dev/examples/notebooks/generated/stl_decomposition.html)?

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

## Experimental Study

**Goal**: Compare speed calculations from the existing PeMS system and multiple versions of the modernized PeMS system to determine which modernized version best matches the existing system. The adjusting parameters including:

- [x] **Baseline**: Fixed vehicle length vs dynamic vehicle length (g-factor or not)
- [x] **Spatial**: G-factor aggregation: detector, station, corridor
- [x] **Temporal**: Moving average smoothing length: 4, 12

| Variation | Baseline | Spatial  | Temporal |
| --------- | -------- | -------- | -------- |
| 1         | ✔️       |          |          |
| 2         |          | detector |          |
| 3         |          | station  |          |
| 4         |          | corridor |          |
| 5         |          | detector | 4        |
| 6         |          | station  | 4        |
| 7         |          | corridor | 4        |
| 8         |          | detector | 12       |
| 9         |          | station  | 12       |
| 10        |          | corridor | 12       |

## Data Preparation: SR 91

### Datasets information:

1. **Time Window**: `10/4/2024 - 10/10/2024`
2. **Temporal Aggregation Level**: `5-minute`
3. **Spatial Aggregation Level**: Detector level (`803` unique stations containing different numbers of lanes).
4. **Detector Status**: Choose only detectors which are diagnosed as `good`. Specifically, for existing PeMS speed, we only choose data if there are two consecutive days reporting good to get rid of imputation data.
5. **Sample Size**: Around `700000` records for each variation.
6. **Seasonality**: `288` Assume the speed has a daily pattern (12\*24 = 288).

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

The results show that all models handle seasonal variations well, with only small differences between datasets. Models 5 and 8 have the least discrepancies. However, there are significant differences in the residuals and trends: residuals are about 50%, and trends are over 95%. The residuals remain because not all seasonal and trend patterns are removed by the STL process, which is expected due to the high volatility of the 5-minute speed dataset. This means the residuals still remain partial trend and seasonality patterns, therefore not following simple white noise patterns. In the future, we might use more complex models to better capture the patterns in speed data. As for the trends through a sample visualization, it is highly nonlinear. Using a t-test to compare them isn’t suitable because time series data don’t meet the normality condition required for t-tests. We need to find other ways to analyze these non-linear patterns.

![image](https://github.com/user-attachments/assets/9efdb0a0-bf2e-4c03-8640-8135a81cabe3)

#### MAE, RMSE, and MAPE

One way is to quantitatively measure the difference between two models using error metrics for trend component to compare performance among models.

- **Mean Absolute Error**: MAE measures the average error magnitude that each error contributes with the same weight to the total average.
- **Root Mean Squared Error**: RMSE is sensitive to large errors.
- **Mean Absolute Percentage Error**: MAPE express error as a percentage of the actual values, which is useful when dealing with relative errors and percentages.

<!---
Explore the non-linear patterns.
**Spearman's Rank Correlation Coefficient**:  It measures the strength and direction of the monotonic relationship between two datasets, which is a non-parametric model that captures non-linear relationships.
--->

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
```

##### Comparison of Error Metrics for Trend Comparison

| Model | MAE  | RMSE  | MAPE  |
| ----- | ---- | ----- | ----- |
| 1     | 9.83 | 10.22 | 16.38 |
| 2     | 8.50 | 8.81  | 14.27 |
| 3     | 4.43 | 4.61  | 7.24  |
| 4     | 8.50 | 8.81  | 14.27 |
| 5     | 3.17 | 3.50  | 5.32  |
| 6     | 4.33 | 4.63  | 7.27  |
| 7     | 8.49 | 8.80  | 14.25 |
| 8     | 3.21 | 3.55  | 5.40  |
| 9     | 4.37 | 4.67  | 7.32  |
| 10    | 8.50 | 8.81  | 14.25 |

![MAE](https://github.com/user-attachments/assets/4ea490de-5160-43e8-a4c0-df963ff98181)
![RMSE](https://github.com/user-attachments/assets/9c4816f0-89cb-4ac0-9ade-55e57efac96a)
![MAPE](https://github.com/user-attachments/assets/e80341af-eddc-4996-ba2f-9a67a735185f)

According to the error metrics, model 5 and 8 still outperforms other models in terms of trend. We choose model 8 as the final model for g-factor speed calculation.

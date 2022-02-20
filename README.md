# 2AMD15
Course project 2AMD15 by group 8

### Install prereqs
```
findspark, pyspark
```

### Raw data format
```
weather_preprocessing/datasets: folder with 2016, 2017, 2018, 2019, 2020 subfolders
          each subfolder should be the extracted contents of the corresponding archive
          from https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/
```

### Data cleaning
`> python weather_preprocessing/pyspark_import.py`
Example output:
```
Data reading took 18.64s
Full dataset: 20570861 rows
Number of stations: 13130
Total number of days in range: 1826
After filtering: 1917185 rows
Number of stations: 4643
Keeping data for 173 stations
Data cleaning took 305.39s
Writing 247249 rows to file
22/02/20 13:27:50 WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory
Scaling row group sizes to 95.00% for 8 writers
Data writing took 300.43s
```

Confirm correctness:
```
> python weather_preprocessing/part1.py
Imported 247249 rows
```
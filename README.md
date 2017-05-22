# SeasonalTemperatureCalc
A mini project to demonstrate Spark - HBase integration for data ingestion and analysis
Development Stack: Scala 2.10.4 Spark 1.6 HBase 1.2 CDH 5.10.1

Problem Statement 1
Normalize and Ingest monthly historical weather station data from NCDC archive into a data store of your choice:
Global monthly summary data: https://www.ncei.noaa.gov/data/gsom/archive/gsom_latest.tar.gz 
Documentation: https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/gsom-gsoy_documentation.pdf 
Station Data (also includes geolocation data):https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt 

Solution
Apache HBase is chosen as the data store which best suits this particular use case. HBase is better suited to handle
random Scans as compared to Hive. The code for data ingestion and storing into HBase using HFiles is given in the 
following two files:
AverageSeasonalTempIngestion.scala
StationDataIngestion.scala

There are many ways of writing data to HBase. The approach used in this solution is to use the "Bulk Load" process.
The Bulk Load process is an efficient way of loading data because it bypasses the requirement to use the Write Ahead Log.
For the seasonal data contained in the gzip file, the design is to read the data and repartition (as tar.gz files are
not splittable). Then, filter out the irrelevant records such as headers or the ones which do not have temperature
information. The previous step is followed by mapping the data to create Key Value pairs for ingestion into HBase via a MR job.
The Key value pairs are writte to HDFS using the HFileFormat2. The HFiles created by this process are then loaded into HBase. 

Normalization in this case means separating the station data (which has lattitude and longitude information) and the
actual weather data (we have chosen to pick average temperature and precipitation as the two facts which we shall 
store in HBase. Also, during the process of ingesting the data, the "Season" for each record will be calculated and stored
to make the subsequent implementaions easier.

The relevant HBase Commands are given below.

Problem Statement 2
Calculate and store average seasonal temperature per year for all years after 1900 for each 1°x1° grid on Geographic Coordinate System. 
For the sake of this problem, assume that spring covers March, April and May; summer covers June, July and August; fall covers September, 
October and November; and winter covers December, January and February. Details of the implementation are left up to you. 
Sparsity of the data should be taken into account in your solution (i.e. store the number of available datapoints for each grid cell).

Solution
The earth could be imagined as a rectangle on the coordinate system with bottom left vertex at (-90,-180) and the top right vertex at (90,180)
The gist of the problem is to attribute station data (tied to its coordinates) to a generic block of 1 degree by 1 degree.
For the purpose of this solution, we have assumed that if any station lies within 100 Sq. Km. (+ or - 0.5 degrees) of the coordinate, 
then it's data is validfor that particular coordinate point. So, for each coordinate point, we will consider all stations which are 
within -0.5 degrees of its lattitude or longitude. Therefore, the round() function could be used to determine which coordinate point
a particular data row can be attributed to.

Technical Design: The design is similar to the ingestion model. Data is read from both Normalized HBase tables, one which holds the
station information (lattitude and longitude) and the other which holds the temperature data. The key to joining the two data sets
is the Station ID. 

Once the data sets are joined, the rounded value of lattitude and longitude is calculated. Using the data frames, a grouping is created 
based on the rounded Lattitude, rounded Longitude, Year and Season. Here, the Season stored in Problem 1 comes in handy.
For the grouped data, we calculate the average temperature, number of data points and the list of stations.

A HFiles are created for the final summary data and stored on HDFS. These HFiles are loaded into HBase using the following command.
Please note that the -D argument had to be added to allow loading of more than 32 files at a time.


## Data Engineering for Distributed Micro-Service Pipeline
The project aims at learning to design a scalable data pipeline that can receive data from streams, combine them with knowledge repository, carry out necessary pre-processing, perform calculations, publish results. The design needs to be implemented using either Java or Python as Microservices with the use case real-time feedback application of NYC taxi services.


## Pipeline Design
The data pipeline design of the NYC taxi real-time application implements event-oriented, distributed and SOA by incorporating the principles from Complex Event Processing (CEP) and Microservices framework. The design patterns are illustrated as 1) high-level components using Information Flow Processing (IFP) models (1), 2) flow of the data in the pipeline using decomposed components and Inter-Process Communication (IPC) of microservices (2)

## Instructions to run the code

1. Sort the taxi trip data and save in the root folder
TLC URL: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Dataset URL: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-01.csv
Taxi Zone Lookup Table: https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
Yellow Trip Data Dictionary:
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
2. Ensure location lookup and crash data also in the root folder
3. Ensure ActiveMQ is up and running
4. Run the python programs in the following order,
	a) trip_freq_count
	b) trip_BusiestLoc_finder
	c) trip_preprocess
	d) trip_stream
	e) accident_stream
	f) accident_preprocess
	g) accident_count
	h) analytics
	i) daily_analytics


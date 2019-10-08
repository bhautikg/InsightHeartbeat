# Insight Heartbeat

My Insight Project was to design a real-time continous ECG classification pipeline.

## Table of Contents
1. [Business Problem](README.md#business-problem)
2. [Solution](README.md#solution)
3. [Installation](README.md#installation)

## Business Problem
### Aims
1. Hospitals can use less resources to better monitor their patients in real time on a large scale.
2. Ability to get fast and accurate overview of a patients heart activity over past few days.

### Description
Many techniques exist to classify, and process ECG signals accurately, but few have been applied on a large scale on real time data. My project aims to classify the signals in real-time and display the classified ECG signals and data. The signals are classified as either normal or abnormal on 5 second windows, and any abnormal signals are indexed and stored for viewing and further analyzing for later.


## Solution

I stored the MIT-BIH Data on S3, it contains timeseries ECG data for individual patients in separte CSV files. I used Kafka to ingest the data line by line for multiple records simultaneously; simulating a real-time data stream of ECG signals from multiple patients. The kafka topic is subscribed by 1 flink cluster which consumes the data, processes/classifies it using flink stream functions, and stores the data into TimeScaleDatabase(Postgress extension for timeseries data). 

The Flink cluster uses two algorithms to achieve the classification:
1. Feature extraction/peak-detection.
2. Classification using a Teager-energy function and R peak - R-peak interval.

Peak detection algorithm was taken from the following TarosDSP project (https://github.com/JorenSix/TarsosDSP/blob/master/src/core/be/tarsos/dsp/beatroot/Peaks.java)

The classification algorithm was developed using results from this paper by Chandrakar Kamath: A NOVEL APPROACH TO ARRHYTHMIA CLASSIFICATION USING RR INTERVAL AND TEAGER ENERGY(https://pdfs.semanticscholar.org/212d/e8964d165c575d768f97c9bcd42fd16816ac.pdf), in Journal of Engineering Science and Technology Vol. 7, No. 6 (2012) 744 - 755 © School of Engineering, Taylor’s University. 

The teager energy function estimates the energy of the source based on the discretized signal. 

Finally, the results are shown using Dash web app. 


### Architecture
<p align="center">
<img src="https://github.com/bhautikg/InsightHeartbeat/tree/master/img/pipeline.png" width="700", height="400">
</p>

## Installation
### Requirements 
* [peg](https://github.com/InsightDataScience/pegasus)
* aws/cloud computing
* [MIT Arrythmia Data Set](https://archive.physionet.org/physiobank/database/mitdb/)

### Setup
* To setup clusters run setup/setup_cluster.sh.
* To setup database follow setup/db_setup.txt.
* To start the pipeline follow the instructions in src/README.md 

### Testing
* Unittests can be run using the run_unittests.sh file in the ./test folder. Results are stored in ./test/results.txt.

### Cluster Configuration
* four m4.large EC2 instances for Kafka producers
* four m4.large EC2 instances for Flink which processes and classifies the signals and writes to postgres database
* one m4.large EC2 instance for TimescaleDB/PostgreSQL database
* one t2.micro EC2 instance for Dash app
```


#!/bin/bash

#Using Python3 for pyspark
export PYSPARK_PYTHON=python3

#Run python code on spark using spark-submit
/usr/hdp/current/spark-client/bin/spark-submit ~/AlphaVantage/test.py


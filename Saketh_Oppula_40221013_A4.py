import os
from itertools import permutations

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *



def restaurant_shift_coworkers(worker_shifts: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the worker_shifts.txt. Performs a series of MapReduce operations via
    PySpark to calculate the number of shifts worked together by each pair of co-workers. Returns the results as an RDD
    sorted by the number of shifts worked together THEN by the names of co-workers in a DESCENDING order.
    :param worker_shifts: RDD object of the contents of worker_shifts.txt.
    :return: RDD of pairs of co-workers and the number of shifts they worked together sorted in a DESCENDING order by
             the number of shifts then by the names of co-workers.
             Example output: [(('Shreya Chmela', 'Fabian Henderson'), 3),
                              (('Fabian Henderson', 'Shreya Chmela'), 3),
                              (('Shreya Chmela', 'Leila Jager'), 2),
                              (('Leila Jager', 'Shreya Chmela'), 2)]
    """
    worker_shifts_s1 = worker_shifts.map(lambda x: (x.split(',')[1], x.split(',')[0]))
    # Output type  ---> [(str,str)]
    workers_shifts_s1_2 = worker_shifts_s1.reduceByKey(lambda x, y: x+"::"+y)

    workers_shifts_s1_sorted = workers_shifts_s1_2.sortBy(lambda x: x[1], ascending=False)
    # print (workers_shifts_s1_sorted.collect())

    def map_helper(x):
        l = x[1].split('::')
        l_out = []
        for i in range(0, len(l) - 1):
            l_out.append(((l[i],l[i+1]),1))
        return l_out

    worker_shifts_s2 = workers_shifts_s1_sorted.flatMap(lambda x: map_helper(x))
    # Output type  ---> [((str,str),1)] or [((str,str),int)]  -- list of tuple of string tuple and int
    workers_shifts_s2_2 = worker_shifts_s2.reduceByKey(lambda x, y: x+y)

    workers_shifts_s2_sorted = workers_shifts_s2_2.sortBy(lambda x: x[1], ascending=False)

    # workers_shifts_s3_mapped = workers_shifts_s2_sorted.map(lambda x: (x[0], 1))

    # Output type  ---> [((str,str),1)] or [((str,str),int)] -- list of tuple of string tuple and int
    workers_shifts_s3_reduced = workers_shifts_s2_sorted.reduceByKey(lambda x, y: x+y)
    workers_shifts_s3_sorted = workers_shifts_s3_reduced.sortBy(lambda x: x[1], ascending=False)
    # print("-----------------------------------------------")
    # print(workers_shifts_s3_reduced.collect())
    # print("-----------------------------------------------")
    return workers_shifts_s3_sorted
    # raise NotImplementedError('Your Implementation Here.')


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    # flights.printSchema()
    # # print a few rows
    # flights.show()


    # # filtration & selection
    flights_selected = flights.select(col("Airline"),month(col("FlightDate")).alias("mm"), col("Cancelled"))
    flights_filtered = flights_selected.filter((flights_selected.mm == 9) & (flights_selected.Cancelled == True))
    flights_grouped = flights_filtered.groupBy(['Airline']).count()
    flights_sorted = flights_grouped.orderBy('count', ascending=False)
    # flights_sorted.show()
    rows = flights_sorted.collect()

    return rows[0].Airline
    # raise NotImplementedError('Your Implementation Here.')


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of 
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    flights_selected = flights.select(col("Airline"), datediff(to_date(lit("2021-11-20")), col("FlightDate")).alias("datediff"), col("Diverted"), )
    flights_filtered = flights_selected.filter((flights_selected.datediff >= 0) & (flights_selected.datediff <= 10) & (flights_selected.Diverted == True))
    # flights_sorted.show()

    return flights_filtered.count()
    # raise NotImplementedError('Your Implementation Here.')


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    """
    flights_filtered = flights.filter((flights.Origin == 'BNA') & (flights.Dest == 'ORD'))
    out = flights_filtered.select(mean('AirTime').alias("av"))
    out_final = out.collect()
    return out_final[0].av

    # raise NotImplementedError('Your Implementation Here.')


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is 
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing. 
    """
    flights_filtered = flights.filter(flights.DepTime.isNull())
    flights_selected = flights_filtered.select('FlightDate')
    flights_distinct = flights_selected.distinct()
    out = flights_distinct.count()
    return out

    # raise NotImplementedError('Your Implementation Here.')



def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.getOrCreate()

    print('########################## Problem 1 ########################')
    # problem 1: restaurant shift coworkers with Spark and MapReduce 
    # read the file
    worker_shifts = sc.textFile('worker_shifts.txt')
    sorted_num_coworking_shifts = restaurant_shift_coworkers(worker_shifts)
    # print the most, least, and average number of shifts together
    sorted_num_coworking_shifts.persist()
    print(sorted_num_coworking_shifts.collect())
    print('Co-Workers with most shifts together:', sorted_num_coworking_shifts.first())
    print('Co-Workers with least shifts together:', sorted_num_coworking_shifts.sortBy(lambda x: (x[1], x[0])).first())
    print('Avg. No. of Shared Shifts:',
          sorted_num_coworking_shifts.map(lambda x: x[1]).reduce(lambda x,y: x+y)/sorted_num_coworking_shifts.count())
    
    print('########################## Problem 2 ########################')
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv('Combined_Flights_2021.csv', header=True, inferSchema=True)
    print('Q1:', air_flights_most_canceled_flights(flights), 'had the most canceled flights in September 2021.')
    print('Q2:', air_flights_diverted_flights(flights), 'flights were diverted between the period of 20th-30th '
                                                       'November 2021.')
    print('Q3:', air_flights_avg_airtime(flights), 'is the average airtime for flights that were flying from '
                                                   'Nashville to Chicago.')
    print('Q4:', air_flights_missing_departure_time(flights), 'unique dates where departure time (DepTime) was '
                                                              'not recorded.')
    

if __name__ == '__main__':
    main()

# coursera-cloud-capstone
Coursera Cloud Computing Specialization Capstone
 
This project contains my code for the Coursera Cloud Computing 
Specialization Capstone. It should not be considered a good example
of Hadoop code - I know the basics but the implementation may be
far from optimal.

The problem definition follows.

__Business Requirements__

For each task, you must answer a subset of the following questions. Each question is over the entire dataset from the US Bureau of Transportation Statistics (BTS), unless otherwise specified.

_Group 1 (Answer any 2):_

* Rank the top 10 most popular airports by numbers of flights to/from the airport.
* Rank the top 10 airlines by on-time arrival performance.
* Rank the days of the week by on-time arrival performance.

_Group 2 (Answer any 3):_

* For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
* For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.
* For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
* For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y.

_Group 3 (Answer both using only Hadoop and Spark):_

*Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?

* Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. More concretely, Tom has the following requirements:
The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
Tom wants to arrive at each destination with as little delay as possible.
Your mission (should you choose to accept it!) is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the flight that satisfies constraints (a) and (b) and has the best on-time performance with respect to constraint (c), if such a flight exists.

For the queries in Group 2 and Question 3.2, you will need to compute the results for ALL input values (e.g., airport X, source-destination pair X-Y, etc.) for which the result is nonempty. These results should then be stored in Cassandra so that the results for an input value can be queried by a user. Then, closer to the grading deadline, we will give you sample queries (airports, flights, etc.) to include in your video demo and report.

For example, after completing Question 2.2, a user should be able to provide an airport code (such as “ATL”) and receive the top 10 airports in decreasing order of on-time departure performance from ATL. Note that for questions such as 2.3, you do not need to compute the values for all possible combinations of X-Y, but rather only for those such that a flight from X to Y exists.

__Technical Requirements__

There are various technical requirements – what needs to be put in HDFS, what needs to be put in Cassandra, etc. – but I don’t think anyone outside of the class really cares about that.

__Data Source__

The files have been downloaded from BTS and made available on a read-only AWS snapshot but we are responsible for determining where to find the data of interest in the 18 databases performing any required cleanup. 

__Budget__

Amazon has donated up to $350 in credit on AWS to each student. I’m sure that’s far in excess of what they reasonably anticipate anyone requiring so I’m guessing they anticipate $50 per technology.
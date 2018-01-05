Author: Prakhar Dogra
G Number: G01009586
CS 657 Assignment 2

The following README gives details about the files contained in this folder:

1. Dataset
	The dataset was downloaded from the MovieLens website from the following link : http://grouplens.org/datasets/movielens/

2. SourceCode
	This folder contains the Source Code of all the MapReduce and Spark programs used to complete the tasks of the assignment. It also contains a python script that was used to generate datasets of 10 percent increments.
	Task #1 to find 20 frequent movie-pairs using:
		- Pairs approach in MapReduce: MapReduce Pairs Mapper.py, MapReduce Pairs Reducer.py
		- Stripes approach in MapReduce: MapReduce Stripes Mapper.py, MapReduce Stripes Reducer.py
		- Pairs approach in Spark: Spark Pairs.py
		- Stripes approach in Spark: Spark Stripes.py
	Task #2 to find movie-pairs with conditional probability > 0.8 
		- Stripes approach in Spark: Spark Conditional Probability.py
	Task #3 to find movie pairs with lift > 1.6
		- Stripes approach in Spark: Spark Lift.py

3. PseudoCode
	This folder contains the Pseudo Codes for all the Mappers and Reducers mentioned above:
	Following are the Pseudo Codes associated with their respective tasks:
	Task #1
		- Pairs approach in MapReduce: MapReduce Pairs.pdf
		- Stripes approach in MapReduce: MapReduce Stripes.pdf
		- Pairs approach in Spark: Spark Pairs.pdf
		- Stripes approach in Spark: Spark Stripes.pdf
	Task #2 
		- Stripes approach in Spark: Spark Conditional Probability.pdf
	Task #3
		- Stripes approach in Spark: Spark Lift.pdf

4. Output
	This folder contains the Output Folders of each respective task:
	Following are the sub folders and format of the output associated with their respective tasks:
	Task #1
		- MapReduce Pairs
		- MapReduce Stripes
		- Spark Pairs
		- Spark Stripes
		Each of the subfolders contain a sample file that contains 20 frequent movie-pairs along with their frequency.
	Task #2 
		- Spark Conditional Probability: It contains a sample file that contains 20 frequent movie-pairs along with their conditional probability P(B/A).
	Task #3
		- Spark Lift: It contains a sample file that contains 20 frequent movie-pairs along with their lift.
5. Graph
	This folder contains the graphs that were plotted for each respective task:
	Task #1
		- MapReduce Pairs and Stripes
		- Spark Pairs and Stripes
	Task #2
		- Spark Conditional Probability
	Task #3
		- Spark Lift
	Each graph plotted is the time taken by the job versus the amount of the data set.
	The graph was plotted for 10%, 20%, 30%,....., 100% data sizes. (In the incements of 10%)
	
6. General Information
	For MapReduce jobs, 8 mappers anf 8 reducers were used.
	For Spark jobs, 16 partitions were used.
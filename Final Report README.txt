Parallelizing Stable Matching Project Files


Configuration, Installation:
There are two ways to install pyspark. The first way is to install the entire Apache Spark package from https://spark.apache.org/downloads.html. Make sure it’s the latest version and have both java and python installed before the installation. If you are installing it from linux, here are some instructions on how to install it from the terminal, https://patilvijay23.medium.com/installing-and-using-pyspark-on-linux-machine-e9f8dddc0c9a. 
Another way is pip installing it directly from the terminal. This is the recommended way to install pyspark, since you’ll be able to access its libraries when you're importing it to a python script. Here is the link to those instructions, https://spark.apache.org/docs/latest/api/python/getting_started/install.html.


Operating Instructions:
The only file necessary to configure in order to run tests on the various improvements we made to the PII algorithm is test.py. This file contains a testing framework, which tests a certain version of the PII algorithm (from another file). It will start testing at a set size of n (men and women), will perform TRIALS total tests on random preference matrices with that set size, after which n will increment by STEP and run that number of tests again, until n increases to the value in BOUND. n, TRIALS, STEP, and BOUND are field variables that can be changed at the top of test.py. To test different algorithm versions, change the import statement on the top from sparkPII.py to whatever file contains the desired version.

To make your own changes to the PII algorithm, edit any of the sparkPII files, which contain each step in the iterations to complete the algorithm, and test using test.py as described above.


Necessary Main Files:
sparkPII: Files beginning with sparkPII contain our various algorithms for improvements to the PII algorithm, with a description of the exact method after. sparkPII.py contains our final, most improved resulting algorithm.


Note: We tracked iterations of the algorithm, having determined the theoretical complexity of each iteration separately. Unfortunately, we did not have the n2 cores necessary to test for higher values of n, so for efficiency the implementation uses parallelism somewhat differently.


Test.py: test.py contains the testing framework. To use it, set the desired set size (of men and women, or n in the file), number of tests per set size, and the increment to each set size after the tests have been run. You can also configure it to run the tests on files other than sparkPII.py if necessary.
Framework.py: This is a supporting file containing the methods needed for computation    and formatting of output for test.py
Detector.py: Converts stored data to readable string, supports test.py


performanceData: Files beginning with performanceData contain the testing data for tests on various algorithms with various test sizes, numbers of cases, and increments.


Other Files of Interest:
gs.py contains the basic Gale-Shapley algorithm implementation, used for comparison.


ImprovedPII_Nov242013.py: Contains a transferred version of a previous PII improvement in python. Missing some supportive methods and thus doesn’t run it properly. Algorithm was found to be highly inefficient.


Preprocessing.py: Contains some of the preprocessing methods we used in trials and experimented with


Known Bugs & Troubleshooting:
There are currently no known bugs in the necessary files. 
The Hadoop/Java code was not successfully converted to spark and python, and the file doesn’t properly execute the algorithm. We simply re-implemented the algorithm more concisely.


Contact
Alec Kyritsis - akyritsis@middlebury.edu
Scott Wynn - scott.h.wynn@gmail.com
Stephora Alberi - salberi1@gulls.salisbury.edu


Acknowledgements
We are extremely grateful for the direction and support given by Dr. Enyue Lu, as well as her patience in acclimating us to the research process and the PII algorithm, which she originally created. We would also like to thank Colin White for his keen insight on cycles in the PII algorithm and their detection, which we were able to apply to further improve the convergence of our PII improvements.


We would also like to mention our Salisbury REU cohort members and professors, who continually provided a further enriching experience, moral support and a strong community. The continual support from the National Science Foundation and Salisbury University was also invaluable in allowing us to take part in and progress through this project.


Finally, we would like to thank our families and the professors from our home institutions, who supported us in getting the opportunity to partake in this project at the Salisbury REU and supported us throughout the process.
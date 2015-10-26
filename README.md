# Sifarish
Content based and collaborative filtering based recommendation and personalization engine implementation on Spark

I would like to thank Dr.Pranab Ghosh and Mr.Naren Gokul for giving me this opportunity to work on Sifarish.

For more details regarding Item based collaborative Filtering technique, follow my other repository, https://github.com/kranthisai/Itemcf

Implementation in Spark:

Step 1: Generating the Ratings data file.A sample ratings file is there in this project. For testing, you can work on that file. If you would like to generate explicit rating file for 1million users, fork sifarish repository by Pranab Ghosh and use the following link.https://github.com/pranab/sifarish/blob/master/resource/tutorial.txt . Under https://github.com/pranab/sifarish/blob/master/resource/tutorial.txt. You can run a small script, that generates Ratings data file.

Step 2: Item Correlation.
We want to find correlation between items i.e. rows in the rating matrix. Although correlation of users is another approach, items based correlation has been found to be more reliable. I will be using the Cosine Distance based correlation. 

This data is consumed by Itemrec class: https://github.com/kranthisai/Sifarish/blob/master/src/main/java/org/sifarish/common/Itemrec.java

Step 3: Correlated Rating
The correlated rating depends on the known rating of an item and correlation between the item and the correlated item. There is a parameter called correlation.modifier through which the impact of the correlation can be controlled. It’ value is around 1.0. If it’s greater than 1 it has the the effect of reducing the impact of low correlation values. In other words, an item that is poorly correlated to the target item, will tend to be filtered out. Choosing a value less than 1.0 has  an equalizing effect of the correlation values.(https://github.com/kranthisai/Sifarish/blob/master/src/main/java/org/sifarish/common/UtilityPredictor.java)

Step 4: Predicted Rating

Generally a weighted average is used based on the correlation value.Final predicted  rating could be weighted by the correlation weight which is the intersection length between two item vectors. It reflects the quality of the correlation. We could have a high correlation value even if there are only few users rating the two items in question, but they happen to be an agreement resulting in high correlation value. But the correlation weight will be small. (https://github.com/kranthisai/Sifarish/blob/master/src/main/java/org/sifarish/common/UtilityAggregator.java)

Next, I will create tutorial document for RDD flow. Also,I will upload one jar file, where you can directly run that jar file from terminal.

In case if you have any queries regarding this project or if you have any suggestions to improve this project, please reach out to me at kranthisai.d@gmail.com. I will be happy in assisting you. If you would like to contribute, I will appreciate your contribution. I will be back with even more recommendation systems in Spark and Hadoop. My next project will be on Real Time Recommendation systems on Spatial Data. Thank you. Have A Nice Day....

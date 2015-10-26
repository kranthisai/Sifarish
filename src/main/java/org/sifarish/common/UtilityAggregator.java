package org.sifarish.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class UtilityAggregator {
	public static void main(String[] args) throws Exception {
		 SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		String path ="utilpred/part-00000";
		JavaRDD<String> data1 =sc.textFile(path);
		JavaPairRDD<String,String> data=data1.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>,String,String>(){
			private List<Tuple2<String,String>> utilpredictiontuple=new ArrayList<Tuple2<String,String>>();
			
			public Iterable<Tuple2<String, String>> call(Iterator<String> t)
					throws Exception {
				while(t.hasNext())
				{
					String tuple=t.next();
					String userid=tuple.split(",")[0];
					String itemid=tuple.split(",")[1];
					String predictedrating=tuple.split(",")[2];
					String Correlationweight=tuple.split(",")[3];
					String correlationvalue=tuple.split(",")[4];
					utilpredictiontuple.add(new Tuple2<String,String>(userid+","+itemid,predictedrating+","+Correlationweight+","+correlationvalue+","+tuple.split(",")[5]));
				}
				
			
				// TODO Auto-generated method stub
				return utilpredictiontuple;
			}
			
		}).sortByKey();
		//data.saveAsTextFile("/home/system/kranthisai/data/aggr");
		JavaPairRDD<String,List<String>> data2=data.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,String>>,String,List<String>>(){
			private List<Tuple2<String,List<String>>> utiltuple=new ArrayList<Tuple2<String,List<String>>>();
			private List<String> predictiontuple=new ArrayList<String>();
			public Iterable<Tuple2<String, List<String>>> call(
					Iterator<Tuple2<String, String>> t) throws Exception {
				// TODO Auto-generated method stub
				
				if(t.hasNext())
				{
					Tuple2<String,String> tuple=t.next();
					String useritem=tuple._1;
					predictiontuple.add(tuple._2);
					while(t.hasNext())
					{
						Tuple2<String,String> tuple1=t.next();
						if(useritem.matches(tuple1._1))
						{
							predictiontuple.add(tuple1._2);
							useritem=tuple1._1;
						}
						else
						{
							utiltuple.add(new Tuple2<String,List<String>>(useritem,predictiontuple));
							useritem=tuple1._1;
							predictiontuple=new ArrayList<String>();
							predictiontuple.add(tuple1._2);
							
						}
					}
					utiltuple.add(new Tuple2<String,List<String>>(useritem,predictiontuple));
				}
				
				
				
				return utiltuple;
			}
			
		});
			//data2.saveAsTextFile("/home/system/kranthisai/data/utilaggrinter");
		JavaRDD<String> ratingaggregator=data2.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,List<String>>>,String>(){

			private String tempuseritem=null;
			private List<String> useritemsratings=new ArrayList<String>();
			private List<String> ratingagg=new ArrayList<String>();
			private int predRating;
			private boolean corrLengthWeightedAverage =false;
			private int sum;
			private int sumWt;
			private int distSum;
			private int corrScale =1000;
			int maxRating=100;
			private int maxStdDev=(35 * maxRating)  / 100;
			private boolean inputRatingStdDevWeightedAverage;
			private int avRating;
			private int utilityScore;
			private String ratingAggregatorStrategy="average";
			private List<Integer> predRatings = new ArrayList<Integer>();
			private Integer medianRating;
			private int maxPredictedRating=0;
			private String fieldDelim=",";
			private StringBuilder stBld = new  StringBuilder();
			private boolean userRatingWithContext;
			private boolean outputAggregationCount=false;
			private int count;
			public Iterable<String> call(Iterator<Tuple2<String, List<String>>> t)
					throws Exception {
				// TODO Auto-generated method stub
				while(t.hasNext())
				{
					Tuple2<String, List<String>> tuple=t.next();
					String useritem=tuple._1;
					List<String> finalaggregation=new ArrayList<String>();
					List<String> utilpredicttuple=tuple._2;
					String user=useritem.split(",")[0];
					String item=useritem.split(",")[1];
						if(!utilpredicttuple.isEmpty())
						{
							sum = sumWt = 0;
							maxRating = 0;
							count = 0;
							stBld.delete(0, stBld.length());
							if (ratingAggregatorStrategy.equals("average")) 
							{
								//System.out.println(useritemsratings.size());
								for(String value:utilpredicttuple)
								{
						
								predRating = Integer.parseInt(value.split(",")[0]);
								
								if (corrLengthWeightedAverage ) {
									//correlation length weighted average
									sum += predRating * Integer.parseInt(value.split(",")[1]);
									sumWt += Integer.parseInt(value.split(",")[1]);
									
								}
								else if (inputRatingStdDevWeightedAverage) {
									//input rating std dev weighted average
									int stdDev = Integer.parseInt(value.split(",")[3]);
									if (stdDev < 0) {
										throw new IllegalStateException("No rating std dev found stdDev:" + stdDev);
									}
									
									int normStdDev = invMapeStdDev(stdDev);
									sum += predRating * normStdDev;
									sumWt += normStdDev;
								}	else {
									//plain average
									sum += predRating;
									++sumWt;
								}
								distSum += (corrScale - Integer.parseInt(value.split(",")[2]));
								++count;
							}
							avRating = sum /  sumWt ;
							utilityScore = avRating;
						}
							else if(ratingAggregatorStrategy.equals("median"))
							{
								for(String value:utilpredicttuple)
								{
									predRating = Integer.parseInt(value.split(",")[0]);
									predRatings.add(predRating);
									
								}
								count = predRatings.size();
								if (count > 1) {
									Collections.sort(predRatings);
									if (count % 2 == 1) {
										//odd
										medianRating = predRatings.get(count / 2);
									} else {
										//even
										medianRating =  (predRatings.get(count / 2 - 1) + predRatings.get(count / 2) )  / 2 ;
									}
								} else {
									medianRating = predRatings.get(0);
								}
								utilityScore = medianRating * corrScale;
							}
							else if (ratingAggregatorStrategy.equals("max")) {
								//max 
								for(String value:utilpredicttuple) {
									predRating = Integer.parseInt(value.split(",")[0]);
									if (predRating > maxRating) {
										maxRating = predRating;
									}
									++count;
								}
								utilityScore = maxRating;
							}
							
						}
						if (utilityScore > maxPredictedRating) {
							maxPredictedRating = utilityScore;
						}
						stBld.append(user).append(fieldDelim).append(item).append(fieldDelim);
			           	
			           	stBld.append(utilityScore);
			          
			        	String agg=stBld.toString();
			        	ratingagg.add(agg);
				   		
			
			        	
					}
				
				return ratingagg;
			}
			private int invMapeStdDev(int stdDev) {
				// TODO Auto-generated method stub
				int norm = maxStdDev - stdDev;
	        	if (norm <= 0) {
	        		norm = 1;
	        	}
	        	return norm;
			}
			
		});
		ratingaggregator.saveAsTextFile("UtilAggregator");
		}
		
	}

			
			
		 
		 
	

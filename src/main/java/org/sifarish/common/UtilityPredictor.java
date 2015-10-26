package org.sifarish.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import akka.japi.Function;

import scala.Tuple2;

public class UtilityPredictor {
	private static String path;
	private static String path1;
	 public static void main(String[] args) throws Exception {
		/* if (args.length != 2) {
		      throw new Exception("Usage BasicLoadSequenceFile [sparkMaster] [input]");
				}*/
		 SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 path ="ratings";
		 path1=conf.get("path1","corealtion/part-00000");
		 JavaRDD<String> data1 = sc.textFile(path).mapPartitions(new FlatMapFunction<Iterator<String>,String>(){

			private List<String> s=new ArrayList<String>();

			public Iterable<String> call(Iterator<String> t) throws Exception {
				// TODO Auto-generated method stub
				while(t.hasNext())
				{
					String s1=t.next();
					s.add(s1+","+1);
					
				}
				return s;
			}
		 
		 
		 });
		 
			
		
		 JavaRDD<String> data2 = sc.textFile(path1);
		 JavaRDD<String> data = sc.union(data1,data2);
		 //data.saveAsTextFile("/home/system/kranthisai/hello");
		 JavaPairRDD<String,List<List<String>>> utilpartition=data.mapPartitionsToPair(new Ratingclass(conf));
		 JavaPairRDD<String,List<List<String>>> utilPredictor=utilpartition.repartition(1).sortByKey();
		 //utilpartition.saveAsTextFile("/home/system/kranthisai/hellop");
		//utilPredictor.saveAsTextFile("/home/system/kranthisai/hello1");
		 JavaRDD<String> predictor=utilPredictor.mapPartitions(new Prediction(conf));
		 predictor.saveAsTextFile("utilpred");
		 
	 }
	
	 private static class Prediction implements FlatMapFunction<Iterator<Tuple2<String,List<List<String>>>>,String>{
		 private String fieldDelim;
	    
	    	private List<List<String>> ratingCorrelations = new ArrayList<List<String>>();
	    	private boolean linearCorrelation;
	    	private int correlationScale;
	    	private int maxRating;
	    	private String userID;
	    	private String itemID;
	    	private int rating;
	    	private String tempkey=null;
	    	private int ratingCorr;
	    	private int weight;
	    	//private long logCounter = 0;
	    	private double correlationModifier;
	    	//private Tuple ratingStat;
	    	private int ratingStdDev;
	    	private boolean userRatingWithContext;
	    	private String ratingContext;
	    	private List<String> finalprediction=new ArrayList<String>();
			private List<List<String>> value=new ArrayList<List<String>>();

			private List<String> userrating=new ArrayList<String>();

			//private Object ratingStat;
		public Prediction(SparkConf conf) {
			// TODO Auto-generated constructor stub
			fieldDelim = conf.get("field.delim", ",");
        	linearCorrelation = conf.getBoolean("correlation.linear", true);
        	correlationScale = conf.getInt("correlation.linear.scale", 1000);
           	maxRating = conf.getInt("max.rating", 100);
           	correlationModifier = conf.getDouble("correlation.modifier", (float)1.1);
        	userRatingWithContext = conf.getBoolean("user.rating.with.context", false);
		}

		public Iterable<String> call(
				Iterator<Tuple2<String, List<List<String>>>> t)
				throws Exception {
			// TODO Auto-generated method stub
			while(t.hasNext())
			{
				Tuple2<String, List<List<String>>> ratingcorr=t.next();
				String key=ratingcorr._1.split(",")[0];
				//System.out.println(ratingcorr._2.get(0));
				value=ratingcorr._2;
				//System.out.println(value.toString());
				
				if(ratingcorr._1.split(",")[1].contains("0"))
				{
					if (tempkey==null)
						tempkey=key;
					//System.out.println(key);
						if(key.matches(tempkey))
						{
							ratingCorrelations.add(ratingcorr._2.get(0));
						}
						else
						{
							ratingCorrelations = new ArrayList<List<String>>();
							ratingCorrelations.add(ratingcorr._2.get(0));
							tempkey=key;
						}
					
					
				}
				else if(ratingcorr._1.split(",")[1].matches("2"))
				{
					//System.out.println("key"+key+","+tempkey);
					if(!ratingCorrelations.isEmpty() && key.matches(tempkey))
					{
						//System.out.println("from else hiiiiiiiiiiiiiiiiiiiii");
						for(int i=0;i<value.size();i++)
						{
							
						String userID = value.get(i).get(0).split(",")[0];
						//System.out.println(value.get(i));
	           			rating = Integer.parseInt(value.get(i).get(0).split(",")[1]);
	           			if (userRatingWithContext) {
	           				ratingContext = value.get(i).get(0).split(",")[2];
	           			}
	           			
	           			for (List<String> ratingCorrTup : ratingCorrelations)
	           			{
	           				itemID = ratingCorrTup.get(0).split(",")[0];
	           				ratingCorr = Integer.parseInt(ratingCorrTup.get(0).split(",")[1]);
	           				weight = Integer.parseInt(ratingCorrTup.get(0).split(",")[2]);
	           				modifyCorrelation();
	           				//System.out.println(ratingCorrelations);
	           				//System.out.println(userID+","+rating+","+itemID+","+ratingCorr+","+weight+","+correlationModifier);
	           				int predRating = linearCorrelation? (rating * ratingCorr) / maxRating : 
	           					(rating  * correlationScale + ratingCorr) /maxRating ;
	           				if (predRating > 0) {
	           					//userID, itemID, predicted rating, correlation length, correlation coeff, input rating std dev
	           					//ratingStdDev = ratingStat != null ? ((Object) ratingStat).getInt(0) :  -1;
	    	           			ratingStdDev=-1;
	           					if (userRatingWithContext) 
	    	           				finalprediction.add(userID + fieldDelim + itemID + fieldDelim + ratingContext + fieldDelim + 
		    	           					predRating + fieldDelim + weight + fieldDelim +ratingCorr  + fieldDelim + ratingStdDev);
	    	           				else
	    	           					finalprediction.add(userID + fieldDelim + itemID + fieldDelim + predRating + fieldDelim + weight + 
	           							fieldDelim +ratingCorr  + fieldDelim + ratingStdDev);
	    	           			
	    	           			}
	           				
	           			}
						}
	           			
					}
					
						ratingCorrelations = new ArrayList<List<String>>();
				}
			}
			return finalprediction;
		}

		private void modifyCorrelation() {
			// TODO Auto-generated method stub
			double ratingCorrDb  =( (double)ratingCorr) / correlationScale;
        	ratingCorrDb = Math.pow(ratingCorrDb, correlationModifier);
        	ratingCorr = (int)(ratingCorrDb * correlationScale);
			
		}
		 
	 }

	private static class Ratingclass implements PairFlatMapFunction<Iterator<String>,String,List<List<String>>>{
		 
		 private int minInputRating;
	 	 private  int inputRating;
	 	 private int minCorrelation=-1;
	 	private String fieldDelim;
		private String subFieldDelim;
		private Integer two = 2;
		private Integer one = 1;
		private Integer zero = 0;
		private boolean linearCorrelation;
		private boolean userRatingWithContext;
		private int ratingTimeWindow;
		private long ratingTimeCutoff;
		private boolean isRatingStatFileSplit;
		private boolean isRatingFileSplit;
		private ArrayList<String> userratings;
		private ArrayList<List<String>> userratingstemp;
		private String[] ratings;
		private long timeStamp;
		private String ratingContext;
		private List<Tuple2<String,List<List<String>>>> useritempairs=new ArrayList<Tuple2<String,List<List<String>>>>();
		private int correlation;
		private int correlationLength;
		
		Ratingclass(SparkConf conf)
		{
			
			String[] file=path.split("/");
			if(file[file.length-1].contains("ratings"))
			{
				isRatingFileSplit=true;
			}
			else
				isRatingStatFileSplit=true;
			
			fieldDelim=conf.get("field.delim",",");
			subFieldDelim = conf.get("sub.field.delim", ":");
			linearCorrelation=conf.getBoolean("correlation.linear", true);
			minInputRating = conf.getInt("min.input.rating",  -1);
	    	minCorrelation = conf.getInt("min.correlation",  -1);
	    	userRatingWithContext = conf.getBoolean("user.rating.with.context", false);
	    	ratingTimeWindow = conf.getInt("rating.time.window.hour",  -1);
	    	ratingTimeCutoff = ratingTimeWindow > 0 ?  System.currentTimeMillis() / 1000 - ratingTimeWindow * 60L * 60L : -1;
		}
		public Iterable<Tuple2<String,List<List<String>>>> call(Iterator<String> s)
				throws Exception {
			// TODO Auto-generated method stub
			

				while(s.hasNext())
				{
					userratings=new ArrayList<String>();
					userratingstemp=new ArrayList<List<String>>();
					String[] items=s.next().split(fieldDelim);
					String itemid=items[0];
					//System.out.println(items[items.length-1]);
					if (isRatingFileSplit && items[items.length-1].equals("1")) 
					{
						boolean toInclude = true;
						for (int i = 1; i < items.length-1; ++i) 
						{
							ratings = items[i].split(subFieldDelim);
							toInclude = true; 
							if (ratingTimeCutoff > 0) {
		            			 timeStamp = Long.parseLong(ratings[2]);
		            			 toInclude = timeStamp > ratingTimeCutoff;
		            		 }
							if (userRatingWithContext) {
		            			 ratingContext = ratings[3];
		            		 }
							inputRating = new Integer(ratings[1]);
		            		toInclude = toInclude && inputRating > minInputRating;
		            		if (toInclude) 
		            			{
		            				userratings=new ArrayList<String>();
		            				userratings.add(ratings[0]+","+inputRating+","+two);
		            				userratingstemp.add(userratings);
		            				
		            			}
		            		
		            	}
						Tuple2<String,List<List<String>>> useritem=new Tuple2<String,List<List<String>>>(itemid+","+2,userratingstemp);
						useritempairs.add(useritem);
		            			
					}
					else
					{
						itemid=itemid.replace("(","");
						correlation =  Integer.parseInt( items[2]);
		        		correlationLength =  Integer.parseInt(items[3].replace(")",""));
		        		if (correlation > minCorrelation) {
		        			if (linearCorrelation) {
		        				userratings=new ArrayList<String>();
			   	   				//other itemID, correlation, intersection length (weight)
			   	   				userratings.add(items[1]+","+correlation+","+correlationLength+","+zero);
			   	   			} else {
			   	   				//other itemID, correlation, intersection length (weight)
			   	   				userratings=new ArrayList<String>();
			   	   				userratings.add(items[1]+","+correlation+","+correlationLength+","+zero);
			   	   			}
		        			userratingstemp.add(userratings);
		        			Tuple2<String,List<List<String>>> useritem=new Tuple2<String,List<List<String>>>(itemid+","+0,userratingstemp);
							useritempairs.add(useritem);
							userratingstemp=new ArrayList<List<String>>();
							//second item
							if (linearCorrelation) {
		        				userratings=new ArrayList<String>();
			   	   				//other itemID, correlation, intersection length (weight)
			   	   				userratings.add(itemid+","+correlation+","+correlationLength+","+zero);
			   	   			} else {
			   	   			userratings=new ArrayList<String>();
			   	   				//other itemID, correlation, intersection length (weight)
			   	   				userratings.add(itemid+","+correlation+","+correlationLength+","+zero);
			   	   			}
							userratingstemp.add(userratings);
		        			useritem=new Tuple2<String,List<List<String>>>(items[1]+","+0,userratingstemp);
							useritempairs.add(useritem);
		        		}
		        		
					}
					}
				return useritempairs;
			}		
		}
		 
	 }
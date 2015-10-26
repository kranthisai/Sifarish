package org.sifarish.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.sifarish.feature.CosineSimilarity;
import org.sifarish.feature.DynamicAttrSimilarityStrategy;

import scala.Tuple2;

public class Itemrec {

	 public static void main(String[] args) {
		    SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
			  //SparkConf conf = new SparkConf().setAppName("Collaborative Filtering").setMaster("local");
		   final int minIntersectionLength=2;
		    JavaSparkContext sc = new JavaSparkContext(conf);
			  String path ="ratings";
			  JavaRDD<String> data = sc.textFile(path);
			  //JavaPairRDD<String,List<Integer>> RatingMatrix=data.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,List<Integer>>>>>)
			  JavaPairRDD<String,String> ratings = data.mapToPair(
				        new PairFunction<String,String,String>() {
				          public Tuple2<String,String> call(String r){
				        	  String[] sarray = r.split(",",2);
				        	  return new Tuple2<String,String>(sarray[0],sarray[1]);
				          }
				        }
				        );
			  
			  //ratings.saveAsTextFile("/home/system/kranthisai/kt.txt");
			  
			  JavaPairRDD<String,List<Integer>> RatingMatrix=ratings.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String,String>>,String,List<Integer>>()
					  {
				  public Iterable<Tuple2<String, List<Integer>>> call(
							Iterator<Tuple2<String,String>> s)
							throws Exception 
							
						{
					  		List<String> users=new ArrayList<String>();
					  		List<Tuple2<String,String>> iterator1=new ArrayList<Tuple2<String,String>>();
					  		List<Tuple2<String,List<Integer>>> ratingitempair=new ArrayList<Tuple2<String,List<Integer>>>();
					  		while(s.hasNext())
					  		{
					  			Tuple2<String,String> itemuserpair=s.next();
					  			iterator1.add(itemuserpair);
					  			String user[]=itemuserpair._2.split(",");
					  			for(int i=0;i<user.length;i++)
					  			{
					  				if(!users.contains(user[i].split(":")[0]))
					  				users.add(user[i].split(":")[0]);
					  			}
					  		}
					  		
					  		
					  		//List<Integer> tempratingbyuser=new ArrayList<Integer>();
					  		//tempratingbyuser=ratingbyuser;
					  		for(int j=0;j<iterator1.size();j++)
					  		{
					  			List<Integer> ratingbyuser=new ArrayList<Integer>();
					  			for (int k=0;k<users.size();k++)
					  				ratingbyuser.add(0);
					  			Tuple2<String,String> itemuserpair1=iterator1.get(j);
					  			String[] userratings=itemuserpair1._2.split(",");
					  			String item=itemuserpair1._1;
					  			for(int i=0;i<userratings.length;i++)
					  			{
					  				String user=userratings[i].split(":")[0];
					  				Integer rating=Integer.parseInt(userratings[i].split(":")[1]);
					  				int indexofuser=users.indexOf(user);
					  				ratingbyuser.set(indexofuser, rating);
					  				
					  			}
					  			Tuple2<String,List<Integer>> ratingmat = new Tuple2<String,List<Integer>>(item,ratingbyuser);
					  			
					  			ratingitempair.add(ratingmat);
					  			
					  		}
								return ratingitempair;
					  
					  
				  		}
				  
				  
					  });
			 // RatingMatrix.saveAsTextFile("/home/system/kranthisai/fop");
			  JavaRDD<Tuple2<String,String>> cosinesimilarity=RatingMatrix.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,List<Integer>>>, Tuple2<String,String>>(){

				public Iterable<Tuple2<String, String>> call(
						Iterator<Tuple2<String, List<Integer>>> t)
						throws Exception {
					// TODO Auto-generated method stub
					List<Tuple2<String,List<Integer>>> itemratingpairs=new ArrayList<Tuple2<String,List<Integer>>>();
					
					List<Tuple2<String,String>> itemcosinesimilarity=new ArrayList<Tuple2<String,String>>();
					Map<String, Object> params = new HashMap<String, Object>();
					DynamicAttrSimilarityStrategy simStrategy;
					simStrategy=DynamicAttrSimilarityStrategy.createSimilarityStrategy("cosine");
					
					while(t.hasNext())
					{
						itemratingpairs.add(t.next());
					}
					for(int i=0;i<itemratingpairs.size();i++)
					{
						for(int j=i+1;j<itemratingpairs.size();j++)
						{
							
							Double dist=(simStrategy.findDistance(itemratingpairs.get(i)._2,itemratingpairs.get(j)._2))*1000;
									dist = dist < 0.0 ? 0.0 : dist;
							int Length=simStrategy.getIntersectionLength();
							int corr_value=dist.intValue();
							if(Length >= minIntersectionLength)
							{
							String item1=itemratingpairs.get(i)._1;
							String item2=itemratingpairs.get(j)._1;
							String items=item1+","+item2;
							String distLength=corr_value+","+Length;
							Tuple2<String,String> itemdistpairs=new Tuple2<String,String>(items,distLength);
							itemcosinesimilarity.add(itemdistpairs);
							}
						}
					}
					return itemcosinesimilarity;
				}				  
			  });
			  cosinesimilarity.saveAsTextFile("corealtion"); 
	 }	 
}


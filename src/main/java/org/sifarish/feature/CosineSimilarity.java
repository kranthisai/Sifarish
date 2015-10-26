package org.sifarish.feature;

import java.util.List;

public class CosineSimilarity extends DynamicAttrSimilarityStrategy{
	@Override
	public double findDistance(List<Integer> src, List<Integer> target) {
		intersectionLength = 0;
		double distance=0,a=0,b=0,c=0;
		//System.out.println("hi i am in cosine class");
		for(int i=0;i<src.size();i++)
		{
			if(src.get(i)!=0 && target.get(i)!=0)
				intersectionLength++;
			a+=src.get(i)*src.get(i);
			b+=target.get(i)*target.get(i);
			c+=src.get(i)*target.get(i);
		}
		if (a==0 || b==0)
			distance=0;
		else
			distance=c/(Math.sqrt(a)*Math.sqrt(b));
		return distance;
	}
	
	

}

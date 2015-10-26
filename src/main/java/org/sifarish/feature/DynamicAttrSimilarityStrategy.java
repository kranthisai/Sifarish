package org.sifarish.feature;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class DynamicAttrSimilarityStrategy {
	
	protected  int intersectionLength;

	public int getIntersectionLength() {
		return intersectionLength;
	}

	
	public  double findDistance(String srcEntityID, String src, String targetEntityID, String target, String groupingID)  throws IOException {
		return 1.0;
	}

	public double findDistance(List<Integer> src, List<Integer> target) {
		// TODO Auto-generated method stub
		return 1.0;
	}
	
	public static DynamicAttrSimilarityStrategy createSimilarityStrategy(String algorithm) 
			throws IOException {
			DynamicAttrSimilarityStrategy  simStrategy = null;
			if (algorithm.equals("jaccard")){
				//double srcNonMatchingTermWeight = (Double)params.get("srcNonMatchingTermWeight");
				//double trgNonMatchingTermWeight = (Double)params.get("trgNonMatchingTermWeight");
				//simStrategy = new JaccardSimilarity();
			}
			 else if (algorithm.equals("cosine")){
					simStrategy = new CosineSimilarity();
			 }
			 else {
					throw new IllegalArgumentException("invalid text similarity algorithms:" + algorithm);
				}
				return simStrategy;
			
			
	}
}

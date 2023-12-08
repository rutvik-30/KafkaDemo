package com.example.kafkaDemo.data;

public class AggregationResult {
	
        int bot = 0;
        int non_bot = 0;
        int isBlock = 0;
        
        
		@Override
		public String toString() {
			return "AggregationResult [bot=" + bot + ", non_bot=" + non_bot + ", isBlock=" + isBlock + "]";
		}
        
        
}

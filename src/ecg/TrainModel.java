package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;
import utils.Pair;
import utils.functions.Func2;

import java.util.function.Function;

public class TrainModel {

	// The average value of the signal l[n] over the entire input.
	public static Query<Integer, Double> qLengthAvg() {
		// Use PeakDetection.qLength() to get the length of the curve for each point
		Query<Integer, Double> lengthQuery = PeakDetection.qLength();

		// Define the fold function to calculate the sum and count for averaging
		Func2<Pair<Double, Long>, Double, Pair<Double, Long>> foldFunc = (acc, value) -> {
			double sum = acc.getLeft() + value;
			long count = acc.getRight() + 1;
			return new Pair<>(sum, count);
		};

		// Initialize the fold operation with a zero sum and zero count
		Query<Double, Pair<Double, Long>> foldQuery = Q.fold(Pair.from(0.0, 0L), foldFunc);

		// Map the accumulated results to the average
		Query<Pair<Double, Long>, Double> avgQuery = Q.map(acc -> {
			if (acc.getRight() > 0) {
				//System.out.println("Average length count: " +  acc.getRight());
				return acc.getLeft() / acc.getRight();
			} else {
				return 0.0;  // Avoid division by zero
			}
		});

		// Pipeline all these steps
		return Q.pipeline(lengthQuery, foldQuery, avgQuery);
	}

	public static void main(String[] args) {
		System.out.println("***********************************************");
		System.out.println("***** Algorithm for finding the threshold *****");
		System.out.println("***********************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100-samples-200.csv"), qLengthAvg(), S.printer());
	}

}

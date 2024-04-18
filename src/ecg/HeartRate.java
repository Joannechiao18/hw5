package ecg;

import dsl.S;
import dsl.Q;
import dsl.Query;
import dsl.Sink;
import utils.functions.Func2;

import java.util.Objects;
import java.util.function.Function;

import static ecg.PeakDetection.qPeaks;

// This file is devoted to the analysis of the heart rate of the patient.
// It is assumed that PeakDetection.qPeaks() has already been implemented.

public class HeartRate {
	// RR interval length (in milliseconds)
	public static Query<Integer, Double> qIntervals() {
		// Fetch the peak timestamps
		Query<Integer, Long> peakTimes = qPeaks();

		// Define a function to calculate differences between consecutive timestamps
		Func2<Double, Long, Double> calculateIntervals = new Func2<Double, Long, Double>() {
			private Long previousTimestamp = null;

			@Override
			public Double apply(Double ignore, Long currentTimestamp) {
				if (previousTimestamp == null) {
					previousTimestamp = currentTimestamp;
					return Double.NaN;  // Return NaN for the first calculation to indicate no interval
				} else {
					double interval = currentTimestamp - previousTimestamp;
					interval *= 2.78;  // Convert the interval to milliseconds
					previousTimestamp = currentTimestamp;
					return interval;
				}
			}
		};

		// Use Q.scan to calculate RR intervals from peak timestamps
		Query<Long, Double> intervalQuery = Q.scan(Double.NaN, calculateIntervals);

		// Filter out the initial NaN and null values
		return Q.pipeline(peakTimes, intervalQuery, Q.filter(d -> d != null && !Double.isNaN(d)));
	}

	// Average heart rate (over entire signal) in bpm.
	public static Query<Integer, Double> qHeartRateAvg() {
		// Assume qIntervals() provides RR intervals as Double values (milliseconds)
		Query<Integer, Double> rrIntervals = qIntervals();

		// Transform RR intervals to heart rates in BPM using a map function directly
		Query<Integer, Double> heartRates = Q.map(rr -> {
			if (rr != null && rr > 0) {
				return 60000.0 / rr;  // Converts intervals to heart rates
			}
			return null;  // Handle invalid intervals
		});

		// Compute the average heart rate
		Query<Integer, Double> averageHeartRate = Q.pipeline(heartRates, Q.foldAvg());

		return averageHeartRate;
	}

	// Standard deviation of NN interval length (over the entire signal)
	// in milliseconds.
	public static Query<Integer, Double> qSDNN() {
		// Fetch the RR intervals
		Query<Integer, Double> rrIntervals = qIntervals();

		// Define a query to accumulate the necessary data for standard deviation calculation
		Query<Integer, double[]> accumulatedData = Q.scan(new double[]{0, 0, 0}, (acc, rr) -> {
			if (rr != null) {
				acc[0] += rr;          // Sum
				acc[1] += rr * rr;     // Sum of squares
				acc[2] += 1;           // Count
			}
			return acc;
		});

		// Define a query to calculate the standard deviation from the accumulated data
		Query<double[], Double> standardDeviationQuery = new Query<double[], Double>() {
			double[] acc = null;

			@Override
			public void start(Sink<Double> sink) {
				// Do nothing at the start
			}

			@Override
			public void next(double[] acc, Sink<Double> sink) {
				this.acc = acc; // Store the accumulated data
			}

			@Override
			public void end(Sink<Double> sink) {
				// Calculate the standard deviation after all data points have been processed
				if (acc != null && acc[2] > 1) { // More than one data point is needed for standard deviation
					double mean = acc[0] / acc[2];
					double meanSq = acc[1] / acc[2];
					sink.next(Math.sqrt(meanSq - (mean * mean)));
				}
			}
		};

		// Map the accumulated data to standard deviation values
		return Q.pipeline(accumulatedData, standardDeviationQuery);
	}


	// RMSSD measure (over the entire signal) in milliseconds.
	public static Query<Integer, Double> qRMSSD() {
		// Fetch the RR intervals
		Query<Integer, Double> rrIntervals = qIntervals();

		// Define a function to calculate differences between consecutive intervals
		Func2<Double, Double, Double> calculateDifferences = new Func2<Double, Double, Double>() {
			private Double previousInterval = null;

			@Override
			public Double apply(Double ignore, Double currentInterval) {
				if (previousInterval == null) {
					previousInterval = currentInterval;
					return Double.NaN;  // Return NaN for the first calculation to indicate no difference
				} else {
					double difference = currentInterval - previousInterval;
					previousInterval = currentInterval;
					return difference * difference;  // Return the squared difference
				}
			}
		};

		// Use Q.scan to calculate squared differences from RR intervals
		Query<Double, Double> squaredDifferences = Q.scan(Double.NaN, calculateDifferences);

		// Filter out the initial NaN value
		Query<Double, Double> filteredDifferences = Q.filter(d -> d != null && !Double.isNaN(d));

		// Compute the mean of the squared differences
		Query<Double, Double> meanSquaredDifference = Q.foldAvg();

		// Compute the square root of the mean squared difference
		Query<Double, Double> rmssd = Q.map(Math::sqrt);

		// Combine the queries into a pipeline
		return Q.pipeline(rrIntervals, squaredDifferences, filteredDifferences, meanSquaredDifference, rmssd);
	}

	public static class Counters {
		public long nn50;
		public long totalIntervals;

		public Counters(long nn50, long totalIntervals) {
			this.nn50 = nn50;
			this.totalIntervals = totalIntervals;
		}
	}

	// Proportion (in %) derived by dividing NN50 by the total number
	// of NN intervals (calculated over the entire signal).
	public static Query<Integer, Double> qPNN50() {
		// Fetch the RR intervals
		Query<Integer, Double> rrIntervals = qIntervals();

		// Define a function to calculate differences between consecutive intervals
		Func2<Double, Double, Double> calculateDifferences = new Func2<Double, Double, Double>() {
			private Double previousInterval = null;

			@Override
			public Double apply(Double ignore, Double currentInterval) {
				if (previousInterval == null) {
					previousInterval = currentInterval;
					return Double.NaN;  // Return NaN for the first calculation to indicate no difference
				} else {
					double difference = Math.abs(currentInterval - previousInterval);
					previousInterval = currentInterval;
					return difference;  // Return the difference
				}
			}
		};

		// Use Q.scan to calculate differences from RR intervals
		Query<Double, Double> differences = Q.scan(Double.NaN, calculateDifferences);

		// Filter out the initial NaN value
		Query<Double, Double> filteredDifferences = Q.filter(d -> d != null && !Double.isNaN(d));

		// Count the differences over 50 ms and total intervals
		Query<Double, Counters> countersQuery = Q.fold(new Counters(0L, 0L), (counters, diff) -> {
			counters.totalIntervals++;
			if (diff > 50) {
				counters.nn50++;
			}
			return counters;
		});
		// Map the final accumulation to the percentage
		Query<Counters, Double> percentage = Q.map(t -> (t.nn50 / (double) t.totalIntervals) * 100);

		return Q.pipeline(rrIntervals, differences, filteredDifferences, countersQuery, percentage);
	}


	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for the Heart Rate *****");
		System.out.println("****************************************");
		System.out.println();

		System.out.println("***** Intervals *****");
		Q.execute(Data.ecgStream("100.csv"), qIntervals(), S.printer());
		System.out.println();

		System.out.println("***** Average heart rate *****");
		Q.execute(Data.ecgStream("100-all.csv"), qHeartRateAvg(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: SDNN *****");
		Q.execute(Data.ecgStream("100-all.csv"), qSDNN(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: RMSSD *****");
		Q.execute(Data.ecgStream("100-all.csv"), qRMSSD(), S.printer());
		System.out.println();

		System.out.println("***** HRV Measure: pNN50 *****");
		Q.execute(Data.ecgStream("100-all.csv"), qPNN50(), S.printer());
		System.out.println();
	}

}

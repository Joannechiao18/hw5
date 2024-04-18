package ecg;

import dsl.*;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function; // Make sure to import the Function interface

import utils.functions.Func1;
import utils.functions.Func2;
import utils.functions.Func3;

public class PeakDetection {

	// The curve length transformation:
	//
	// adjust: x[n] = raw[n] - 1024
	// smooth: y[n] = (x[n-2] + x[n-1] + x[n] + x[n+1] + x[n+2]) / 5
	// deriv: d[n] = (y[n+1] - y[n-1]) / 2
	// length: l[n] = t(d[n-w]) + ... + t(d[n+w]), where
	// w = 20 (samples) and t(d) = sqrt(1.0 + d * d)

	private static final int WINDOW_WIDTH = 20; // Given as the width for length calculation


	public static Query<Integer, Double> qLength() {

		/*Query<Integer, Double>adjust=Q.map(x->(double)x-1024);
		Query<Double, Double>smooth=Q.sWindowInv(5,0.0,(agg,newItem)->agg+newItem/5, (agg, oldItem)->agg-oldItem/5);
		Query<Double, Double>derivative=Q.sWindow3((prev,curr,next)->(next-prev)/2);
		Query<Double, Double>length=Q.sWindowInv(41, 0.0, (agg, newItem)->agg+Math.sqrt(1.0+newItem*newItem)
				, (agg, oldItem)->agg-Math.sqrt(1.0+oldItem*oldItem));
		return Q.pipeline(adjust,smooth,derivative,length);*/

		Query<Integer, Double> adjust = Q.map(x -> (double) x - 1024);

		Query<Double, Double> smooth = new CenteredSlidingWindow<>(5, 0.0,
				(agg, newItem) -> agg + newItem / 5,
				(agg, oldItem) -> agg - oldItem / 5);

		Query<Double, Double> derivative = Q.sWindow3((prev, curr, next) -> (next - prev) / 2);

		Query<Double, Double> length = new CenteredSlidingWindow<>(41, 0.0,
				(agg, newItem) -> agg + Math.sqrt(1.0 + newItem * newItem),
				(agg, oldItem) -> agg - Math.sqrt(1.0 + oldItem * oldItem));

		return Q.pipeline(adjust, smooth, derivative, length);

	}

	// In order to detect peaks we need both the raw (or adjusted)
	// signal and the signal given by the curve length transformation.
	// Use the datatype VTL and implement the class Detect.
	private static long ts;

	public static Query<Integer, Long> qPeaks() {
		ts=0;
		Func2<Integer, Double, VTL>combineToVTL=(raw, length)->new VTL(
				raw, ts++, length);

		Query<Integer, Integer>orinalStream=Q.id();
		Query<Integer, Double>lengthStream=qLength();
		Query<Integer, VTL>vtlstream=Q.parallel(orinalStream, lengthStream, combineToVTL);
		Detect detect=new Detect();

		return Q.pipeline(vtlstream, detect);
	}

	public static void main(String[] args) {
		System.out.println("****************************************");
		System.out.println("***** Algorithm for Peak Detection *****");
		System.out.println("****************************************");
		System.out.println();

		Q.execute(Data.ecgStream("100-samples-1000.csv"), qPeaks(), S.printer());
	}

}

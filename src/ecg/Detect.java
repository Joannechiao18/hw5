package ecg;

import dsl.*;
import utils.Pair;

import java.util.LinkedList;


// The detection algorithm (decision rule) that we described in class
// (or your own slight variant of it).
//
// (1) Determine the threshold using the class TrainModel.
//
// (2) When l[n] exceeds the threshold, search for peak (max x[n] or raw[n])
//     in the next 40 samples.
//
// (3) No peak should be detected for 72 samples after the last peak.
//
// OUTPUT: The timestamp of each peak.

public class Detect implements Query<VTL, Long> {
	private double THRESHOLD;
	private int ignoreCount = 0;
	private Pair<Long, Integer> peak = Pair.from(0L, Integer.MIN_VALUE);
	private LinkedList<VTL> sampleBuffer = new LinkedList<>();
	private boolean isBuffering = false;

	public Detect() {
		this.THRESHOLD = determineThreshold();
	}

	// Choose this to be two times the average length over the entire signal.
	private static double determineThreshold() {
		System.out.println("Determining threshold for peak detection.");
		Query<Integer, Double> avgQuery = TrainModel.qLengthAvg();
		SCollector<Double> collector = new SCollector<>();
		Q.execute(Data.ecgStream("100-samples-200.csv"), avgQuery, collector);
		double avgLength = collector.list.stream().mapToDouble(val -> val).average().orElse(0.0);
		//System.out.println("Average length: " + avgLength);
		return avgLength * 2;
	}

	@Override
	public void start(Sink<Long> sink) {
		//System.out.println("Threshold set to: " + THRESHOLD);
		reset();
	}

	private void reset() {
		ignoreCount = 0;
		peak = Pair.from(0L, Integer.MIN_VALUE);
		sampleBuffer.clear();
		isBuffering = false;
	}

	@Override
	public void next(VTL item, Sink<Long> sink) {
		//System.out.println("Processing item: l=" + item.l + ", v=" + item.v + ", ts=" + item.ts);
		if (ignoreCount > 0) {
			//System.out.println("In cooldown period, ignoring item.");
			ignoreCount--;
			return;
		}

		// Check if the item's length exceeds the threshold and start buffering if not already active
		if (item.l > THRESHOLD && !isBuffering) {
			//System.out.println("Item length exceeds threshold, starting to buffer. Triggering item details: l=" + item.l + ", v=" + item.v + ", ts=" + item.ts);
			isBuffering = true;
			sampleBuffer.add(item);  // Include the triggering item in the buffer
			//System.out.println("Adding triggering item to buffer. Buffer size is now: 1. Item details: l=" + item.l + ", v=" + item.v + ", ts=" + item.ts);
		} else if (isBuffering) {
			sampleBuffer.add(item);
			//System.out.println("Adding item to buffer. Current buffer size: " + sampleBuffer.size() + ". Item details: l=" + item.l + ", v=" + item.v + ", ts=" + item.ts);

			// Check if we have collected 40 samples, including the triggering one
			if (sampleBuffer.size() == 40) {
				//System.out.println("Buffer full. Processing for peaks.");
				findPeakInBuffer(sink);
				sampleBuffer.clear();
				isBuffering = false;
			}
		}
	}

	private void findPeakInBuffer(Sink<Long> sink) {
		VTL highest = null;
		for (VTL vtl : sampleBuffer) {
			if (highest == null || vtl.v > highest.v) {
				highest = vtl; // Find the highest peak in the buffer based on item's value
			}
		}

		//System.out.println("Highest peak in buffer: " + highest);
		//System.out.println("Current peak: " + peak);

		peak= Pair.from(0L, Integer.MIN_VALUE);

		//If highest is not null, which means that there are samples in the buffer.
		//If the value (v) of the highest sample is greater than the value of the current peak (peak.getRight()), or if no peak has been found yet (peak.getRight() == Integer.MIN_VALUE).
		if (highest != null && (peak.getRight() == Integer.MIN_VALUE || highest.v > peak.getRight())) {
			peak = Pair.from(highest.ts, highest.v);
			System.out.println("New peak found: " + peak.getLeft() + " at value: " + peak.getRight() + ", with full item details: l=" + highest.l + ", v=" + highest.v + ", ts=" + highest.ts);
			sink.next(peak.getLeft());
			ignoreCount = 72; // Set cooldown period after detecting a peak
		} else {
			System.out.println("No new peak found or existing peak is higher.");
		}
	}


	@Override
	public void end(Sink<Long> sink) {
		if (!sampleBuffer.isEmpty()) {
			System.out.println("End of data stream. Processing remaining data in buffer.");
			findPeakInBuffer(sink);
		}
		sink.end();
	}
}
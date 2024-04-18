package ecg;

import dsl.Query;
import dsl.Sink;

import java.util.function.BinaryOperator;

public class CenteredSlidingWindow<A> implements Query<A, A> {

    private final A init;  // Neutral element representing zero
    private final BinaryOperator<A> insert;
    private final BinaryOperator<A> remove;
    private final int wndSize; // window size
    private final A[] buffer;
    private A agg; // current aggregate
    private int indexOldest; // index to oldest element
    private int nElements; // number of elements in buffer

    public CenteredSlidingWindow(int wndSize, A init, BinaryOperator<A> insert, BinaryOperator<A> remove) {
        if (wndSize < 1) {
            throw new IllegalArgumentException("Window size should be >= 1");
        }
        this.init = init;
        this.insert = insert;
        this.remove = remove;
        this.wndSize = wndSize;
        this.buffer = (A[]) new Object[wndSize];
        this.agg = init;
        this.indexOldest = 0;
        this.nElements = 0;
    }

    @Override
    public void start(Sink<A> sink) {
        //System.out.println("in centeredSlidingWindow start method");
        // Pre-fill half of the window with 'init' (zeros)
        for (int i = 0; i < wndSize / 2; i++) {
            buffer[i] = init;
            indexOldest = (indexOldest + 1) % wndSize;  // Prepare the indexOldest to start from the middle
        }
    }

    @Override
    public void next(A item, Sink<A> sink) {
        int indexNew = (indexOldest + nElements) % wndSize;

        if (nElements >= wndSize) { // The window is fully populated
            agg = remove.apply(agg, buffer[indexOldest]);
            buffer[indexOldest] = item;
            indexOldest = (indexOldest + 1) % wndSize;
        } else { // Initially filling the window
            buffer[indexNew] = item;
            nElements++;
        }

        agg = insert.apply(agg, item);  // Update aggregate

        // Calculate and output the aggregate only if the window is conceptually full
        if (nElements >= wndSize / 2 + 1) {
            sink.next(agg);
            //System.out.println("Output aggregate at index: " + (nElements - wndSize / 2 - 1) + ", Aggregate: " + agg);
        }
    }

    @Override
    public void end(Sink<A> sink) {
        sink.end();
    }
}
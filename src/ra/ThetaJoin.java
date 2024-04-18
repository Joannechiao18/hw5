package ra;

import java.util.function.BiPredicate;
import java.util.ArrayList;
import java.util.List;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

public class ThetaJoin<A,B> implements Query<Or<A,B>,Pair<A,B>> {

	private final BiPredicate<A,B> theta;
	private final List<A> leftItems = new ArrayList<>();
	private final List<B> rightItems = new ArrayList<>();

	private ThetaJoin(BiPredicate<A,B> theta) {
		this.theta = theta;
	}

	public static <A,B> ThetaJoin<A,B> from(BiPredicate<A,B> theta) {
		return new ThetaJoin<>(theta);
	}

	@Override
	public void start(Sink<Pair<A,B>> sink) {
		// Optionally reset the state if required
		leftItems.clear();
		rightItems.clear();
	}

	@Override
	public void next(Or<A,B> item, Sink<Pair<A,B>> sink) {
		if (item.isLeft()) {
			A a = item.getLeft();
			leftItems.add(a);
			// For each item in rightItems, if they satisfy the theta condition with 'a', emit the pair
			for (B b : rightItems) {
				if (theta.test(a, b)) {
					sink.next(Pair.from(a, b)); // Use static factory method
				}
			}
		} else if (item.isRight()) {
			B b = item.getRight();
			rightItems.add(b);
			// For each item in leftItems, if they satisfy the theta condition with 'b', emit the pair
			for (A a : leftItems) {
				if (theta.test(a, b)) {
					sink.next(Pair.from(a, b)); // Use static factory method
				}
			}
		}
	}


	@Override
	public void end(Sink<Pair<A,B>> sink) {
		// Any cleanup if necessary
	}
}

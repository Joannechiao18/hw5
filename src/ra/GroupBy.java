package ra;

import dsl.Query;
import dsl.Sink;
import utils.Pair;
import utils.functions.Func2;

import java.util.LinkedHashMap;
import java.util.Map;

public class GroupBy<K, A, B> implements Query<Pair<K, A>, Pair<K, B>> {

	private final B init;
	private final Func2<B, A, B> op;
	private Map<K, B> aggregates;
	private Map<K, Boolean> keyOrder;

	private GroupBy(B init, Func2<B, A, B> op) {
		this.init = init;
		this.op = op;
		this.aggregates = new LinkedHashMap<>(); // Preserves insertion order
		this.keyOrder = new LinkedHashMap<>();
	}

	public static <K, A, B> GroupBy<K, A, B> from(B init, Func2<B, A, B> op) {
		return new GroupBy<>(init, op);
	}

	@Override
	public void start(Sink<Pair<K, B>> sink) {
		// Reset state if needed. For new instances, this is effectively a no-op.
		aggregates.clear();
		keyOrder.clear();
	}

	@Override
	public void next(Pair<K, A> item, Sink<Pair<K, B>> sink) {
		K key = item.getLeft();
		A value = item.getRight();

		// Check if the key is new. If so, remember its order and initialize its aggregate.
		keyOrder.putIfAbsent(key, true);

		// Perform aggregation
		aggregates.compute(key, (k, currentAggregate) -> {
			if (currentAggregate == null) {
				// This is the first value for this key, so use the initial value as the base
				return op.apply(init, value);
			} else {
				// Apply the aggregate function to the current aggregate and the new value
				return op.apply(currentAggregate, value);
			}
		});
	}

	@Override
	public void end(Sink<Pair<K, B>> sink) {
		// Output all results in the order of their first occurrence
		keyOrder.forEach((key, value) -> {
			B aggregate = aggregates.get(key);
			sink.next(Pair.from(key, aggregate)); // Correctly use `next` instead of `put`
		});

		// Signify the end of data processing
		sink.end();
	}
}

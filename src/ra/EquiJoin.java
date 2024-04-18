package ra;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import dsl.Query;
import dsl.Sink;
import utils.Or;
import utils.Pair;

public class EquiJoin<A, B, T> implements Query<Or<A, B>, Pair<A, B>> {

	private final Function<A, T> f;
	private final Function<B, T> g;
	private final HashMap<T, List<A>> leftTable = new HashMap<>();
	private final HashMap<T, List<B>> rightTable = new HashMap<>();

	private EquiJoin(Function<A, T> f, Function<B, T> g) {
		this.f = f;
		this.g = g;
	}

	public static <A, B, T> EquiJoin<A, B, T> from(Function<A, T> f, Function<B, T> g) {
		return new EquiJoin<>(f, g);
	}

	@Override
	public void start(Sink<Pair<A, B>> sink) {
		// Initialization, if needed, can be done here
	}

	@Override
	public void next(Or<A, B> item, Sink<Pair<A, B>> sink) {
		if (item.isLeft()) {
			A a = item.getLeft(); // Get the left value
			T key = f.apply(a); // Apply the function to get the key
			leftTable.computeIfAbsent(key, k -> new LinkedList<>()).add(a);
			// If there are matching items in the right table, emit all matching pairs
			rightTable.getOrDefault(key, Collections.emptyList())
					.forEach(b -> sink.next(Pair.from(a, b)));
		} else if (item.isRight()) {
			B b = item.getRight(); // Get the right value
			T key = g.apply(b); // Apply the function to get the key
			rightTable.computeIfAbsent(key, k -> new LinkedList<>()).add(b);
			// If there are matching items in the left table, emit all matching pairs
			leftTable.getOrDefault(key, Collections.emptyList())
					.forEach(a -> sink.next(Pair.from(a, b)));
		}
	}


	@Override
	public void end(Sink<Pair<A, B>> sink) {
		// Finalization, if needed, can be done here
	}
}

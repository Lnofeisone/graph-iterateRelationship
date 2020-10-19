package iterateRelationship;

import java.util.Iterator;
import java.util.function.BiFunction;

public class GroupIterator<T> {

	private Iterator<T> iterator;
	private BiFunction<T, T, Boolean> inSameGroup;
	private T lastResult, resultBuffer;

	public GroupIterator(Iterator<T> iterator, BiFunction<T, T, Boolean> inSameGroup) {
		this.iterator = iterator;
		this.inSameGroup = inSameGroup;
	}

	public boolean hasNext() {
		return (iterator.hasNext() || resultBuffer != null);
	}

	public T next() {
		if (resultBuffer != null) {
			lastResult = resultBuffer;
			resultBuffer = null;
		} else {
			lastResult = iterator.next();
		}

		return lastResult;
	}

	public boolean nextIsInSameGroup() {
		if (resultBuffer == null) {
			resultBuffer = iterator.next();
		}

		return inSameGroup.apply(lastResult, resultBuffer);
	}
}

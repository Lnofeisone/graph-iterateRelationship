package iterateRelationship;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

// This class defines a set of bins.
// Records are first sorted into these bins, then inserted into the database from these bins.

public class BinSet {

	private int nThreads, nBins, nRounds;
	private int thisRound = 0;
	private long count = 0;
	private List<Map<String,Object>>[][] bins;
	private GroupIterator<Map<String,Object>>[][] iterators;


	@SuppressWarnings("unchecked")
	public BinSet(GroupIterator<Map<String, Object>> result, 
			BiFunction<Map<String, Object>, Map<String, Object>, Boolean> inSameGroup, 
			int batchsize, int concurrency, String sourceIdProperty, String targetIdProperty) {
		// Establish threads, bins, rounds based on parameters
		nThreads = Integer.highestOneBit(concurrency);	// must be a power of 2
		nBins = nThreads << 1;	// 2*nThreads
		nRounds = nBins << 1;	// 2*nBins = 4*nThreads

		// Construct 2-D array of bins
		bins = new List[nBins][nBins];
		for (int i = 0; i < nBins; i++) {
			for (int j = 0; j < nBins; j++) {
				bins[i][j] = new LinkedList<Map<String,Object>>();
			}
		}

		// Assign each result row to a bin, using sourceProperty and targetProperty to determine which bin
		int sourceBin;
		int targetBin;
		int mask = nBins - 1;
		long maxToLoad = batchsize * nBins * nBins;
		while (result.hasNext() && (count < maxToLoad || result.nextIsInSameGroup())) {
			Map<String,Object> row = result.next();

			Object sourceIdPropertyValue = row.get(sourceIdProperty);
			if (sourceIdPropertyValue instanceof Long) {
				sourceBin = ((Long) sourceIdPropertyValue).intValue() & mask;
			} else if (sourceIdPropertyValue instanceof String) {
				sourceBin = ((String) sourceIdPropertyValue).chars().sum() & mask;
			} else {
				sourceBin = sourceIdPropertyValue.hashCode() & mask;
			}

			Object targetIdPropertyValue = row.get(targetIdProperty);
			if (targetIdPropertyValue instanceof Long) {
				targetBin = ((Long) targetIdPropertyValue).intValue() & mask;
			} else if (targetIdPropertyValue instanceof String) {
				targetBin = ((String) targetIdPropertyValue).chars().sum() & mask;
			} else {
				targetBin = targetIdPropertyValue.hashCode() & mask;
			}

			bins[sourceBin][targetBin].add(row);
			count++;
		}

		// Get iterators for each bin and divide them into rounds
		iterators = new GroupIterator[nRounds][nThreads];
		for (int roundPair = 0; roundPair < nBins; roundPair++) {
			List<GroupIterator<Map<String,Object>>> iteratorsFirstRound = 
					new ArrayList<GroupIterator<Map<String,Object>>>(nThreads);
			List<GroupIterator<Map<String,Object>>> iteratorsSecondRound = 
					new ArrayList<GroupIterator<Map<String,Object>>>(nThreads);

			for (int x = 0; x < nBins; x++) {
				int y = x^roundPair;

				if (y < x || iteratorsFirstRound.size() >= nThreads) {
					iteratorsSecondRound.add(
							new GroupIterator<Map<String, Object>>(bins[x][y].iterator(), inSameGroup));
				} else {
					iteratorsFirstRound.add(
							new GroupIterator<Map<String, Object>>(bins[x][y].iterator(), inSameGroup));
				}
			}

			iterators[2*roundPair] = iteratorsFirstRound.toArray(new GroupIterator[nThreads]);
			iterators[2*roundPair+1] = iteratorsSecondRound.toArray(new GroupIterator[nThreads]);
		}
	}


	// Return true iff any iterator has data.
	public boolean hasData() {
		if (iterators == null) { return false; }
		for (int i = 0; i < iterators.length; i++) {
			for (int j = 0; j < iterators[i].length; j++) {
				if (iterators[i][j].hasNext()) { return true; }
			}
		}
		return false;
	}


	public boolean hasNextRound() { return (thisRound < nRounds); }

	public GroupIterator<Map<String,Object>>[] getNextRound() { return iterators[pickIterator(thisRound++)]; }

	public int pickIterator(int round) {
		int iterator = 0;
		int mask = nRounds - 1;

		if (round % 2 == 0) {
			iterator = round/2;
		} else {
			iterator = ((round-1)/2)^mask;
		}

		System.out.println("Using iterator " + iterator);
		return iterator;
	}

	public long getCount() { return count; }

	public double getEntropy() {
		double entropy = 0;

		for (int i = 0; i < nBins; i++) {
			for (int j = 0; j < nBins; j++) {
				double p = bins[i][j].size() / ((double) count);
				if (p > 0) {
					entropy += -p * Math.log(p);
				}
			}
		}

		return entropy / Math.log(2);
	}
}

package iterateRelationship;

import static apoc.util.Util.merge;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import apoc.Pools;
import apoc.periodic.Periodic;
import apoc.util.Util;

public class IterateRelationship extends Periodic {
	
	/**
     * invoke cypherAction in batched transactions being feeded from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     */
    @SuppressWarnings("unchecked")
	@Procedure(name = "apoc.periodic.iterateRelationship", mode = Mode.WRITE)
    @Description("apoc.periodic.iterateRelationship('statement returning items', 'statement per item', 'property of relationship source for binning', 'property of relationship target for binning', {batchSize:1000,iterateList:true,params:{},concurrency:50,retries:5}) YIELD batches, total - run the second statement for each item returned by the first statement. Returns number of batches and total processed rows")
    public Stream<BatchAndTotalResult> iterateRelationship(
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("sourceIdProperty") String sourceIdProperty,
            @Name("targetIdProperty") String targetIdProperty,
            @Name("config") Map<String,Object> config) {

        long batchSize = Util.toLong(config.getOrDefault("batchSize", 1000));
        int concurrency = Util.toInteger(config.getOrDefault("concurrency", 50));
        boolean iterateList = Util.toBoolean(config.getOrDefault("iterateList", true));
        boolean groupSameSourceTarget = Util.toBoolean(config.getOrDefault("groupSameSourceTarget", false));
        long retries = Util.toLong(config.getOrDefault("retries", 5)); // todo sleep/delay or push to end of batch to try again or immediate ?
        Map<String,Object> params = (Map<String, Object>)config.getOrDefault("params", Collections.emptyMap());
        int failedParams = Util.toInteger(config.getOrDefault("failedParams", -1));
        
        try (Result result = db.execute(slottedRuntime(cypherIterate),params)) {
            Pair<String,Boolean> prepared = prepareInnerStatement(cypherAction, iterateList, result.columns(), "_batch");
            String innerStatement = prepared.first();
            iterateList=prepared.other();
            log.info("starting batching from `%s` operation using iteration `%s` in separate thread", cypherIterate,cypherAction);
            return iterateRelationshipAndExecuteBatchedInSeparateThread((int)batchSize, iterateList, groupSameSourceTarget, retries, result, (p) -> db.execute(innerStatement, merge(params, p)).close(), concurrency, failedParams, sourceIdProperty, targetIdProperty);
        }
    }
    
    
    static String slottedRuntime(String cypherIterate) {
        if (RUNTIME_PATTERN.matcher(cypherIterate).find()) {
            return cypherIterate;
        }
        Matcher matcher = CYPHER_PREFIX_PATTERN.matcher(cypherIterate.substring(0, Math.min(15,cypherIterate.length())));
        return matcher.find() ? CYPHER_PREFIX_PATTERN.matcher(cypherIterate).replaceFirst(CYPHER_RUNTIME_SLOTTED) : CYPHER_RUNTIME_SLOTTED + cypherIterate;
    }
    
    
	private Stream<BatchAndTotalResult> iterateRelationshipAndExecuteBatchedInSeparateThread(int batchsize, boolean iterateList,
			boolean groupSameSourceTarget, long retries, Iterator<Map<String, Object>> incomingResult, 
			Consumer<Map<String, Object>> consumer, int concurrency, int failedParams, String sourceIdProperty, String targetIdProperty) {
    	
    	ExecutorService pool = Pools.DEFAULT;
    	long batches = 0;
    	long start = System.nanoTime();
    	AtomicLong count = new AtomicLong();
    	AtomicInteger failedOps = new AtomicInteger();
    	AtomicLong retried = new AtomicLong();
    	Map<String,Long> operationErrors = new ConcurrentHashMap<>();
    	AtomicInteger failedBatches = new AtomicInteger();
    	Map<String,Long> batchErrors = new HashMap<>();
    	Map<String, List<Map<String,Object>>> failedParamsMap = new ConcurrentHashMap<>();
    	long successes = 0;
    	
    	// Construct a GroupIterator to ensure that records with the same source and target
    	// end up in the same group.
    	BiFunction<Map<String, Object>, Map<String, Object>, Boolean> inSameGroup;
    	if (groupSameSourceTarget) {
    		inSameGroup = (row1, row2) -> (row1.get(sourceIdProperty).equals(row2.get(sourceIdProperty)) && 
	    			row1.get(targetIdProperty).equals(row2.get(targetIdProperty)));
    	} else {
    		inSameGroup = (row1, row2) -> false;
    	}
    	GroupIterator<Map<String, Object>> groupIncomingResult = 
    			new GroupIterator<Map<String, Object>>(incomingResult, inSameGroup);

    	List<IteratorThread> iteratorThreads = null;
    	Future<BinSet> incomingFuture = null;
    	BinSet outgoingBinSet = null;
    	
    	// Two things to do: (1) load results coming from the source and (2) put them in the graph.
    	// Keep going around as long as at least one of these things needs to be done.
    	mainLoop:
    	while (groupIncomingResult.hasNext() || (outgoingBinSet != null && outgoingBinSet.hasData())) {
    		if (Util.transactionIsTerminated(terminationGuard)) break mainLoop;
    		
    		System.out.println("Starting cycle");
    		printFreeMemory();

    		// (1) If there are results coming from the source, create a BinSet and load it.
    		if (groupIncomingResult.hasNext()) {
    			incomingFuture = pool.submit(() -> {
    				System.out.println("Loading new BinSet");
    				printFreeMemory();
    				BinSet incomingBinSet = new BinSet(groupIncomingResult, inSameGroup, batchsize, concurrency, 
    						sourceIdProperty, targetIdProperty);
    				System.out.println("Loaded new BinSet with " + incomingBinSet.getCount() + " records");
    				System.out.println("Entropy: " + incomingBinSet.getEntropy() + " bits");
    				printFreeMemory();
    				return incomingBinSet;
    			});
    		}

    		// (2) If there are results in the last loaded BinSet, put them in the graph.
    		if (outgoingBinSet != null && outgoingBinSet.hasData()) {
    			//long postRoundTimeout = 0;
    			
    			// For each round:
    			int round = 1;
    			while (outgoingBinSet.hasNextRound()) {
    				System.out.println("Round " + round++);
    				printFreeMemory();
    				GroupIterator<Map<String,Object>>[] iterators = outgoingBinSet.getNextRound();

    				// Create IteratorThreads with the iterators for this round.
    				iteratorThreads = new ArrayList<IteratorThread>(iterators.length); 
    				for (int thread = 0; thread < iterators.length; thread++) {
    					iteratorThreads.add(new IteratorThread(iterators[thread]));
    				}
    				//int failuresThisRound = 0;

    				// While any IteratorThread can accept work:
    				while (iteratorThreads.stream().anyMatch(IteratorThread::canAcceptWork)) {
        				if (Util.transactionIsTerminated(terminationGuard)) break mainLoop;
    					if (log.isDebugEnabled()) log.debug("execute in batch no " + batches + " batch size " + batchsize);

    					// Find the first IteratorThread that can accept work
        				IteratorThread thisIteratorThread = iteratorThreads.stream()
    							.filter(IteratorThread::canAcceptWork).findFirst().orElse(null);

    					// From that iterator, find a batch of rows
        				List<Map<String,Object>> batch = takeWholeGroup(thisIteratorThread.getIterator(), batchsize);

    					// Turn that batch into a task
        				long currentBatchSize = batch.size();
    					Callable<Long> task;
    					if (iterateList) {
    						long finalBatches = batches;
    						task = () -> {
    							long c = count.addAndGet(currentBatchSize);
    							if (Util.transactionIsTerminated(terminationGuard)) return 0L;
    							try {
    								Map<String, Object> params = Util.map("_count", c, "_batch", batch);
    								retried.addAndGet(retry(consumer,params,0,retries));
    							} catch (Exception e) {
    								failedOps.addAndGet(batchsize);
    								if (failedParams >= 0) { 
    									failedParamsMap.put(Long.toString(finalBatches), new ArrayList<Map<String,Object>>(batch.subList(0, Math.min(failedParams+1, batch.size())))); 
    								}
    								recordError(operationErrors, e);
    							}
    							return currentBatchSize;
    						};
    					} else {
    						final long finalBatches = batches;
    						task = () -> {
    							if (Util.transactionIsTerminated(terminationGuard)) return 0L;
    							return batch.stream().map(
    									p -> {
    										long c = count.incrementAndGet();
    										if (c % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) return 0;
    										try {
    											Map<String, Object> params = merge(p, Util.map("_count", c, "_batch", batch));
    											retried.addAndGet(retry(consumer,params,0,retries));
    										} catch (Exception e) {
    											failedOps.incrementAndGet();
    											if (failedParams >= 0) { 
    												failedParamsMap.put(Long.toString(finalBatches), new ArrayList<Map<String,Object>>(batch.subList(0, Math.min(failedParams+1, batch.size())))); 
    											}
    											recordError(operationErrors, e);
    										}
    										return 1;
    									}).mapToLong(l -> l).sum();
    						};
    					}

    					// Add that task to the pool of things that need to be done
    					Future<Long> thisFuture = inTxFuture(pool, db, task); // Util.
    					// Assign it to the current IteratorThread
    					thisIteratorThread.assignWork(thisFuture, task, currentBatchSize);
    					batches++;

    					// While any IteratorThreads are not retired, and all IteratorThreads that are not retired
    					// are not free, wait until one finishes working and free it up.
    					while (iteratorThreads.stream().anyMatch(x -> !x.isRetired()) &&
    							iteratorThreads.stream().filter(x -> !x.isRetired()).allMatch(x -> !x.isFree())) {
    						while (iteratorThreads.stream().filter(x -> !x.isRetired())
    								.allMatch(IteratorThread::isWorking)) { // none done yet, block for a bit
    							LockSupport.parkNanos(1000);
    						}
    						// One of the IteratorThreads is done! Find it and accept its work
    						List<IteratorThread> iteratorThreadsFinished = iteratorThreads.stream()
    								.filter(x -> !x.isRetired() && !x.isWorking()).collect(Collectors.toList());
    						for (IteratorThread it : iteratorThreadsFinished) {
    							// If this task ran into an InterruptedException or an ExecutionException,
    							// retry up to a specified number of times. This is a workaround for the issue described here:
    							// https://github.com/neo4j/neo4j/issues/12040
    							try {
    								successes += it.getFuture().get();
    								it.acceptWork();
    							} catch (InterruptedException | ExecutionException e) {
									//failuresThisRound++;
    								// If there are still retries left, run the task again.
    								if (it.getRetries() < retries) {
    									log.info("Rerunning task, retry " + (it.getRetries()+1));
    									// Since the previous batch failed, but we're retrying, decrease the count
    									count.addAndGet(-it.getBatchSize());
    									Future<Long> newFuture = Util.inTxFuture(pool, db, it.getTask());
    									it.assignWork(newFuture, it.getTask(), it.getBatchSize());
    								}
    								// Otherwise, declare the batch a failure
    								else {
    									failedBatches.incrementAndGet();
    									batchErrors.compute(e.getMessage(),(s, i) -> i == null ? 1 : i + 1);
    									it.acceptWork();	// accept failure
    								}
    							}
    						}
    					}
    				}
    			} // end loop over rounds
    		} // end putting results from outgoing BinSet in the graph
    		
    		// At this point thing (2) is done. Now wait until thing (1) is done.
    		// The output from thing (1) becomes the input for thing (2) next time.
    		try {
				outgoingBinSet = incomingFuture.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				break;
			}
    		
    		// Print some things that are proxies for memory usage
    		System.out.println("------------ END OF CYCLE ----------------");
    		System.out.println("Operation errors: " + operationErrors.size());
    		System.out.println("Batch errors: " + batchErrors.size());
    		System.out.println("Failed params map size: " + failedParamsMap.size());
    		int totalFailedParamsElements = failedParamsMap.values().stream().mapToInt( // for each List in the Map of Lists
    				x -> x.stream().mapToInt(	// for each Map in the List of maps
    						y -> y.size()
    				).sum()
    		).sum();
    		System.out.println("Total failed params elements: " + totalFailedParamsElements);
    		
    	} // end two things to do (mainLoop)
    	
    	boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
    	if (wasTerminated) {
    		successes += iteratorThreads.stream().map(IteratorThread::getFuture)
    				.mapToLong(f -> Util.getFutureOrCancel(f, batchErrors, failedBatches, 0L)).sum();
    	}
    	Util.logErrors("Error during iterate.commit:", batchErrors, log);
    	Util.logErrors("Error during iterate.execute:", operationErrors, log);
    	long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
    	BatchAndTotalResult result = 
    			new BatchAndTotalResult(batches, count.get(), timeTaken, successes, failedOps.get(), failedBatches.get(), retried.get(), operationErrors, batchErrors, wasTerminated, failedParamsMap);        
    	return Stream.of(result);
    }
	
	
	
	public static <T> Future<T> inTxFuture(ExecutorService pool, GraphDatabaseService db, Callable<T> callable) {
        try {
            return pool.submit(() -> {
            	return transactionWithRetries(db, callable, 0, 5);
            });
        } catch (Exception e) {
            throw new RuntimeException("Error executing in separate transaction", e);
        }
    }
	
	
	public static <T> T transactionWithRetries(GraphDatabaseService db, Callable<T> callable, int thisRetry, int maxRetries) {
		try (Transaction tx = db.beginTx()) {
            T result = callable.call();
            tx.success();
            return result;
        }
        catch (DeadlockDetectedException e) {
        	if (thisRetry < maxRetries) {
        		System.out.println("Deadlock on try " + thisRetry + ", rerunning");
        		return transactionWithRetries(db, callable, thisRetry + 1, maxRetries);
        	}
        	else {
        		throw new RuntimeException("Error executing in separate transaction", e);
        	}
        } catch (Exception e) {
        	throw new RuntimeException("Error executing in separate transaction", e);
		}
	}
	
	
    public static <T> List<T> takeWholeGroup(GroupIterator<T> iterator, int batchsize) {
        List<T> result = new LinkedList<T>();
        while (iterator.hasNext() && (batchsize-- > 0 || iterator.nextIsInSameGroup())) {
            result.add(iterator.next());
        }
        return result;
    }
	
	
	
	private void printFreeMemory() {
		double freeMem = (double) Runtime.getRuntime().freeMemory() / 1073741824;
		DecimalFormat df = new DecimalFormat("0.000");
		System.out.println("Free memory: " + df.format(freeMem) + "G");
	}
    
    
    
	private void recordError(Map<String, Long> executionErrors, Exception e) {
        executionErrors.compute(getMessages(e),(s, i) -> i == null ? 1 : i + 1);
    }
    
    private String getMessages(Throwable e) {
        Set<String> errors = new LinkedHashSet<>();
        do {
            errors.add(e.getMessage());
            e = e.getCause();
        } while (e.getCause() != null && !e.getCause().equals(e));
        return String.join("\n",errors);
    }
    
    
    // In each round, each thread is working from one iterator.
    // This object keeps the iterator and the thread together.
    private class IteratorThread {
    	private GroupIterator<Map<String, Object>> iterator;
    	private Future<Long> future;
    	private Callable<Long> task;
    	// Free = available to do more work if available.
    	private boolean isFree = true;
    	private int retries;
    	private long batchSize;
    	
    	public IteratorThread(GroupIterator<Map<String, Object>> iterator) {
    		this.iterator = iterator;
    	}
    	
    	public void assignWork(Future<Long> future, Callable<Long> task, long batchSize) {
    		this.future = future;
    		this.batchSize = batchSize;
    		isFree = false;
    		
    		// If the task is the same as before, it's a retry; otherwise reset retries.
    		if (this.task == task) {
    			retries++;
    		} else {
    			this.task = task;
    			retries = 0;
    		}
    	}
    	
    	public void acceptWork() {
    		isFree = true;
    	}
    	
    	public boolean hasMoreWork() { return iterator.hasNext(); }
    	public boolean isWorking() { return (future != null && !future.isDone()); }
    	public boolean isFree() { return isFree; }
    	public boolean canAcceptWork() { return (isFree() & hasMoreWork()); }
    	public boolean isRetired() { return (isFree() & !hasMoreWork()); }
    	
    	public GroupIterator<Map<String, Object>> getIterator() { return iterator; }
    	public Future<Long> getFuture() { return future; }
    	public Callable<Long> getTask() { return task; }
    	public int getRetries() { return retries; }
    	public long getBatchSize() { return batchSize; }
    }
}

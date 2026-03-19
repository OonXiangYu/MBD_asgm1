import java.util.*;

public class CC {

	// Lock
	static class Lock {
		String type; // "S" (Shared) or "X" (Exclusive)
		Set<Integer> holders = new HashSet<>(); // the set of tid currently holding this lock
	}

	// Log
	static class LogEntry {
		int timestamp; // when its happen
		String type; // R, W, C, A
		int tid; // transaction id
		int recordId; 
		int oldValue; // value before write
		int newValue; // value after write
		int prev; // index of previous log entry for the same transaction

		LogEntry(int ts, String type, int tid, int recordId, int oldValue, int newValue, int prev) {
			this.timestamp = ts;
			this.type = type;
			this.tid = tid;
			this.recordId = recordId;
			this.oldValue = oldValue;
			this.newValue = newValue;
			this.prev = prev;
		}
	}
	
	// Helpers
	// W(3,5) -> 3, R(3) -> 3
	static int getRecord(String op) {
		int s = op.indexOf("(") + 1; // start after (
		int e = op.contains(",") ? op.indexOf(",") : op.indexOf(")"); // end at , if there is one else end at )
		return Integer.parseInt(op.substring(s, e));
	}

	// W(3,5) -> 5
	static int getValue(String op) {
		int s = op.indexOf(",") + 1; // start after ,
		int e = op.indexOf(")"); // end at )
		return Integer.parseInt(op.substring(s, e));
	}

	static List<LogEntry> log = new ArrayList<>();
	static int globalTime = 0;
	static Map<Integer, Integer> lastLog = new HashMap<>(); // maps each transaction index

	// Reset
	static void reset() {
		log.clear();
		globalTime = 0;
		lastLog.clear();
	}

	// Main
	public static int[] executeSchedule(int[] db, List<String> transactions) {

		reset();

		int n = transactions.size(); // number of transaction

		List<List<String>> ops = new ArrayList<>();
		for (String t : transactions) {
			ops.add(Arrays.asList(t.split(";"))); // get all operation per transaction by splitting with ;
		}

		int[] index = new int[n]; // track which operation each transaction currently on
		boolean[] aborted = new boolean[n]; // whether each transaction has been aborted

		Map<Integer, Lock> lockTable = new HashMap<>(); // maps record ID to its current lock

		List<Integer> schedule = new ArrayList<>(); 

		while (true) {

			boolean progress = false;

			for (int t = 0; t < n; t++) {

				if (aborted[t]) // skip if aborted
					continue;
				if (index[t] >= ops.get(t).size()) // skip if finish
					continue;

				String op = ops.get(t).get(index[t]); // get the current operation for t

				if (canExecute(op, t, lockTable)) { // check if can run now, prevent conflict

					execute(op, t, db, lockTable, schedule);

					index[t]++; // move to next
					progress = true; // continue loop
				}
			}

			// Deadlock detection
			Map<Integer, Set<Integer>> graph = buildWaitGraph(ops, index, lockTable, aborted, n); // builds a graph

			List<Integer> cycle = detectCycle(graph); // check if there is a cycle in the graph

			if (!cycle.isEmpty()) { // if deadlock

				int victim = Collections.max(cycle); // pick a victim, which is the transaction with the highest index in the dl

				abortTransaction(victim, db, lockTable, aborted); // rollback, release lock, abort
				progress = true;
			}

			if (!progress) // nothing ran and no dl resolved then stop
				break;
		}

		// Print Schedule
		System.out.println("Schedule:");
		System.out.println(schedule);

		// Print Log
		System.out.println("\nLog:");
		for (LogEntry e : log) {

			if (e.type.equals("R")) {
				System.out.println("R:" + e.timestamp + ",T" + e.tid + "," + e.recordId + "," + e.newValue + "," + e.prev);
			}

			else if (e.type.equals("W")) {
				System.out.println("W:" + e.timestamp + ",T" + e.tid + "," + e.recordId + "," + e.oldValue + "," + e.newValue + "," + e.prev);
			}

			else if (e.type.equals("C")) {
				System.out.println("C:" + e.timestamp + ",T" + e.tid + "," + e.prev);
			}

			else if (e.type.equals("A")) {
				System.out.println("A:" + e.timestamp + ",T" + e.tid + "," + e.prev);
			}
		}

		return db;
	}

	// Execute
	static void execute(String op, int t, int[] db, Map<Integer, Lock> lockTable, List<Integer> schedule) {

		int tid = t + 1;

		schedule.add(t);

		if (op.startsWith("R")) { // if read

			int record = getRecord(op); // get the recordId

			acquireShared(record, t, lockTable); // need a share lock

			int value = db[record]; // read the value

			addLog("R", tid, record, -1, value, t); // log
		}

		else if (op.startsWith("W")) { // if write

			int record = getRecord(op); // get the recordId
			int newVal = getValue(op); // get the value

			acquireExclusive(record, t, lockTable); // need exclusive lock

			int oldVal = db[record]; // save the old value for rollback
			db[record] = newVal; // write the new value into db

			addLog("W", tid, record, oldVal, newVal, t); // log
		}

		else if (op.equals("C")) { // if commit

			addLog("C", tid, -1, -1, -1, t); // log
			releaseLocks(t, lockTable); // release all lock this transaction holds
		}
	}

	// Adding Log
	static void addLog(String type, int tid, int record, int oldVal, int newVal, int tIndex) {

		int prev = lastLog.getOrDefault(tIndex, -1); // looks up the index of this transaction previous log entry

		LogEntry e = new LogEntry(globalTime++, type, tid, record, oldVal, newVal, prev); // create log with current timestamp

		log.add(e); // append
		lastLog.put(tIndex, log.size() - 1); // update pointer
	}

	// Locking
	static void acquireShared(int record, int t, Map<Integer, Lock> lockTable) {

		lockTable.putIfAbsent(record, new Lock()); // create new lock for this record if it don't have
		Lock lock = lockTable.get(record);

		if (lock.type == null || lock.holders.isEmpty()) { // lock is free
			lock.type = "S"; // set it to shared lock
			lock.holders.add(t); // index
		} else if (lock.type.equals("S")) { // if another hold a shared lock
			lock.holders.add(t); // just add index
		}
	}

	static void acquireExclusive(int record, int t, Map<Integer, Lock> lockTable) {

		lockTable.putIfAbsent(record, new Lock()); // create new lock for this record if it don't have
		Lock lock = lockTable.get(record);

		if (lock.type == null || lock.holders.isEmpty()) { // lock is free
			lock.type = "X"; // set it to exclusive
			lock.holders.clear();
			lock.holders.add(t); // add index
		} else if (lock.type.equals("S") && lock.holders.contains(t) && lock.holders.size() == 1) { // if lock is shared but hold by one transaction
			lock.type = "X"; // upgrade to exclusive
		}
	}

	static void releaseLocks(int t, Map<Integer, Lock> lockTable) {

		List<Integer> remove = new ArrayList<>();

		for (Map.Entry<Integer, Lock> e : lockTable.entrySet()) { // loop through all locks

			Lock lock = e.getValue();
			lock.holders.remove(t);

			if (lock.holders.isEmpty()) // no ones hold the lock
				remove.add(e.getKey()); // mark the record for removal
		}

		for (int r : remove)
			lockTable.remove(r); // dlt all fully released lock
	}

	// Allow Execute
	static boolean canExecute(String op, int t, Map<Integer, Lock> lockTable) {

		if (op.startsWith("R")) { // if read
			int record = getRecord(op); // get record id
			if (!lockTable.containsKey(record)) // no lock on record
				return true; // read

			Lock lock = lockTable.get(record);
			if (lock.type == null || lock.holders.isEmpty()) // lock is null or no holder
				return true; // read

			if (lock.type.equals("S")) // shared lock
				return true; // read

			return lock.type.equals("X") && lock.holders.contains(t); // read only it holding the exclusive lock
		}

		if (op.startsWith("W")) {
			int record = getRecord(op);
			if (!lockTable.containsKey(record)) // no lock
				return true; // write

			Lock lock = lockTable.get(record);
			if (lock.type == null || lock.holders.isEmpty()) // lock is null or no holder
				return true; // write

			if (lock.type.equals("X") && lock.holders.contains(t) && lock.holders.size() == 1) // if holding exclusive lock
				return true; // write
			if (lock.type.equals("S") && lock.holders.contains(t) && lock.holders.size() == 1) // if holding shared lock
				return true; // write

			return false;
		}

		return true;
	}

	// Wait Graph
	static Map<Integer, Set<Integer>> buildWaitGraph(List<List<String>> ops, int[] index, Map<Integer, Lock> lockTable,
			boolean[] aborted, int n) {

		Map<Integer, Set<Integer>> graph = new HashMap<>();

		for (int t = 0; t < n; t++) {

			// skip those finish
			if (aborted[t])
				continue;
			if (index[t] >= ops.get(t).size())
				continue;

			String op = ops.get(t).get(index[t]);

			if (!canExecute(op, t, lockTable)) { // if current operation is block

				int record = getRecord(op); // get the id

				if (!lockTable.containsKey(record))
					continue;

				Lock lock = lockTable.get(record);

				graph.putIfAbsent(t, new HashSet<>()); // add transaction as a node in graph if not there

				for (int holder : lock.holders) { // loop through every transaction block t
					if (holder != t) // skip itself
						graph.get(t).add(holder); // draw edge, means t is waiting holder to release the lock
				}
			}
		}

		return graph;
	}

	// Deadlock
	static List<Integer> detectCycle(Map<Integer, Set<Integer>> graph) {

		Set<Integer> visited = new HashSet<>(); // fully explored
		Set<Integer> stack = new HashSet<>(); // track nodes in the current DFS

		for (int node : graph.keySet()) {
			List<Integer> cycle = dfs(node, graph, visited, stack, new ArrayList<>());
			if (!cycle.isEmpty()) // a cycle found
				return cycle; // return
		}

		return new ArrayList<>(); // no deadlock
	}

	static List<Integer> dfs(int node, Map<Integer, Set<Integer>> graph, Set<Integer> visited, Set<Integer> stack,
			List<Integer> path) {

		if (stack.contains(node)) { // cycle found
			path.add(node);
			return path;
		}

		if (visited.contains(node)) // skip those who fully explored
			return new ArrayList<>();

		// mark those as being explored
		visited.add(node); 
		stack.add(node);
		path.add(node);

		for (int nei : graph.getOrDefault(node, new HashSet<>())) { // visit each neighbor one by one
			List<Integer> res = dfs(nei, graph, visited, stack, new ArrayList<>(path));
			if (!res.isEmpty()) // cycle found
				return res;
		}

		stack.remove(node);
		return new ArrayList<>();
	}

	// Abort
	static void abortTransaction(int t, int[] db, Map<Integer, Lock> lockTable, boolean[] aborted) {

		int tid = t + 1;

		for (int i = log.size() - 1; i >= 0; i--) { // rollback loop
			LogEntry e = log.get(i);
			if (e.tid == tid && e.type.equals("W")) { // only undo write belongings this t
				db[e.recordId] = e.oldValue;
			}
		}

		releaseLocks(t, lockTable); // free lock
		aborted[t] = true;

		addLog("A", tid, -1, -1, -1, t); // add log
	}
}
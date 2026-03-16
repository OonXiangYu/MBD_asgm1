import java.util.*;

public class CC {

	/**
	 * Notes: - Execute all given transactions, using locking. - Each element in the
	 * transactions List represents all operations performed by one transaction, in
	 * order. - No operation in a transaction can be executed out of order, but
	 * operations may be interleaved with other transactions if allowed by the
	 * locking. - The index of the transaction in the list is equivalent to the
	 * transaction ID. - Print the log to the console at the end of the method. -
	 * Return the new db state after executing the transactions.
	 * 
	 * @param db           the initial status of the db
	 * @param transactions the schedule, which basically is a {@link List} of
	 *                     transactions.
	 * @return the final status of the db
	 */
	static class Lock {
		String type; // S or X
		Set<Integer> holders = new HashSet<>();
	}

	static Map<Integer, Lock> lockTable = new HashMap<>();

	public static int[] executeSchedule(int[] db, List<String> transactions)
	{

		int n = transactions.size();

		List<String[]> operations = new ArrayList<>();

		for (String s : transactions)
			operations.add(s.split(";"));

		int[] pointer = new int[n];

		int[] lastLog = new int[n];
		Arrays.fill(lastLog, -1);

		List<String> log = new ArrayList<>();

		int timestamp = 0;

		boolean finished = false;

		while (!finished)
		{
			finished = true;

			for (int t = 0; t < n; t++)
			{
				String[] ops = operations.get(t);

				if (pointer[t] >= ops.length)
					continue;

				finished = false;

				String op = ops[pointer[t]].trim();

				int tid = t + 1;

				if (op.startsWith("R"))
				{
					int record = Integer.parseInt(op.substring(2, op.length() - 1));

					int value = db[record];

					System.out.println("T" + tid + ":R(" + record + ") -> " + value);

					String entry = "R:" + timestamp + ",T" + tid + "," + record + "," + value + "," + lastLog[t];

					log.add(entry);

					lastLog[t] = timestamp;

					timestamp++;
				}

				else if (op.startsWith("W"))
				{
					String inside = op.substring(2, op.length() - 1);
					String[] parts = inside.split(",");

					int record = Integer.parseInt(parts[0]);
					int newValue = Integer.parseInt(parts[1]);

					int oldValue = db[record];

					db[record] = newValue;

					System.out.println("T" + tid + ":W(" + record + "," + newValue + ")");

					String entry = "W:" + timestamp + ",T" + tid + "," + record + "," + oldValue + "," + newValue + "," + lastLog[t];

					log.add(entry);

					lastLog[t] = timestamp;

					timestamp++;
				}

				else if (op.equals("C"))
				{
					System.out.println("T" + tid + ":C");

					String entry = "C:" + timestamp + ",T" + tid + "," + lastLog[t];

					log.add(entry);

					lastLog[t] = timestamp;

					timestamp++;
				}

				pointer[t]++;
			}
		}

		System.out.println("\nSYSTEM LOG:");

		for (String entry : log)
			System.out.println(entry);

		return db;
	}

	private static boolean canAcquireShared(int record, int tid) {

		Lock lock = lockTable.get(record);

		if (lock == null)
			return true;

		if (lock.type.equals("X") && !lock.holders.contains(tid))
			return false;

		return true;
	}

	private static void acquireShared(int record, int tid) {

		Lock lock = lockTable.get(record);

		if (lock == null) {
			lock = new Lock();
			lock.type = "S";
			lockTable.put(record, lock);
		}

		lock.holders.add(tid);
	}

	private static boolean canAcquireExclusive(int record, int tid) {

		Lock lock = lockTable.get(record);

		if (lock == null)
			return true;

		if (lock.type.equals("X") && lock.holders.contains(tid))
			return true;

		if (lock.type.equals("S") && lock.holders.size() == 1 && lock.holders.contains(tid))
			return true;

		return false;
	}

	private static void acquireExclusive(int record, int tid) {

		Lock lock = lockTable.get(record);

		if (lock == null) {
			lock = new Lock();
			lockTable.put(record, lock);
		}

		lock.type = "X";
		lock.holders.clear();
		lock.holders.add(tid);
	}

	private static void releaseLocks(int tid) {

		for (Lock lock : lockTable.values()) {
			lock.holders.remove(tid);
		}
	}
}
import java.util.*;

public class MBD_asgm1 {
	
	class Lock {
        String type; // S or X
        Set<Integer> holders = new HashSet<>();
    }

    Map<Integer, Lock> lockTable = new HashMap<>();
	
    public int[] executeSchedule(List<String> schedules) {

        int[] database = new int[10];

        for (int i = 0; i < 10; i++) {
            database[i] = i;
        }

        int n = schedules.size();

        List<String[]> operations = new ArrayList<>();

        for (String s : schedules) {
            operations.add(s.split(";"));
        }

        int[] pointer = new int[n];
        boolean finished = false;

        while (!finished) {

            finished = true;

            for (int t = 0; t < n; t++) {

                String[] ops = operations.get(t);

                if (pointer[t] >= ops.length) continue;

                finished = false;

                String op = ops[pointer[t]].trim();
                int tid = t + 1;

                if (op.startsWith("R")) {

                    int record = Integer.parseInt(op.substring(2, op.length() - 1));

                    if (canAcquireShared(record, tid)) {

                        acquireShared(record, tid);

                        int value = database[record];

                        System.out.println("T" + tid + ":R(" + record + ") -> " + value);

                        pointer[t]++;
                    }

                }

                else if (op.startsWith("W")) {

                    String inside = op.substring(2, op.length() - 1);
                    String[] parts = inside.split(",");

                    int record = Integer.parseInt(parts[0]);
                    int value = Integer.parseInt(parts[1]);

                    if (canAcquireExclusive(record, tid)) {

                        acquireExclusive(record, tid);

                        database[record] = value;

                        System.out.println("T" + tid + ":W(" + record + "," + value + ")");

                        pointer[t]++;
                    }

                }

                else if (op.equals("C")) {

                    releaseLocks(tid);

                    System.out.println("T" + tid + ":C");

                    pointer[t]++;
                }

            }
        }

        return database;
    }
	
	private boolean canAcquireShared(int record, int tid) {

        Lock lock = lockTable.get(record);

        if (lock == null) return true;

        if (lock.type.equals("X") && !lock.holders.contains(tid)) return false;

        return true;
    }

    private void acquireShared(int record, int tid) {

        Lock lock = lockTable.get(record);

        if (lock == null) {
            lock = new Lock();
            lock.type = "S";
            lockTable.put(record, lock);
        }

        lock.holders.add(tid);
    }

    private boolean canAcquireExclusive(int record, int tid) {

        Lock lock = lockTable.get(record);

        if (lock == null) return true;

        if (lock.type.equals("X") && lock.holders.contains(tid)) return true;

        if (lock.type.equals("S") && lock.holders.size() == 1 && lock.holders.contains(tid))
            return true;

        return false;
    }

    private void acquireExclusive(int record, int tid) {

        Lock lock = lockTable.get(record);

        if (lock == null) {
            lock = new Lock();
            lockTable.put(record, lock);
        }

        lock.type = "X";
        lock.holders.clear();
        lock.holders.add(tid);
    }

    private void releaseLocks(int tid) {

        for (Lock lock : lockTable.values()) {
            lock.holders.remove(tid);
        }
    }

	public static void main(String[] args) {
		MBD_asgm1 db = new MBD_asgm1();

        List<String> schedules = new ArrayList<>();

        schedules.add("W(1,5);C");
        schedules.add("R(9);R(7);C");
        schedules.add("R(1);C");

        int[] result = db.executeSchedule(schedules);

        System.out.println("Final Database:");

        System.out.println(Arrays.toString(result));

	}

}

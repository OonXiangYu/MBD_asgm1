import java.util.*;

public class CC {

    static class Lock {
        String type; // "S" or "X"
        Set<Integer> holders = new HashSet<>();
    }

    // must be static and shared
    static Map<Integer, Lock> lockTable = new HashMap<>();

    public static int[] executeSchedule(int[] db, List<String> transactions) {

        int n = transactions.size();
        List<String[]> operations = new ArrayList<>();
        for (String s : transactions) operations.add(s.split(";"));

        int[] pointer = new int[n];
        int[] lastLog = new int[n];
        Arrays.fill(lastLog, -1);

        List<String> log = new ArrayList<>();
        int timestamp = 0;

        boolean finished = false;

        while (!finished) {
            finished = true;

            for (int t = 0; t < n; t++) {
                String[] ops = operations.get(t);
                if (pointer[t] >= ops.length) continue;

                finished = false;

                String op = ops[pointer[t]].trim();
                int tid = t + 1;

                // READ
                if (op.startsWith("R")) {
                    int record = Integer.parseInt(op.substring(2, op.length() - 1));
                    int value = db[record];
                    System.out.println("T" + tid + ":R(" + record + ") -> " + value);
                    log.add("R:" + timestamp + ",T" + tid + "," + record + "," + value + "," + lastLog[t]);
                    lastLog[t] = timestamp++;
                }

                // WRITE
                else if (op.startsWith("W")) {
                    String inside = op.substring(2, op.length() - 1);
                    String[] parts = inside.split(",");
                    int record = Integer.parseInt(parts[0]);
                    int newValue = Integer.parseInt(parts[1]);
                    int oldValue = db[record];
                    db[record] = newValue;
                    System.out.println("T" + tid + ":W(" + record + "," + newValue + ")");
                    log.add("W:" + timestamp + ",T" + tid + "," + record + "," + oldValue + "," + newValue + "," + lastLog[t]);
                    lastLog[t] = timestamp++;
                }

                // COMMIT
                else if (op.equals("C")) {
                    System.out.println("T" + tid + ":C");
                    log.add("C:" + timestamp + ",T" + tid + "," + lastLog[t]);
                    lastLog[t] = timestamp++;
                }

                pointer[t]++;
            }
        }

        System.out.println("\nSYSTEM LOG:");
        for (String entry : log) System.out.println(entry);

        return db;
    }
}
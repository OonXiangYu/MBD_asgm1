import java.util.*;

public class MBD_asgm1 {
	
	public int[] executeSchedule(List<String> schedules) {

        // create database
        int[] database = new int[10];

        for (int i = 0; i < 10; i++) {
            database[i] = i;
        }

        int n = schedules.size();

        // split operations for each transaction
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

                    int value = database[record];

                    System.out.println("T" + tid + ":R(" + record + ") -> " + value);

                }

                else if (op.startsWith("W")) {

                    String inside = op.substring(2, op.length() - 1);
                    String[] parts = inside.split(",");

                    int record = Integer.parseInt(parts[0]);
                    int value = Integer.parseInt(parts[1]);

                    database[record] = value;

                    System.out.println("T" + tid + ":W(" + record + "," + value + ")");

                }

                else if (op.equals("C")) {

                    System.out.println("T" + tid + ":C");

                }

                pointer[t]++;

            }
        }

        return database;
    }

	public static void main(String[] args) {
		

	}

}

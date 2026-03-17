import java.util.*;

public class MBD_asgm1 {
    public static void main(String[] args) {
        int[] db = new int[10];
        for (int i = 0; i < 10; i++) db[i] = i;

        List<String> transactions = new ArrayList<>();
        transactions.add("W(1,5);C");
        transactions.add("R(9);R(7);C");
        transactions.add("R(1);C");

        int[] result = CC.executeSchedule(db, transactions);

        System.out.println("\nFinal Database:");
        System.out.println(Arrays.toString(result));
    }
}
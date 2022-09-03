package top.byteinfo;

import java.util.Random;

public class CB01 {
    private static final String desp = """
            Given an integer n, return a string array answer (1-indexed) where:
            0    answer[i] == "FizzBuzz" if i is divisible by 3 and 5.
            1    answer[i] == "Fizz" if i is divisible by 3.
            2    answer[i] == "Buzz" if i is divisible by 5.
            3    answer[i] == i (as a string) if none of the above conditions are true.
                        
            Example 1:
            Input: n = 3
            Output: ["1","2","Fizz"]
                        
            Example 2:
            Input: n = 5
            Output: ["1","2","Fizz","4","Buzz"]
                        
            Example 3:
            Input: n = 15
            Output: ["1","2","Fizz","4","Buzz","Fizz","7","8","Fizz","Buzz","11","Fizz","13","14","FizzBuzz"]
                        
            Constraints:
            1 <= n <= 104
            """;

    public static void main(String[] args) {

        String parse = """
                                
                """;
        int top = (int) 1e2;

        Random random = new Random();
        int n = random.nextInt(15, 35);
        String[] ss = new String[n];
        while (n-- > 0) {
            ss[n] = generate(n);
            System.out.println(ss[n]);
        }


    }

    private static String generate(int n) {
        switch (compute(n)) {
            case 0:
                return "0";
            case 1:
                return "1";
            case 2:
                return "2";
            case 3:
                return "3";
            default:
                throw new RuntimeException("error");
        }
    }

    private static int compute(int n) {
        n++;
        boolean[] bs = new boolean[4];
        bs[0] = n % 15 == 0;
        bs[1] = (n % 3 == 0) && (n % 5 != 0);
        bs[2] = (n % 5 == 0) && (n % 3 != 0);
        bs[3] = (n % 5 != 0) && (n % 3 != 0);
        for (int i = 0; i < 4; i++) {

            if (bs[i]) {
                return i;
            }
        }
        return -1;
    }
}

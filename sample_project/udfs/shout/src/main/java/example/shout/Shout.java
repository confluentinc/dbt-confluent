package example.shout;

import java.util.Locale;
import org.apache.flink.table.functions.ScalarFunction;

/** Scalar UDF: upper-cases a string and appends an exclamation point ("hi" -> "HI!"). */
public class Shout extends ScalarFunction {
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return input.toUpperCase(Locale.ROOT) + "!";
    }
}

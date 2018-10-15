package log;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    private static final SimpleDateFormat DATE_Util = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static Date parse(String date) {
        try {
            return DATE_Util.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String format(Date date) {
        return DATE_Util.format(date);
    }
}

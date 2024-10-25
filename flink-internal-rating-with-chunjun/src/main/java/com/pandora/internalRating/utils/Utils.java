package com.pandora.internalRating.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Utils {
    public static LocalDate convertToLocalDate(String timeStr, String pattern) {
        if (timeStr == null) {
            return LocalDate.MIN;
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);

        return LocalDate.parse(timeStr, dateTimeFormatter);
    }

    public static String convertToString(LocalDate timeDate, String pattern) {
        if (timeDate == null) {
            return "19700101";
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);

        return timeDate.format(dateTimeFormatter);
    }

    public static int convertExpectrateToNum(String expectrate) {
        if (expectrate == null) {
            return 100;
        }
        int expectrateNum;
        switch (expectrate) {
            case "负面":
                expectrateNum = -10;
                break;
            case "稳定":
        }
    }
}

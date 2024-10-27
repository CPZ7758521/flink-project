package com.pandora.internalRating.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
                expectrateNum = 0;
                break;
            case "正面":
                expectrateNum = 10;
                break;
            default:
                expectrateNum = 100;
        }
        return expectrateNum;
    }

    public static int convertWatchrateToNum(String watchrate) {
        if (watchrate == null) {
            return 100;
        }

        int watchrateNum;
        switch (watchrate) {
            case "负面":
                watchrateNum = -10;
                break;
            case "--":
            case "中性":
                watchrateNum = 0;
                break;
            case "正面":
                watchrateNum = 10;
                break;
            default:
                watchrateNum = 100;
        }
        return watchrateNum;
    }

    public static String convertBondCode(String mktcode01, String code) {
        if (mktcode01 == null) {
            return code;
        }
        String bond_code;
        switch (mktcode01) {
            case "1":
                bond_code = code + ".SH";
                break;
            case "2":
                bond_code = code + ".SZ";
                break;
            case "4":
                bond_code = code + ".IB";
                break;
            case "6":
                bond_code = code + ".BJ";
                break;
            default:
                bond_code = null;
        }

        return bond_code;
    }

    public static int compare_last_code_id(Integer code_id, Integer last_code_id) {
        if (code_id == null || last_code_id == null) {
            return 0;
        }
        int conde_id_diff = code_id - last_code_id;

        if (conde_id_diff > 0) {
            return -1;
        } else if (conde_id_diff < 0) {
            return 1;
        } else {
            return 0;
        }
    }

    public static int compare_last_expectrateNum_watchrateNum (Integer current_num, Integer last_num) {
        if (current_num == null || last_num == null) {
            return 0;
        }
        int num_diff = current_num - last_num;
        if ((num_diff <= -10 && num_diff > -90) || num_diff == -110) {
            return -1;
        } else if ((num_diff >= 10 && num_diff < 90) || num_diff == -90) {
            return 1;
        } else {
            return 0;
        }
    }

    public static ArrayList<String> iteratorToArrayList(Iterator<String> iterator, String submittime) {
        ArrayList<String> arrayList = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false).collect(Collectors.toCollection(new Supplier<ArrayList<String>>() {
            @Override
            public ArrayList<String> get() {
                return new ArrayList<String>();
            }
        }));

        HashSet<String> submittimeSet = new HashSet<>(arrayList);
        if (!submittimeSet.contains(submittime) && (!"".equals(submittime))) {
            arrayList.add(submittime);
        }

        Collections.sort(arrayList);
        Collections.reverse(arrayList);
        return arrayList;

    }


    public static int compare_last_isindraftdown(Integer isindraftdown, Integer last_isindraftdown) {
        if (isindraftdown == null) {
            isindraftdown = 0;
        }
        if (last_isindraftdown == null) {
            last_isindraftdown = 0;
        }

        int isindraftdown_diff = isindraftdown - last_isindraftdown;
        if (isindraftdown_diff == 1) {
            return 1;
        } else {
            return 0;
        }
    }

}

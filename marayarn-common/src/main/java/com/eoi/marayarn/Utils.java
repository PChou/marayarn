package com.eoi.marayarn;

import java.util.List;

public class Utils {
    public static boolean StringEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static boolean ListEmpty(List list) {
        return list == null || list.isEmpty();
    }

    public static boolean StringEquals(String a, String b) {
        if (a == null && b == null)
            return true;
        if (a != null && a.equals(b))
            return true;
        return false;
    }

    public static String urlJoin(String first, String... more) {
        for(String item : more) {
            if(first.endsWith("/") && item.startsWith("/")) {
                first = String.format("%s%s", first.substring(0, first.length()-1), item);
            } else if(first.endsWith("/") || item.startsWith("/")) {
                first = String.format("%s%s", first, item);
            } else {
                first = String.format("%s/%s", first, item);
            }
        }
        return first;
    }
}

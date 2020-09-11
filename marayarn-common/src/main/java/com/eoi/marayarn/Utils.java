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
}

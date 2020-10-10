package com.eoi.marayarn;

public class StdIOUtil {
    private StdIOUtil() {
        //forbid init instance
    }

    public static void println(String x) {
        System.out.println(x);
    }

    public static void println() {
        System.out.println();
    }

    public static void printf(String format, Object ... args) {
        System.out.printf(format, args);
    }

    public static void printlnF(String format, Object ... args) {
        printf(format, args);
        println();
    }

    public static void printlnP(String x) {
        println();
        println(x);
    }

    public static void printlnS(String x) {
        println(x);
        println();
    }

    public static void printlnFP(String format, Object ... args) {
        println();
        printlnF(format, args);
    }

    public static void printlnFS(String format, Object ... args) {
        printlnF(format, args);
        println();
    }
}

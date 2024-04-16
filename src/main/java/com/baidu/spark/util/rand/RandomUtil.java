package com.baidu.spark.util.rand;

import java.util.Random;

public class RandomUtil {

    private static final Random random = new Random();
    private static final String ALL_CARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    public static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(ALL_CARACTERS.charAt(random.nextInt(ALL_CARACTERS.length())));
        }
        return sb.toString();
    }

    public static byte[] randomByteArray(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i]= (byte)(ALL_CARACTERS.charAt(random.nextInt(ALL_CARACTERS.length())));
        }
        return bytes;
    }

    public static void randomByteArray(byte[] bytes,int randomNum) {
        for (int i = 0; i < randomNum; i++) {
            bytes[random.nextInt(randomNum)]= (byte)(ALL_CARACTERS.charAt(random.nextInt(ALL_CARACTERS.length())));
        }
    }

    public static String randomDecimal(int precision, int scale) {
        // if scale is 38 and precision is 0, only generate 5 digits.
        int maxScale = scale - precision;
        if (maxScale > 5) {
            maxScale = 5;
        }
        int limit = (int)Math.pow(10, maxScale);
        return random.nextInt(limit) + "";
    }

    public static String randomDouble() {
        return random.nextDouble() + "";
    }

    public static void main(String[] args) {
        byte[] bytes = randomByteArray(1024);
        for (int i = 0; i < 100000000; i++) {
            randomByteArray(bytes, 10);
        }

    }
}

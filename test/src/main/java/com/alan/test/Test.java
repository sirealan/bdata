package com.alan.test;


import com.alan.fp.RecUtil;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
//        int[] aa = new int[]{1, 2, 3, 4, 5};
        int[] arr = new int[] {9,4,6,8,3,10,4,6};
        /**
         * 9,4,6,8,3,10,4,6
         * 9,4,6,8,3,6,4,10 a[i] = 5
         * 4,4,6,8,3,6,9,10 a[i] a[j] =4
         * low=0,high=5 4,4,6,8,3,6
         * 4,4,3,8,6,6,9,10
         * 4,4,6,8,3,6,9,10
         * 3,4,6,8,4,6,9,10
         *
         */
//        RecUtil.quickSort(arr,0,arr.length - 1);
        RecUtil.quickSort2(arr,0,arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }


}

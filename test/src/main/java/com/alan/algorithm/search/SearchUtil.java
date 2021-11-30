package com.alan.algorithm.search;

public class SearchUtil {

    /**
     * 二分查找
     *
     * @param arr
     * @param key
     */
    public static Integer binarySearch(int[] arr, int key) {
        int h = arr.length - 1;
        int l = 0;
        while (l <= h) {
            int mid = (l + h) >>> 1;
            int midKey = arr[mid];
            if (key < midKey) {
                h = mid - 1;
            } else if (key > midKey) {
                l = mid + 1;
            } else {
                return mid;
            }
        }
        return -1;
    }
}

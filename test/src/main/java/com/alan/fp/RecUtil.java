package com.alan.fp;

import fj.data.Array;

import java.math.BigInteger;

public class RecUtil {
    public static void quickSort2(int[] arr, int low, int high) {
        int p, i, j, tmp;
        if (low >= high) return;
        p = arr[low];
        i = low;
        j = high;
        while (i < j) {
            while (i < j && arr[j] >= p) j--;
            while (i < j && arr[i] <= p) i++;
            tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
        }
        arr[low]=arr[i];
        arr[i]=p;
        quickSort2(arr,low,j-1);
        quickSort2(arr,j+1,high);
    }

    public static void quickSort(int[] arr, int low, int high) {
        int p, i, j, temp;
        if (low >= high) {
            return;
        }//p就是基准数,这里就是每个数组的第一个
        p = arr[low];
        i = low;
        j = high;
        while (i < j) {//右边当发现小于p的值时停止循环
            while (arr[j] >= p && i < j) {
                j--;
            }
            //这里一定是右边开始，上下这两个循环不能调换（下面有解析，可以先想想） 左边当发现大于p的值时停止循环
            while (arr[i] <= p && i < j) {
                i++;
            }
            temp = arr[j];
            arr[j] = arr[i];
            arr[i] = temp;
        }
        arr[low] = arr[i];//这里的arr[i]一定是停小于p的，经过i、j交换后i处的值一定是小于p的(j先走)
        arr[i] = p;
        quickSort(arr, low, j - 1);  //对左边快排
        quickSort(arr, j + 1, high); //对右边快排

    }

    public static BigInteger jc(BigInteger a) {
        class A {
            public BigInteger tcp_(BigInteger a, BigInteger b, BigInteger c) {
                if (c.equals(BigInteger.ZERO)) {
                    return BigInteger.ZERO;
                } else if (c.equals(BigInteger.ONE)) {
                    return a.multiply(b);
                } else {
                    return tcp_(a.multiply(b), b.add(BigInteger.ONE), c.subtract(BigInteger.ONE));
                }

            }
        }
        return new A().tcp_(BigInteger.ONE, BigInteger.ONE, a);
    }

    public static BigInteger jc2(BigInteger a) {
        class A {
            public BigInteger tcp_(BigInteger a, BigInteger b) {
                if (b.equals(BigInteger.ZERO)) {
                    return BigInteger.ZERO;
                } else if (b.equals(BigInteger.ONE)) {
                    return a.multiply(b);
                } else {
                    return tcp_(a.multiply(b), b.subtract(BigInteger.ONE));
                }
            }
        }
        return new A().tcp_(BigInteger.ONE, a);
    }

    public static Integer jc21(Integer a) {
        class A {
            public Integer tcp_(Integer a, Integer b) {
                if (b.equals(0)) {
                    return 0;
                } else if (b.equals(1)) {
                    return a * b;
                } else {
                    return tcp_(a * b, b - 1);
                }
            }
        }
        return new A().tcp_(1, a);
    }

    public static Array<Integer> tcp(Integer a) {
        Array<Integer> rl = Array.empty();
        Integer length = jc21(a);
        if (a == 1) {
            rl.array(1);
        }
        return rl;
    }

}

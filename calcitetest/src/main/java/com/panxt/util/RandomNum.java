package com.panxt.util;

import java.util.Random;

public class RandomNum {

    /**
     * 返回从fromNum到toNum的随机数字
     * @param fromNum
     * @param toNum
     * @return
     */
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }

    public static void main(String[] args) {
        int randInt = getRandInt(100, 105);
        System.out.println(randInt);
    }
}

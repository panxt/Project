package com.panxt.util;


import java.util.Date;
import java.util.Random;

/**
 * 生成随机时间
 */
public class RandomDate {
    Long logDateTime =0L;//
    int maxTimeStep=0 ;


    public RandomDate(Date startDate,Date endDate,int num) {

        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();

    }


    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }

    public static void main(String[] args) {
        Date date = new Date(System.currentTimeMillis());
        Date date1 = new Date(System.currentTimeMillis() - 100000L);

        RandomDate randomDate = new RandomDate(date1,date,2);
        Date randomDate1 = randomDate.getRandomDate();


        System.out.println(randomDate1);
    }



}

package com.panxt.util;

public class RanData<T> {
    T value;
    int weight;


    public RanData(T value, int weight) {
        this.value = value;
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

}

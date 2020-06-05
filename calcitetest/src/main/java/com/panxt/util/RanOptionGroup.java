package com.panxt.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RanOptionGroup<T> {
    int totalWeight = 0;

    List<RanData> optList = new ArrayList();

    public RanOptionGroup(RanData<T>... opts) {
        for (RanData opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }

        }
    }

    public RanData<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }
}

package org.next.test.spark.aggregate;

import lombok.Getter;

import java.io.Serializable;

public class AvgCount implements Serializable {

    static final long serialVersionUID = 42L;

    @Getter
    private long total;
    @Getter
    private long count;

    public AvgCount(long total, long count) {
        this.total = total;
        this.count = count;
    }

    public AvgCount plusTotal(long add) {
        this.total += add;
        return this;
    }

    public AvgCount plusCount(long add) {
        this.count += add;
        return this;
    }

    public AvgCount union(AvgCount avg) {
        return this
                .plusCount(avg.getCount())
                .plusTotal(avg.getTotal());
    }

    public double avg() {
        return count == 0 ? 0.0 : total / (double) count;
    }
}

package org.dava.core.database.service.operations.delete;

public class CountChange {
    private long oldCount;
    private long change;


    public CountChange(long oldCount, long change) {
        this.oldCount = oldCount;
        this.change = change;
    }

    public void decrementNewCount() {
        change--;
    }



    /*
        getter setter
     */

    public long getOldCount() {
        return oldCount;
    }

    public long getChange() {
        return change;
    }
}

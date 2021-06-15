package tb.checksum.processor;

public class Progress {
    private final long total;
    private long cur;
    private final int step;
    private int lastPrintProg = 0;

    public Progress(long total, int step) {
        this.total = total;
        this.step = step;
    }

    public long getTotal() {
        return total;
    }


    public long getCur() {
        return cur;
    }

    public void setCur(long cur) {
        this.cur = cur;
    }

    public boolean tryPrint(){
        int newProg = (int) ((cur * 100)/total);
        if (newProg == 100 || newProg - lastPrintProg > step){
            lastPrintProg = newProg;
            return true;
        }
        return false;
    }

    public void addCur(long size){
        this.cur += size;
    }

    @Override
    public String toString(){
        return ((cur * 100)/total) +"%";
    }
}

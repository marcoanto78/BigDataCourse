package multithread;

public class Threaded implements Runnable {
    double sum;
    long start, end;
    Data data;
    int index=-1000;
    public Threaded(Data data){
        this.data = data;
    }
    public void run(){
        double partialSum = doSum(getIndex());
        setSum(partialSum);
    }
    private synchronized void setSum(double sum){
        this.sum+=sum;
    }
    private synchronized int getIndex(){
        index += 1000;
        return index;
    }
    public double doSum(int i){
        double mySum=0;
        for(int j=0; j<1000; j++){
            mySum += data.getElement(i+j);
        }
        return mySum;
    }
    public void printSum(){
        System.out.println("Sum value is: " + sum);
    }
}
package multithread;

public class Serial {
    double sum;
    long start, end;
    Data data;
    public Serial(Data data){
        this.data = data;
    }

    public void doSum(){
        start=System.currentTimeMillis();
        System.out.println(data.getElement(10));
        for(int i=0; i<10000; i++){
            sum += data.getElement(i);
        }
        end=System.currentTimeMillis();
        System.out.println("Sum value is: " + sum);
        System.out.println("Serial Time: " + (end-start));
    }
}
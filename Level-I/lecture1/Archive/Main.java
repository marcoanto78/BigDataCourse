package multithread;

public class Main {
    public static void main(String[] args){
        Data data = new Data();
        //System.exit(0);
        long start, end;

        Serial serial = new Serial(data);
        serial.doSum();

        Threaded threaded = new Threaded(data);
        start=System.currentTimeMillis();
        for(int i=0; i<9; i++)
            new Thread(threaded).start();
        Thread t = new Thread(threaded);
        t.start();
        try{
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        end=System.currentTimeMillis();
        threaded.printSum();
        System.out.println("Parallel Time: " + (end-start));
    }

}

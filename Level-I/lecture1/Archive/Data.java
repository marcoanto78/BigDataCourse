package multithread;

import java.util.Random;

public class Data {
    int[] data = new int[10000];
    public Data(){
        Random val = new Random();
        for(int i=0; i<10000; i++){
            data[i] = val.nextInt(10000);
        }
    }
    public int getElement(int index){
        if(index>=data.length)
            return 0;
        return data[index];
    }
}

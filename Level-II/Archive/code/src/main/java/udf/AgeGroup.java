package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(
        name="AgeGroup",
        value="returns age group for a given age as input",
        extended="SELECT agegroup(10) from citizen;"
)
public class AgeGroup extends UDF {
    public Text evaluate(IntWritable input) {
        if(input == null) return null;
        int age = input.get();
        if(age > 0 && age<=14)
            return new Text("Children");
        else if(age>14 && age <=24)
            return new Text("Youth");
        else if(age>24 && age<=64)
            return new Text("Adult");
        else
            return new Text("Senior");
    }
}
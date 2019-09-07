package udf;

import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class AgeGroupTest {
    @Test
    public void testUDF() {
        AgeGroup example = new AgeGroup();
        Assert.assertEquals("Children", example.evaluate(new IntWritable(9)).toString());
    }

    @Test
    public void testUDFNullCheck() {
        AgeGroup example = new AgeGroup();
        Assert.assertNull(example.evaluate(null));
    }
}
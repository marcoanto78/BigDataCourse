package group;

import junit.framework.Assert;
import org.junit.Test;

public class PersonTest {
    @Test
    public void testUDF() {
        Person example = new Person("john",12);
        Assert.assertEquals("child", example.getAgeGroup());
    }

    public void testUDFyouth() {
        Person example = new Person("tony",20);
        Assert.assertEquals("youth", example.getAgeGroup());
    }

}
package udf;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class IsAcceptedTest {

    @Test
    public void checkAcceptanceResult() throws HiveException{

        //Set input values
        List<Double> individualScore = new LinkedList<Double>();
        individualScore.add(7.5);
        individualScore.add(7.5);
        individualScore.add(7.0);
        individualScore.add(5.5);

        Double minIndividualScore = 6.0;

        Double minAvgScore = 6.5;

        //create function class instance
        IsAccepted isAccepted = new IsAccepted();

        //Prepare inspector objects
        ObjectInspector doubleOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        ObjectInspector doubleListOI = ObjectInspectorFactory.getStandardListObjectInspector(doubleOI);

        //Get the handle for return value inspector
        JavaBooleanObjectInspector acceptanceResult = (JavaBooleanObjectInspector) isAccepted.initialize(new
                ObjectInspector[] {doubleListOI, doubleOI, doubleOI});

        //Call evaluate method to carry out the actual test
        //1. Needs improvement
        Object result = isAccepted.evaluate(new DeferredObject[]{new DeferredJavaObject(individualScore), new
        DeferredJavaObject(minIndividualScore), new DeferredJavaObject(minAvgScore)} );
        Assert.assertEquals(false, acceptanceResult.get(result));

        //2. Accepted
        minIndividualScore = 5.5;
        result = isAccepted.evaluate(new DeferredObject[]{new DeferredJavaObject(individualScore), new
                DeferredJavaObject(minIndividualScore), new DeferredJavaObject(minAvgScore)} );
        Assert.assertEquals(true, acceptanceResult.get(result));

        //3. Returns null
        result = isAccepted.evaluate(new DeferredObject[]{new DeferredJavaObject(individualScore), new
                DeferredJavaObject(minIndividualScore), new DeferredJavaObject(null)} );
        Assert.assertNull(result);
    }
}
package udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

@Description(
        name="IsAccepted",
        value="returns true for a given individual score list, min individual and average scores expected",
        extended="SELECT isaccepted(List<Double>, Double, Double) from citizen;"
)
public class IsAccepted extends GenericUDF {
    ListObjectInspector individualBands;
    DoubleObjectInspector individualMinScore;
    DoubleObjectInspector avgMinScore;

    @Override
    public String getDisplayString(String[] arg0) {
        return "IsAccepted()"; // this name will be displayed in the explain plan
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check #arguments received. Should be 3
        if (arguments.length != 3) {
            throw new UDFArgumentLengthException("IsAccepted takes only 3 arguments: List<T>, T, T");
        }

        // 1. Check we received the right object types.
        ObjectInspector inputIndividualBandList = arguments[0];
        ObjectInspector inputIndividualMinScore = arguments[1];
        ObjectInspector inputMinAvgScore = arguments[2];

        if (!(inputIndividualBandList instanceof ListObjectInspector) || !(inputIndividualMinScore instanceof DoubleObjectInspector)
                || !(inputMinAvgScore instanceof DoubleObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second and third arguments must be a double, ");
        }

        //Get the input arguments in class instance veriables
        this.individualBands = (ListObjectInspector) inputIndividualBandList;
        this.individualMinScore = (DoubleObjectInspector) inputIndividualMinScore;
        this.avgMinScore = (DoubleObjectInspector) inputIndividualMinScore;

        // 2. Check that the list contains double values
        if(!(individualBands.getListElementObjectInspector() instanceof DoubleObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of doubles");
        }

        // the return type of our function is a boolean, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // get the list and string from the deferred objects using the object inspectors

        List<Double> individualBandsList = (List<Double>) this.individualBands.getList(arguments[0].get());
        Double minIndividualScore = (Double) individualMinScore.getPrimitiveJavaObject(arguments[1].get());
        Double minAvgScore = (Double) avgMinScore.getPrimitiveJavaObject(arguments[2].get());

        // check for nulls
        if (individualBandsList == null || minIndividualScore == null || minAvgScore == null) {
            return null;
        }

        // Decide if the score is acceptable
        double sumVal = 0;
        for(Double band: individualBandsList) {
            if (band < minIndividualScore)
                return new Boolean(false);
            sumVal += band;
        }

        if(sumVal/individualBandsList.size() < minAvgScore)
            return new Boolean(false);
        return new Boolean(true);
    }
}
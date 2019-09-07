package udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.exec.Description;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@Description(name = "HasPassedComplex", value = "_FUNC_(expr) - Returns status of exam")
public class HasPassedComplex extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly three arguments are expected.");
        }

        //Validate column argument
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be Double, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }

        return new HasPassedComplexEvaluator();
    }

    public static class HasPassedComplexEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector inputOI, doubleOI;
        ObjectInspector outputOI;

        double total = 0;
        int count = 0;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {

            assert (parameters.length == 3);
            super.init(m, parameters);

            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            } else {
                doubleOI = (PrimitiveObjectInspector) parameters[0];
            }

            // init output object inspectors
            // For partial function - array of integers
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                    ObjectInspectorOptions.JAVA);

            return outputOI;
        }

        /**
         * class for storing the current sum of letters
         */
        static class ScoreSumAgg implements AggregationBuffer {
            double sum = 0;
            int count = 0;
            double minAvg=0;
            String status = "YES";
            void add(double num, double minInd, double minAvg) {
                sum += num;
                count++;
                if(num < minInd)
                    status = "NO";
                this.minAvg = minAvg;
            }
            void add(double avg) {
                sum += avg;
                count++;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ScoreSumAgg result = new ScoreSumAgg();
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            agg = new ScoreSumAgg();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 3);
            if (parameters[0] != null) {
                ScoreSumAgg myagg = (ScoreSumAgg) agg;
                Double p1 = (Double) inputOI.getPrimitiveJavaObject(parameters[0]);
                Double p2 = (Double) inputOI.getPrimitiveJavaObject(parameters[1]);
                Double p3 = (Double) inputOI.getPrimitiveJavaObject(parameters[2]);
                myagg.add(p1, p2, p3);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ScoreSumAgg myagg = (ScoreSumAgg) agg;
            ArrayList<String> returnObj = new ArrayList<String>(3);
            returnObj.add(0, String.valueOf(myagg.sum));
            returnObj.add(1, String.valueOf(myagg.count));
            returnObj.add(2, myagg.status);
            return myagg.count == 0 ? null : (Object) agg;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial != null) {
                ScoreSumAgg myagg1 = (ScoreSumAgg) agg;
                ArrayList<String> partialAggBuff = (ArrayList<String>) partial;
                myagg1.sum += Double.valueOf(partialAggBuff.get(0));
                myagg1.count += Double.valueOf(partialAggBuff.get(1));
                if(partialAggBuff.get(2).equals("NO")){
                    myagg1.status = "NO";
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ScoreSumAgg myagg = (ScoreSumAgg) agg;
            if (myagg.count == 0)
                return null;
            if(myagg.sum/myagg.count < myagg.minAvg)
                myagg.status = "NO";
            return myagg.status;
        }
    }
}

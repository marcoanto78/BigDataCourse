package udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@Description(name = "hasPassed",
        value = "_FUNC_(col) - UDAF to return result YES or NO based on a individual score")
public class HasPassed extends UDAF{

    public static class TempDataHolder{
        private double sum=0;
        private int count = 0;
        private String status = "YES";
        private double avgMin = 0;
    }

    /**
     * The actual class for doing the aggregation. Hive will automatically look
     * for all internal classes of the UDAF that implements UDAFEvaluator.
   */
    public static class HasPassedEvaluator implements UDAFEvaluator {

        TempDataHolder state;

        public HasPassedEvaluator() {
            super();
            state = new TempDataHolder();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            state.sum = 0;
            state.count = 0;
            state.status = "YES";
            state.avgMin = 0;
        }

        /**
         * Iterate through one row of original data.
         *
         * The number and type of arguments need to the same as we call this UDAF
         * from Hive command line.
         *
         * This function should always return true.
         */
        public boolean iterate(Double indScore, Double minInd, Double avgInd) {
            if (indScore != null && minInd != null && avgInd != null) {
                if(indScore < minInd)
                    state.status = "NO";
                state.sum += indScore;
                state.count++;
                state.avgMin = avgInd;
            }
            else
                state.status = "NULL";

            return true;
        }

        /**
         * Terminate a partial aggregation and return the state. If the state is a
         * primitive, just return primitive Java classes like Integer or String.
         */
        public TempDataHolder terminatePartial() {
            // This is SQL standard - average of zero items should be null.
            return state.count == 0 ? null : state;
        }

        /**
         * Merge with a partial aggregation.
         *
         * This function should always have a single argument which has the same
         * type as the return value of terminatePartial().
         */
        public boolean merge(TempDataHolder o) {
            if (o != null) {
                state.sum += o.sum;
                state.count += o.count;
                if(o.status.equals("NO"))
                    state.status = "NO";
                else if (o.status.equals("NULL"))
                    state.status = "NULL";

            }
            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public String terminate() {
            // This is SQL standard - average of zero items should be null.
            if (state.count == 0)
                return null;
            if((state.sum/state.count) < state.avgMin)
                state.status = "NO";
            return state.status.toString() ;
        }
    }

    private HasPassed() {
        // prevent instantiation
    }

}

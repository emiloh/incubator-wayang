package org.qcri.rheem.core.platform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JsonSerializable;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Captures data of a execution of a set of {@link ExecutionOperator}s.
 */
public class PartialExecution implements JsonSerializable {

    private final long measuredExecutionTime;

    private final Collection<OptimizationContext.OperatorContext> operatorContexts;

    /**
     * Creates a new instance.
     * @param measuredExecutionTime the time measured for the partial execution
     * @param operatorContexts for all executed {@link ExecutionOperator}s
     */
    public PartialExecution(long measuredExecutionTime, Collection<OptimizationContext.OperatorContext> operatorContexts) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.operatorContexts = operatorContexts;
    }

    public static PartialExecution fromJson(JSONObject jsonObject, OptimizationContext optimizationContext) {
        return new PartialExecution(
                jsonObject.getLong("millis"),
                StreamSupport.stream(jsonObject.getJSONArray("opCtxs").spliterator(), false)
                .map(json -> optimizationContext.addOperatorContextFromJson((JSONObject) json))
                .collect(Collectors.toList())
        );
    }

    /**
     * Converts this instance into a {@link JSONObject}.
     * @return the {@link JSONObject}
     */
    public JSONObject toJson() {
        final JSONObject jsonThis = new JSONObject();
        jsonThis.put("millis", this.measuredExecutionTime);
        final JSONArray jsonOpCtxs = new JSONArray();
        jsonThis.put("opCtxs", jsonOpCtxs);
        for (OptimizationContext.OperatorContext operatorContext : this.operatorContexts) {
            jsonOpCtxs.put(operatorContext.toJson());
        }
        return jsonThis;
    }
}

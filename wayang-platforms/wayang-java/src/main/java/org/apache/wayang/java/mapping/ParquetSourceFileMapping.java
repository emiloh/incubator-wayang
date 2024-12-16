package org.apache.wayang.java.mapping;

import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.java.operators.JavaParquetFileSource;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;

/* Heavily inspired by TextFileSourceMapping.java, and some taken verbatim*/

public class ParquetSourceFileMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern(){
        final OperatorPattern<ParquetFileSource> pattern = new OperatorPattern<ParquetFileSource>(
                "ParquetSource", new ParquetFileSource((String) null, new String[0]), false
        );

        return SubplanPattern.createSingleton(pattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory(){
        return new ReplacementSubplanFactory.OfSingleOperators<ParquetFileSource>(
                (matchedOperator, epoch) -> new JavaParquetFileSource(matchedOperator).at(epoch)
        );
    }
}

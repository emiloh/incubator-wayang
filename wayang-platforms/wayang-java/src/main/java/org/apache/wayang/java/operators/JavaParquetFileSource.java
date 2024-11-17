package org.apache.wayang.java.operators;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.wayang.basic.operators.ParquetFileSource;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/* HEAVILY INSPIRED BY JavaTextFileSource and some parts are taken verbatim */

public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator{

    public JavaParquetFileSource(String inputFile) {
        super(inputFile);
    }

    public JavaParquetFileSource(ParquetFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>
        evaluate(ChannelInstance[] inputs,
                 ChannelInstance[] outputs,
                 JavaExecutor javaExecutor,
                 OptimizationContext.OperatorContext operatorContext) {

        String  inputFile = this.getInputFile().trim();


       ParquetReader<GenericRecord> pReader = ParquetReader.<GenericRecord>builder(inputFile)
               .build();

        return null;
    }

    /* Taken from JavaTextFileSource.java - ParquetFileSource does not have any input channels either. */
    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public JavaPlatform getPlatform() {
        return JavaExecutionOperator.super.getPlatform();
    }

    private class ParquetSchema(){
        private final Schema lineorder;
        private final Schema
        private ParquetSchema(){

        }
    }
}

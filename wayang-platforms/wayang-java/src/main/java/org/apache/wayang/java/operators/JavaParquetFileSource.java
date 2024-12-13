package org.apache.wayang.java.operators;

import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetFileSource;

import org.apache.wayang.basic.types.ColumnType;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.platform.lineage.LazyExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.wayang.java.channels.StreamChannel;


import java.awt.geom.GeneralPath;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

/* HEAVILY INSPIRED BY JavaTextFileSource and some parts are taken verbatim */

public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator{

    public JavaParquetFileSource(String inputFile, String[] columnNames) {
        super(inputFile, columnNames);
    }

    public JavaParquetFileSource(String inputFile, String[] columnNames, ColumnType[] columnTypes) {
        super(inputFile, columnNames, columnTypes);
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

        String input = this.getInputFile().trim();
        Path inputPath = new Path(input);

        Configuration config = new Configuration();
        Optional<Schema> schema = this.getSchema();

        if (schema.isPresent()) {
            config.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, schema.toString());
        }

        Stream<GenericRecord> stream = getParquetStream(inputPath, config);
        Stream<Record> streamRecord = convertToWayangRecord(stream);

        ((StreamChannel.Instance) outputs[0]).accept(streamRecord);

        ExecutionLineageNode mainLineage = new ExecutionLineageNode(operatorContext);

        outputs[0].getLineage().addPredecessor(mainLineage);

        return mainLineage.collectAndMark();
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

    private Stream<GenericRecord> getParquetStream(Path path, Configuration config) {
        try {
            InputFile inputFile = HadoopInputFile.fromPath(path, config);
            ParquetReader<GenericRecord> parquetReader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withConf(config)
                    .build();

            List<GenericRecord> records = new ArrayList<>();
            while((parquetReader.read()) != null) {
                records.add(parquetReader.read());
            }
            return records.stream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<Record> convertToWayangRecord(Stream<GenericRecord> stream){
        Function<GenericRecord, Record> mapper = new Function<GenericRecord, Record>() {
            @Override
            public Record apply(GenericRecord genericRecord) {
                int grSize = genericRecord.getSchema().getFields().size();
                Object[] record = new Object[grSize];
                List<Schema.Field> grFields = genericRecord.getSchema().getFields();

                for (int i = 0; i < grSize; i++) {
                    record[i] = genericRecord.get(grFields.get(i).name());
                }

                return new Record(record);
            }
        };

        return stream.map(mapper);
    }
}

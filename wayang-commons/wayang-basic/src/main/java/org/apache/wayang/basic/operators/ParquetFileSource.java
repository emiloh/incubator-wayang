package org.apache.wayang.basic.operators;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.basic.types.ColumnType;

import java.util.Optional;

public class ParquetFileSource extends TableSource {
    // The path to the input file
    private final String inputFile;
    private final Optional<Schema> schema;

    public ParquetFileSource(String inputFile, String[] columnNames) {
        super(inputFile, columnNames);
        this.inputFile = inputFile;

        this.schema = createSchema(columnNames);
    }
    public ParquetFileSource(String inputFile, String[] columnNames, ColumnType[] columnTypes) {
        super(inputFile, columnNames);
        this.inputFile = inputFile;

        this.schema = createSchema(columnNames, columnTypes);
    }
    public ParquetFileSource(TableSource that) {
        super(that);
        this.inputFile = that.getTableName();

        this.schema = createSchema(new String[1]);
    }

    public String getInputFile() {
        return inputFile;
    }

    public Optional<Schema> getSchema() {
        return schema;
    }

    private Optional<Schema> createSchema(String[] columnNames, ColumnType... columnTypes) {
        if ((columnNames.length != columnTypes.length) | (columnNames == null || columnNames.length == 0)
                | ( columnTypes == null || columnTypes.length == 0 ) ) {
            return Optional.empty();
        }

        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("record").fields();

        for (int i = 0; i < columnNames.length; i++) {
            switch (columnTypes[i]) {
                case STRING:
                    builder.requiredString(columnNames[i]);
                    break;
                case DOUBLE:
                    builder.requiredDouble(columnNames[i]);
                    break;
                case BOOLEAN:
                    builder.requiredBoolean(columnNames[i]);
                    break;
                case INTEGER:
                    builder.requiredInt(columnNames[i]);
                    break;
                case LONG:
                    builder.requiredLong(columnNames[i]);
                    break;
                case FLOAT:
                    builder.requiredFloat(columnNames[i]);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + columnTypes[i]);
            }
        }

        Schema schema = builder.endRecord();

        return Optional.of(schema);
    }
}

package org.apache.wayang.basic.operators;

public class ParquetFileSource extends TableSource {

    // The path to the input file
    private final String inputFile;

    public ParquetFileSource(String inputFile, String... columnNames) {
        super(inputFile, columnNames);
        this.inputFile = inputFile;
    }

    public ParquetFileSource(TableSource that) {
        super(that);
        this.inputFile = that.getTableName();
    }

    public String getInputFile() {
        return inputFile;
    }
}

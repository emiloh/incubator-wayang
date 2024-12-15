package org.apache.wayang.apps.parquet_csv;

import org.apache.wayang.api.DistinctDataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.ColumnType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetFileSource;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class TpchPartBench {
    public static void main(String[] args) {

        if (args.length == 0) {
            System.err.println("Specify path to data folder and data file");
            System.exit(1);
        }

        String inputFolder = args[1];
        String outputFile = args[2];

        System.out.println("Input folder: " + inputFolder);
        System.out.println("Output file: " + outputFile);


        try {
            FileWriter writer = new FileWriter(outputFile);

            int[] sfs = new int[]{1, 10, 100};
            int[] runs = new int[]{20, 20, 10};

            for (int i = 0; i < sfs.length; i++) {
                String inputParquet = inputFolder.concat(String.format("tpch_part_%s.parquet", sfs[i]));
                String inputCsv = inputFolder.concat(String.format("tpch_part_%s.csv", sfs[i]));
                writer.write(String.format("--- TPCH Part benchmark - SF%s  ---\n", sfs[i]));
                reportCsv(inputCsv, writer, runs[i], sfs[i]);
                reportParquet(inputParquet, writer, runs[i], sfs[i], false);
                reportParquet(inputParquet, writer, runs[i], sfs[i], true);
            }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void reportCsv(String filepath, FileWriter writer, int runs, int sf) throws IOException {
        // Create wayang context
        WayangContext context = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        // Create plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("yelp_csv")
                .withUdfJarOf(YelpBench.class);

        long[] times = new long[runs];

        // Collect all zero labels and count them
        for (int i = 0; i < runs; i++) {
            DistinctDataQuantaBuilder<String> csv = planBuilder
                    .readTextFile(filepath)
                    .map(row -> row.split(",")[0]).withName("Get partkeys")
                    .distinct();

            long startTime = System.currentTimeMillis();
            csv.collect();
            long endTime = System.currentTimeMillis();

            times[i] = endTime - startTime;
        }

        double mean = Arrays.stream(times).average().getAsDouble();
        double variance = Arrays.stream(times).mapToDouble(time -> Math.pow((time - mean), 2)).average().getAsDouble();
        double stddev = Math.sqrt(variance);

        writer.write(String.format("csv - %.6f - %.6f\n", mean, stddev));
    }

    private static void reportParquet(String filepath, FileWriter writer, int runs, int sf, boolean projection) throws IOException {
        // Create wayang context
        WayangContext context = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        // Create plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName(String.format("yelp_parquet%s", (projection ? "_projection" :"")))
                .withUdfJarOf(YelpBench.class);

        JavaParquetFileSource fileSource = new JavaParquetFileSource(filepath, new String[]{"P_PARTKEY"});
        if (projection) {
            fileSource = new JavaParquetFileSource(filepath, new String[]{"P_PARTKEY"}, new ColumnType[]{ColumnType.OPTIONAL_LONG});
        }

        long[] times = new long[runs];

        // Collect all zero labels and count them
        for (int i = 0; i < runs; i++) {
            DistinctDataQuantaBuilder<Long> parquet = planBuilder
                    .readParquet(fileSource)
                    .map(l -> l.getLong(0)).withName("Get partkeys")
                    .distinct();

            long startTime = System.currentTimeMillis();
            parquet.collect();
            long endTime = System.currentTimeMillis();

            times[i] = endTime - startTime;
        }

        double mean = Arrays.stream(times).average().getAsDouble();
        double variance = Arrays.stream(times).mapToDouble(time -> Math.pow((time - mean), 2)).average().getAsDouble();
        double stddev = Math.sqrt(variance);

        writer.write(String.format("parquet %s - %.6f - %.6f\n", (projection ? "projection" : ""), mean, stddev));
    }
}



package org.apache.wayang.apps.parquet_csv;

import org.apache.flink.util.CollectionUtil;
import org.apache.wayang.api.CountDataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.basic.types.ColumnType;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetFileSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

public class Main {
    public static void main(String[] args) {

        if (args.length != 2) {
            System.err.println("Only specify path to data folder");
            System.exit(1);
        }

        String inputFolder = args[1];
        System.out.println("Input folder: " + inputFolder);

        // Create a wayang context
        WayangContext context = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());

        // Get a plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("yelp_number_of_0_labels")
                .withUdfJarOf(Main.class);

        // Create wayang plan for parquet with projection
        /*Collection<Long> zero_labels = planBuilder
                .readParquet(new JavaParquetFileSource(inputFolder.concat("train_yelp.parquet"), new String[]{"label"}, new ColumnType[]{ColumnType.INTEGER}))
                .filter(l -> l.getInt(0) == 0).withName("Remove non-zero labels")
                .count()
                .collect();
        */
        CountDataQuantaBuilder<String> zero_labels_csv = planBuilder
                .readTextFile(inputFolder.concat("train_yelp.csv"))
                .filter(row -> row.startsWith("0")).withName("Remove non-zero labels")
                .count();

        long startTime = System.currentTimeMillis();

        Collection<Long> csv_res = zero_labels_csv.collect();

        long endTime = System.currentTimeMillis();

        System.out.println("Number of zero labels: " + csv_res.size() + ", execution time (ms): " + (endTime - startTime));


    }
}

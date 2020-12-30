import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

public static void main(String [] args){

    Logger.getLogger("org.apache").setLevel(Level.OFF);

    SparkSession spark = SparkSession.builder()
            .appName("Combine 2 Datasets")
            .master("local")
            .getOrCreate();

    Dataset<Row> durhamDF = buildDurhamParkDataFrames(spark);
    durhamDF.printSchema();
    durhamDF.show(10);

}

public static Dataset<Row> buildDurhamParkDataFrames(SparkSession spark){

    Dataset<Row> df = spark.read().format("json").option("multiline",true)
            .load("src/main/ResourceFiles/durham-parks.json");

    df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
            df.col("fields.objectid"), lit("_Durham")))
            .withColumn("park_name", df.col("fields.park_name"))
            .withColumn("city", lit("Durham"))
            .withColumn("address", df.col("fields.address"))
            .withColumn("has_playground", df.col("fields.playground"))
            .withColumn("zipcode", df.col("fields.zip"))
            .withColumn("land_in_acres", df.col("fields.acres"))
            .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
            .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
            .drop("fields").drop("geometry").drop("record_timestamp").drop("recordid")
            .drop("datasetid");

    return df;

}



public static void practice(){
    //    System.setProperty("hadoop.home.dir", "c:hadoop");
//    Logger.getLogger("org.apache").setLevel(Level.OFF);


    SparkSession spark = SparkSession.builder().appName("testingsql").master("local")
            .getOrCreate();

    //Stop the errors
    spark.sparkContext().setLogLevel("ERROR");

    Dataset<Row> dataset = spark.read()
//            .format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .csv("/Users/faradjanondo/Desktop/resources/exams/students.csv");
    dataset.show(10);
    dataset.printSchema();
    Row firstRow = dataset.first();
    System.out.println("\n");
    String subject = firstRow.get(2).toString();
    System.out.println("Subject\n: "+ subject);
//     long counting = dataset.count();
//     System.out.println("WHY NOT PRINT");
//    System.out.println("Count is :"+dataset.count());


    spark.close();
}
}

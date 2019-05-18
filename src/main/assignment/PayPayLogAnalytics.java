import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.collection.JavaConverters;
import scala.collection.JavaConverters.*;

import javax.xml.crypto.Data;

public class PayPayLogAnalytics {
    final static SparkSession sparkSession = SparkSession.builder().appName("DataEngineerChallenge_Rishav").master("local[*]").getOrCreate();
    final static Utility utility = new Utility();

    private Dataset loadAndPreprocessLogs(String logFilePath) {
        Dataset<String> dataset = sparkSession.read().textFile(logFilePath);
        Dataset parsedDataset = dataset.map((MapFunction<String, Row>) line -> utility.parseLine(line), RowEncoder.apply(utility.createAndGetSchema()));
        parsedDataset = parsedDataset.withColumn("timeStamp", parsedDataset.col("timeStamp").cast(DataTypes.TimestampType));
        return parsedDataset;
    }

    /**
     * 1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
     * Method to SessionNize ,i.e., aggregrate all page hits by visitor/IP during a session
     *
     * @param mainDataset
     * @param sessionTime
     * @param unitOfTime
     * @return sessionDataset
     */
    public Dataset sessionize(Dataset mainDataset, int sessionTime, String unitOfTime) {
        Dataset sessionDataset = mainDataset.select(functions.window(mainDataset.col("timeStamp"), (sessionTime + " " + unitOfTime)).as("fixedSessionWindow"), mainDataset.col("timeStamp"), mainDataset.col("clientIp")).groupBy("fixedSessionWindow", "clientIp").count().withColumnRenamed("count", "numberHitsInSessionForIp");
        sessionDataset = sessionDataset.withColumn("sessionId", functions.monotonicallyIncreasingId());
        return sessionDataset;
    }

    /**
     * Method to find session duration
     *
     * @param mainDataset
     * @param sessionizeDataset
     * @return dataset with each user session duration within a timewindow
     */
    public Dataset calculateSessionDurations(Dataset mainDataset, Dataset sessionizeDataset) {
        Dataset datasetWithTimeStamps = mainDataset.select(functions.window(mainDataset.col("timeStamp"), "15 minutes").alias("fixedSessionWindow"), mainDataset.col("timeStamp"), mainDataset.col("clientIp"), mainDataset.col("request"));
        List<String> columsToJoinAt = new ArrayList<>();
        columsToJoinAt.add("fixedSessionWindow");
        columsToJoinAt.add("clientIp");
        Dataset sessionizeWithDurationDataset = datasetWithTimeStamps.join(sessionizeDataset, JavaConverters.asScalaBuffer(columsToJoinAt).toSeq());
        Dataset firstHitTimeStamps = sessionizeWithDurationDataset.groupBy("sessionId").agg(functions.min("timeStamp").alias("firstHitTimeStamp"));
        sessionizeWithDurationDataset = firstHitTimeStamps.join(sessionizeWithDurationDataset, "sessionId");
        sessionizeWithDurationDataset = sessionizeWithDurationDataset.withColumn("timeDiffwithFirstHit",
                functions.unix_timestamp(sessionizeWithDurationDataset.col("timeStamp"))
                        .minus(functions.unix_timestamp(sessionizeWithDurationDataset.col("firstHitTimeStamp"))));
        Dataset tmpdf = sessionizeWithDurationDataset.groupBy("sessionId").agg(functions.max("timeDiffwithFirstHit").alias("sessionDuration"));
        sessionizeWithDurationDataset = sessionizeWithDurationDataset.join(tmpdf, "sessionId");
        return sessionizeWithDurationDataset;
    }

    public void executor(String logFilePath) {
        Dataset mainDataset = loadAndPreprocessLogs(logFilePath);
        mainDataset.cache();
        Dataset sessionizeDataset = sessionize(mainDataset, 15, "minutes");
        sessionizeDataset.cache();
        Dataset sessionizeWithDurationDataset = calculateSessionDurations(mainDataset, sessionizeDataset);
        sessionizeWithDurationDataset.cache();
        Dataset averageSessionDurationsDataset = calculateAvergaeSessionDurations(sessionizeWithDurationDataset);
        Dataset uniqueVisitDataset = determineUniqueURLVisitPerSession(sessionizeWithDurationDataset);
        Dataset mostEngaged = mostEngagedUsers(sessionizeWithDurationDataset);
        mainDataset.write().save("./output/mainDataset.parquet");
        sessionizeDataset.write().save("./output/sessionizeDataset.parquet");
        averageSessionDurationsDataset.write().save("./output/averageSessionDurationsDataset.parquet");
        uniqueVisitDataset.write().save("./output/uniqueVisitDataset.parquet");
        mostEngaged.write().save("./output/mostEngaged.parquet");
    }

    /**
     *
     * @param sessionizeWithDurationDataset
     * @return mostEngaged Users
     */
    public Dataset mostEngagedUsers(Dataset sessionizeWithDurationDataset) {
        Dataset mostEngaged = sessionizeWithDurationDataset.select("clientIp", "sessionId", "sessionDuration").sort(sessionizeWithDurationDataset.col("sessionDuration").desc()).distinct();
        return mostEngaged;
    }

    /**
     * 3.Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
     *
     * @param sessionizeDataset
     * @return uniqueVisitDataset
     */
    public Dataset determineUniqueURLVisitPerSession(Dataset sessionizeDataset) {
        Dataset uniqueVisitDataset = sessionizeDataset.groupBy("sessionId", "request").count().distinct().withColumnRenamed("count", "hitURLcount");
        return uniqueVisitDataset;
    }

    /**
     * 2.Determine the average session time
     * Method to find average session Duration
     *
     * @param sessionizeWithDurationDataset
     * @return
     */
    public Dataset calculateAvergaeSessionDurations(Dataset sessionizeWithDurationDataset) {
        Dataset averageSessionDurationsDataset=sessionizeWithDurationDataset.select(functions.avg("sessionDuration").alias("averageSessionDuration"));
        return averageSessionDurationsDataset;
    }

    public static void main(String[] args) {
        String logPath = "data/input_log_file.log";
        PayPayLogAnalytics logAnalytics = new PayPayLogAnalytics();
        logAnalytics.executor(logPath);
    }

//    public static void main(String[] args) {
//        Dataset d=sparkSession.read().parquet("output/sessionizeDataset.parquet").sort("sessionId");
//        Dataset x=d.groupBy().agg(functions.avg("sessionId")).alias("averageSessionDuration");
//        x.show(false);
//    }
}

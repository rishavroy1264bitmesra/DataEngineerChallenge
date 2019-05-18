import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Executor {
    final static SparkSession sparkSession = SparkSession.builder().appName("DataEngineerChallenge_Rishav").master("local[*]").getOrCreate();

    public void logAnalyticsExecutor(String logFilePath) {
        PayPayLogAnalytics logAnalytics = new PayPayLogAnalytics();
        logAnalytics.executor(logFilePath);
    }

    public void loadAndShowLogAnalyticsResults() {
        Dataset mainDataset = sparkSession.read().parquet("./output/mainDataset.parquet");
        Dataset sessionizeDataset = sparkSession.read().parquet("./output/sessionizeDataset.parquet");
        Dataset averageSessionDurationsDataset = sparkSession.read().parquet("./output/averageSessionDurationsDataset.parquet");
        Dataset uniqueVisitDataset = sparkSession.read().parquet("./output/uniqueVisitDataset.parquet");
        Dataset mostEngaged = sparkSession.read().parquet("./output/mostEngaged.parquet");
        mostEngaged=mostEngaged.sort(mostEngaged.col("sessionDuration").desc());
        mainDataset.show(false);
        sessionizeDataset.show(false);
        averageSessionDurationsDataset.show(false);
        uniqueVisitDataset.show(false);
        mostEngaged.show(20,false);
    }


    public static void main(String[] args) {
        Executor executor=new Executor();
        executor.loadAndShowLogAnalyticsResults();
        String logPath = "data/input_log_file.log";
//        executor.logAnalyticsExecutor(logPath);
    }
}

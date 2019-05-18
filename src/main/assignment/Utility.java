import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utility {
    final static Pattern PATTERN = Pattern.compile("^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) (\\S+) \"(\\S+ \\S+ \\S+)\" \"([^\"]*)\" (\\S+) (\\S+)");
    public Row parseLine(String lineText){
        Matcher matcher=PATTERN.matcher(lineText);
        matcher.find();
        String[] backendIpPort = {"-", "-"};
        String[] clientIpPort = {"-", "-"};
        if (!matcher.group(4).equalsIgnoreCase("-")) backendIpPort = matcher.group(4).split(":");
        if (!matcher.group(3).equalsIgnoreCase("-")) clientIpPort = matcher.group(3).split(":");
        return RowFactory.create(matcher.group(1), matcher.group(2), clientIpPort[0], clientIpPort[1], backendIpPort[0], backendIpPort[1], matcher.group(5), matcher.group(6), matcher.group(7), matcher.group(8), matcher.group(9), matcher.group(10), matcher.group(11), matcher.group(12), matcher.group(13), matcher.group(14), matcher.group(15)) ;
    }

    public StructType createAndGetSchema(){
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("timeStamp", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("loadBalancerName", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("clientIp", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("clientPort", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("backendIp", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("backendPort", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("requestProcessingTimeInSeconds", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("backendProcessingTimeInSeconds", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("responseProcessingTime", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("elbStatusCode", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("backendStatusCode", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("receivedBytes", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sentBytes", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("request", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("userAgent", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sslCipher", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sslProtocol", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        return structType;
    }
}

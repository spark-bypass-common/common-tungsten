package com.qunar.spark.tungsten.api;

import com.qunar.spark.base.json.JsonMapper;
import com.qunar.spark.tungsten.base.CommonEncoders;
import com.qunar.spark.tungsten.base.CoreIO;
import com.qunar.spark.tungsten.base.SparkSessions;
import com.qunar.spark.tungsten.base.TypeConverter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 生成与使用{@link org.apache.spark.sql.Dataset}的通用java入口类
 */
public class JDataSets {

    private SparkSession sparkSession = SparkSessions.getSparkSession();

    private <T> JDataSet<T> createFromDataset(Dataset<T> dataset) {
        return new JDataSet<>(dataset);
    }

    public JDataSet<String> readTextFromHdfs(String path) {
        Dataset<String> dataset = CoreIO.read(path);
        return createFromDataset(dataset);
    }

    public <T> JDataSet<T> readTextFromHdfs(String path, MapFunction<String, T> convert) {
        Dataset<T> dataset = CoreIO.read(path)
                .map(convert, CommonEncoders.<T>encoderForSingle(TypeConverter.classToTypeTag()));
        return createFromDataset(dataset);
    }

    public JDataSet<String> readLzoFromHdfs(String path) {
        Dataset<String> dataset = CoreIO.readLZO(path);
        return createFromDataset(dataset);
    }

    public <T> JDataSet<T> readLzoFromHdfs(String path, MapFunction<String, T> convert) {
        Dataset<T> dataset = CoreIO.readLZO(path)
                .map(convert, CommonEncoders.<T>encoderForSingle(TypeConverter.classToTypeTag()));
        return createFromDataset(dataset);
    }

    public <T> void writeToHdfs(JDataSet<T> content, String path) {
        content.map(JsonMapper::writeValueAsString).getInnerDataset().write().save();
    }

}

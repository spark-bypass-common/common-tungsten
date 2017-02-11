package com.qunar.spark.tungsten.api.java;

import com.fasterxml.jackson.core.type.TypeReference;
import com.qunar.spark.tungsten.api.DataSet;
import com.qunar.spark.tungsten.base.SparkSessions;
import com.qunar.spark.tungsten.base.TypeConverter$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.reflect.ClassTag;
import scala.reflect.api.TypeTags;

/**
 * 生成与使用{@link org.apache.spark.sql.Dataset}的通用java入口类
 */
public class JDataSets {

    private SparkSession sparkSession = SparkSessions.getSparkSession();

    private <T> DataSet<T> createFromDataset(Dataset<T> dataset) {
        return new DataSet<>(dataset, ScalaConverterDriver.getTypeTagOfT());
    }



    /**
     * scala-java转换的驱动封装,隐藏复杂的scala api调用
     */
    static final class ScalaConverterDriver {

        /**
         * 借用Jackson的{@link TypeReference}获得泛型T所对应的实际类型
         */
        @SuppressWarnings("unchecked")
        static <T> Class<T> getClassOfT() {
            return (Class<T>) new TypeReference<T>() {
            }.getType();
        }

        /**
         * 由{@link Class<T>}转换为{@link ClassTag<T>}
         */
        static <T> ClassTag<T> getClassTagOfT() {
            return scala.reflect.ClassTag$.MODULE$.apply(getClassOfT());
        }

        /**
         * 由{@link Class<T>}转换为{@link TypeTags.TypeTag<T>}
         */
        static <T> TypeTags.TypeTag<T> getTypeTagOfT() {
            return TypeConverter$.MODULE$.classToTypeTag();
        }

    }

}

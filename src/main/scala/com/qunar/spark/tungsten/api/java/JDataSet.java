package com.qunar.spark.tungsten.api.java;

import com.qunar.spark.tungsten.api.DataSet;
import com.qunar.spark.tungsten.base.CommonEncoders;
import com.qunar.spark.tungsten.base.TypeConverter;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConversions$;
import scala.collection.Seq;
import scala.reflect.api.TypeTags;

import java.util.List;

/**
 * {@link DataSet}的java版本
 */
public class JDataSet<T> {

    private Dataset<T> innerDataset;

    private Dataset<T> getInnerDataset() {
        return innerDataset;
    }

    protected JDataSet(Dataset<T> dataset) {
        this.innerDataset = dataset;
    }

    /**
     * 以下四个常用的算子专为 java api 设计,所有的方法签名都屏蔽了
     * {@link scala.reflect.api.TypeTags.TypeTag},而将实现细节封
     * 装在方法内部,以求给java开发者提供一组简洁的接口.
     */

    public JDataSet<T> filter(FilterFunction<T> func) {
        Dataset<T> newDataset = innerDataset.filter(func);
        return new JDataSet<>(newDataset);
    }

    public <U> JDataSet<U> map(MapFunction<T, U> func) {
        Dataset<U> newDataset = innerDataset.map(func, CommonEncoders.<U>encoderForSingle(TypeConverter.classToTypeTag()));
        return new JDataSet<>(newDataset);
    }

    public <U> JDataSet<U> mapPartitions(MapPartitionsFunction<T, U> func) {
        Dataset<U> newDataset = innerDataset.mapPartitions(func, CommonEncoders.<U>encoderForSingle(TypeConverter.classToTypeTag()));
        return new JDataSet<>(newDataset);
    }

    public <U> JDataSet<U> flatMap(FlatMapFunction<T, U> func) {
        Dataset<U> newDataset = innerDataset.flatMap(func, CommonEncoders.<U>encoderForSingle(TypeConverter.classToTypeTag()));
        return new JDataSet<>(newDataset);
    }

    /**
     * 为了便于使用{@link DataSet}封装的复杂的join类算子,以复用代码,这里专门提出来
     * 一个内部类以作其使用接口
     */
    private static class DataSetDriver<T> {

        public static DataSet<T> getDataSet() {
            return new DataSet<T>(innerDataset, TypeConverter.classToTypeTag());
        }

        public <A> JDataSet<A> dataSetToJDataSet(DataSet<A> dataSet) {
            Dataset<A> dataset = dataSet.getInnerDataset();
            return new JDataSet<>(dataset);
        }

        public <A> DataSet<A> jDataSetToDataSet(JDataSet<A> jDataSet) {
            Dataset<A> dataset = jDataSet.getInnerDataset();
            return new DataSet<>(dataset, TypeConverter.classToTypeTag());
        }

    }



    /* join相关的连接算子 */

    /**
     * <strong>左外连接算子</strong>
     * </p>
     * 包装{@link DataSet}的{@link DataSet#leftOuterJoin}方法,复用复杂的转换逻辑
     *
     * @param genJoinKey 数据集记录生成key的函数
     */
    public <K> JDataSet<Tuple2<T, T>> leftOuterJoin(JDataSet<T> anotherJDataSet, Function1<T, K> genJoinKey) {
        DataSet<Tuple2<T, T>> dataset = dataSet.leftOuterJoin(jDataSetToDataSet(anotherJDataSet), genJoinKey, TypeConverter.classToTypeTag());
        return dataSetToJDataSet(dataset);
    }

    /**
     * <strong>cogroup算子</strong>
     * </p>
     * 包装{@link DataSet}的{@link DataSet#cogroup}方法,复用复杂的转换逻辑
     *
     * @param genJoinKey 数据集记录生成key的函数
     */
    public <K> JDataSet<Tuple2<List<T>, List<T>>> cogroup(JDataSet<T> anotherJDataSet, Function1<T, K> genJoinKey) {
        DataSet<Tuple2<Seq<T>, Seq<T>>> dataset = dataSet.cogroup(jDataSetToDataSet(anotherJDataSet), genJoinKey, TypeConverter.classToTypeTag());
        JDataSet<Tuple2<Seq<T>, Seq<T>>> jDataSet = dataSetToJDataSet(dataset);
        // scala集合转换为java集合
        return jDataSet.map((tuple) -> {
            List<T> listLeft = JavaConversions$.MODULE$.seqAsJavaList(tuple._1);
            List<T> listRight = JavaConversions$.MODULE$.seqAsJavaList(tuple._2);
            return new Tuple2<>(listLeft, listRight);
        });
    }

}

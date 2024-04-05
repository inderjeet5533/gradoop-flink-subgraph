package org.gradoop.flink.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataSetImpl<T> implements DataSet<T> {

    private final StreamExecutionEnvironment env;
    private final DataStream<T> internalStream;
    private final TypeInformation<T> typeInfo;

    public DataSetImpl(StreamExecutionEnvironment env, DataStream<T> internalStream, TypeInformation<T> typeInfo) {
        this.env = env;
        this.internalStream = internalStream;
        this.typeInfo = typeInfo;
    }

    @Override
    public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
        if (filter == null) {
            throw new NullPointerException("Filter function must not be null.");
        } else {
            return internalStream.filter(filter);
        }
    }

    @Override
    public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
        return new JoinedStreams(internalStream, otherStream);
    }

    @Override
    public DataStream<T> union(DataStream<T>... streams) {
        return internalStream.union(streams);
    }

    @Override
    public <K> KeyedStream<T, K> distinct(KeySelector<T, K> key) {
        return internalStream.keyBy(key);
    }

    @Override
    public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {
        return internalStream.flatMap(flatMapper);
    }

}

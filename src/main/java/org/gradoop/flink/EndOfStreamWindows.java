package org.gradoop.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Collection;
import java.util.Collections;

public class EndOfStreamWindows {

    public static <T> WindowAssigner<Object, GlobalWindow> get() {
        return new WindowAssigner<Object, GlobalWindow>() {
            @Override
            public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
                return Collections.singletonList(GlobalWindow.get());
            }

            @Override
            public Trigger<Object, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
                return null;
            }

            @Override
            public TypeSerializer<GlobalWindow> getWindowSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public boolean isEventTime() {
                return false;
            }

            @Override
            public String toString() {
                return "EndOfStreamWindows";
            }
        };
    }
}

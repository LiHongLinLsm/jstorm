/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.common.metric.old.window;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.common.metric.old.operator.Sampling;
import com.alibaba.jstorm.common.metric.old.operator.convert.Convertor;
import com.alibaba.jstorm.common.metric.old.operator.merger.Merger;
import com.alibaba.jstorm.common.metric.old.operator.updater.Updater;
import com.alibaba.jstorm.utils.IntervalCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//所有的metric都需要在component初始化中被设置。
//注册metrics后，只是在定时进行记录metrics，但metrics该如何显示，
// 这就取决于IMetricsConsumer，在Config中可以手动进行注册自定义的metricsConsumer，
// 也可以直接使用storm中提供的记录日志的LoggingMetricsConsumer，该consumer会以日志的形式记录统计指标
public class Metric<T, V> implements Sampling<Map<Integer, T>> {
    private static final long serialVersionUID = -1362345159511508074L;
    private static final Logger LOG = LoggerFactory.getLogger(Metric.class);

    protected static boolean enable;

    public static void setEnable(boolean e) {
        enable = e;
    }

    //统计部分历史数据
    protected List<RollingWindow<V>> rollingWindows;
    //统计所有历史数据
    protected AllWindow<V> allWindow;

    protected int[] windowSeconds = { StatBuckets.MINUTE_WINDOW, StatBuckets.HOUR_WINDOW, StatBuckets.DAY_WINDOW };
    protected int bucketSize = StatBuckets.NUM_STAT_BUCKETS;
    protected V defaultValue;
    protected Updater<V> updater;
    protected Merger<V> merger;
    protected Convertor<V, T> convertor;
    protected Callback callback;

    protected int interval; // unit is second
    protected IntervalCheck intervalCheck;
    //由于所有的rollingWindows个数为3个（比如15,30,60分钟的），couterMetric只是记录couter信息，15,30,60分钟采样，
    //所以，该值对所有窗口，只需要一个，而不是数组。
    protected V unflushed;

    public Metric() {
    }

    //改方法求得所有inteval值得最大公约数。比如interval[]={4,8,6}，则结果为2.
    public int getInterval() {
        if (windowSeconds == null || windowSeconds.length == 0) {
            return StatBuckets.NUM_STAT_BUCKETS;
        }

        int intervals[] = new int[windowSeconds.length];
        int smallest = Integer.MAX_VALUE;
        for (int i = 0; i < windowSeconds.length; i++) {
            int interval = windowSeconds[i] / bucketSize;
            intervals[i] = interval;
            if (interval < smallest) {
                smallest = interval;
            }
        }

        for (int goodInterval = smallest; goodInterval > 1; goodInterval--) {
            boolean good = true;
            for (int interval : intervals) {
                if (interval % goodInterval != 0) {
                    good = false;
                    break;
                }
            }

            if (good == true) {
                return goodInterval;
            }
        }

        return 1;
    }

    public void init() {
        if (defaultValue == null || updater == null || merger == null || convertor == null) {
            throw new IllegalArgumentException("Invalid argements");
        }

        rollingWindows = new ArrayList<RollingWindow<V>>();
        if (windowSeconds != null) {
            rollingWindows.clear();
            for (int windowSize : windowSeconds) {
                RollingWindow<V> rollingWindow = new RollingWindow<V>(defaultValue, windowSize / bucketSize, windowSize, updater, merger);
                rollingWindows.add(rollingWindow);
            }
        }

        allWindow = new AllWindow<V>(defaultValue, updater, merger);

        this.interval = getInterval();
        this.intervalCheck = new IntervalCheck();
        this.intervalCheck.setInterval(interval);
    }

    /**
     * In order to improve performance Do
     */
    @Override
    public void update(Number obj) {
        if (enable == false) {
            return;
        }

        if (intervalCheck.check()) {
            flush();
        }
        synchronized (this) {
            unflushed = updater.update(obj, unflushed);
        }
    }

    public synchronized void flush() {
        if (unflushed == null) {
            return;
        }
        for (RollingWindow<V> rollingWindow : rollingWindows) {
            rollingWindow.updateBatch(unflushed);
        }
        allWindow.updateBatch(unflushed);
        unflushed = null;
    }

    //key:窗口长度，比如15分钟，30分钟，val：统计数据。。
    @Override
    public Map<Integer, T> getSnapshot() {
        flush();
        Map<Integer, T> ret = new TreeMap<Integer, T>();
        for (RollingWindow<V> rollingWindow : rollingWindows) {
            //getsnap中只是合并了小窗口，并没计算最终统计数值。
            V value = rollingWindow.getSnapshot();
            //计算统计数据，由用户自定义convert转换得到。
            ret.put(rollingWindow.getWindowSecond(), convertor.convert(value));
        }

        ret.put(StatBuckets.ALL_TIME_WINDOW, convertor.convert(allWindow.getSnapshot()));

        if (callback != null) {
            callback.execute(this);
        }
        return ret;
    }

    public T getAllTimeValue() {
        return convertor.convert(allWindow.getSnapshot());
    }

    public int[] getWindowSeconds() {
        return windowSeconds;
    }

    public void setWindowSeconds(int[] windowSeconds) {
        this.windowSeconds = windowSeconds;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public V getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(V defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Updater<V> getUpdater() {
        return updater;
    }

    public void setUpdater(Updater<V> updater) {
        this.updater = updater;
    }

    public Merger<V> getMerger() {
        return merger;
    }

    public void setMerger(Merger<V> merger) {
        this.merger = merger;
    }

    public Convertor<V, T> getConvertor() {
        return convertor;
    }

    public void setConvertor(Convertor<V, T> convertor) {
        this.convertor = convertor;
    }

    public Callback getCallback() {
        return callback;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

}

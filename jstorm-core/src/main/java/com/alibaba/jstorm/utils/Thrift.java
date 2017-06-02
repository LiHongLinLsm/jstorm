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
package com.alibaba.jstorm.utils;

import backtype.storm.generated.*;
import backtype.storm.generated.StormTopology._Fields;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.IBolt;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.cluster.StormStatus;
import com.alibaba.jstorm.daemon.nimbus.StatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Thrift utils
 * 
 * 2012-03-28
 * 
 * @author yannian
 * 
 */
public class Thrift {
    private static Logger LOG = LoggerFactory.getLogger(Thrift.class);

    public static StormStatus topologyInitialStatusToStormStatus(TopologyInitialStatus tStatus) {
        if (tStatus.equals(TopologyInitialStatus.ACTIVE)) {
            return new StormStatus(StatusType.active);
        } else {
            return new StormStatus(StatusType.inactive);
        }
    }

    public static CustomStreamGrouping instantiateJavaObject(JavaObject obj) {

        List<JavaObjectArg> args = obj.get_args_list();
        Class[] paraTypes = new Class[args.size()];
        Object[] paraValues = new Object[args.size()];
        for (int i = 0; i < args.size(); i++) {
            JavaObjectArg arg = args.get(i);
            paraValues[i] = arg.getFieldValue();

            if (arg.getSetField().equals(JavaObjectArg._Fields.INT_ARG)) {
                paraTypes[i] = Integer.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.LONG_ARG)) {
                paraTypes[i] = Long.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.STRING_ARG)) {
                paraTypes[i] = String.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.BOOL_ARG)) {
                paraTypes[i] = Boolean.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.BINARY_ARG)) {
                paraTypes[i] = ByteBuffer.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.DOUBLE_ARG)) {
                paraTypes[i] = Double.class;
            } else {
                paraTypes[i] = Object.class;
            }
        }

        try {
            Class clas = Class.forName(obj.get_full_class_name());
            Constructor cons = clas.getConstructor(paraTypes);
            return (CustomStreamGrouping) cons.newInstance(paraValues);
        } catch (Exception e) {
            LOG.error("instantiate_java_object fail", e);
        }

        return null;

    }

    public static Grouping._Fields groupingType(Grouping grouping) {
        return grouping.getSetField();
    }

    public static List<String> fieldGrouping(Grouping grouping) {
        if (!Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
            throw new IllegalArgumentException("Tried to get grouping fields from non fields grouping");
        }

        return grouping.get_fields();
    }

    public static boolean isGlobalGrouping(Grouping grouping) {
        if (Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
            return fieldGrouping(grouping).isEmpty();
        }

        return false;
    }

    public static int parallelismHint(ComponentCommon component_common) {
        int phint = component_common.get_parallelism_hint();
        if (!component_common.is_set_parallelism_hint()) {
            phint = 1;
        }
        return phint;
    }

    public static StreamInfo directOutputFields(List<String> fields) {
        return new StreamInfo(fields, true);
    }

    public static StreamInfo outputFields(List<String> fields) {
        return new StreamInfo(fields, false);
    }

    public static Grouping mkFieldsGrouping(List<String> fields) {
        return Grouping.fields(fields);
    }

    public static Grouping mkDirectGrouping() {
        return Grouping.direct(new NullStruct());
    }

    public static Grouping mkAllGrouping() {
        return Grouping.all(new NullStruct());
    }

    private static ComponentCommon mkComponentcommon(Map<GlobalStreamId, Grouping> inputs, HashMap<String, StreamInfo> output_spec, Integer parallelism_hint) {
        ComponentCommon ret = new ComponentCommon(inputs, output_spec);
        if (parallelism_hint != null) {
            ret.set_parallelism_hint(parallelism_hint);
        }
        return ret;
    }

    public static Bolt mkBolt(Map<GlobalStreamId, Grouping> inputs, IBolt bolt, HashMap<String, StreamInfo> output, Integer p) {
        ComponentCommon common = mkComponentcommon(inputs, output, p);
        byte[] boltSer = Utils.serialize(bolt);
        ComponentObject component = ComponentObject.serialized_java(boltSer);
        return new Bolt(component, common);
    }

    public static StormTopology._Fields[] STORM_TOPOLOGY_FIELDS = null;
    public static StormTopology._Fields[] SPOUT_FIELDS = { StormTopology._Fields.SPOUTS, StormTopology._Fields.STATE_SPOUTS };
    static {
        Set<_Fields> keys = StormTopology.metaDataMap.keySet();
        STORM_TOPOLOGY_FIELDS = new StormTopology._Fields[keys.size()];
        keys.toArray(STORM_TOPOLOGY_FIELDS);
    }


}

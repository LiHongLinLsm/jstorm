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
package storm.trident.planner.processor;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.tuple.Fields;
import java.util.List;
import storm.trident.planner.TupleReceiver;
import storm.trident.tuple.TridentTuple.Factory;

/**
 * 对应于bolt中单个tridentProcessor的上下文。
 * 一个bolt对应多个processor。
 */
public class TridentContext {
    //该处理节点，新产生的field.
    Fields selfFields;
    List<Factory> parentFactories;
    List<String> parentStreams;
    //表示哪些处理节点将接受该节点的消息。
    List<TupleReceiver> receivers;
    String outStreamId;
    //该处理节点的编号，为subtopBolt中的统一编号。表示该context代表的Node在本bolt中排序索引。
    int stateIndex;
    BatchOutputCollector collector;
    
    public TridentContext(Fields selfFields, List<Factory> parentFactories,
            List<String> parentStreams, List<TupleReceiver> receivers, 
            String outStreamId, int stateIndex, BatchOutputCollector collector) {
        this.selfFields = selfFields;
        this.parentFactories = parentFactories;
        this.parentStreams = parentStreams;
        this.receivers = receivers;
        this.outStreamId = outStreamId;
        this.stateIndex = stateIndex;
        this.collector = collector;
    }

    public List<Factory> getParentTupleFactories() {
        return parentFactories;
    }

    public Fields getSelfOutputFields() {
        return selfFields;
    }

    public List<String> getParentStreams() {
        return parentStreams;
    }

    public List<TupleReceiver> getReceivers() {
        return receivers;
    }

    public String getOutStreamId() {
        return outStreamId;
    }

    public int getStateIndex() {
        return stateIndex;
    }

    // for reporting errors
    public BatchOutputCollector getDelegateCollector() {
        return collector;
    }
}

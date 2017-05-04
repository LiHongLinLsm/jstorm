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
package backtype.storm.coordination;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IComponent;
import backtype.storm.tuple.Tuple;
import java.io.Serializable;
import java.util.Map;

//显著特点是，该bolt必须有个finishBatch方法。。。
public interface IBatchBolt<T> extends Serializable, IComponent {
    /**
     *
     * @param conf
     * @param context
     * @param collector
     * @param id:每个事务对应于一个IBatchBolt。batch处理完后，该bolt被销毁，所以，该T id为transactionAtemp,用来唯一标记。
     */
    void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, T id);

    void execute(Tuple tuple);

    void finishBatch();
}

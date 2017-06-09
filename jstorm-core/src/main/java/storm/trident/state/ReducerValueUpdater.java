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
package storm.trident.state;

import java.util.List;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

//reduce表示本机内部，合并操作
//combiner表示经过reduce和再不同机器见reduce。。
public class ReducerValueUpdater implements ValueUpdater<Object> {
    List<TridentTuple> tuples;
    ReducerAggregator agg;

    public ReducerValueUpdater(ReducerAggregator agg, List<TridentTuple> tuples) {
        this.agg = agg;
        this.tuples = tuples;
    }

    @Override
    public Object update(Object stored) {
        Object ret = (stored == null) ? this.agg.init() : stored;
        for (TridentTuple t : tuples) {
            ret = this.agg.reduce(ret, t);
        }
        return ret;
    }
}

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
package com.alibaba.jstorm.batch.impl;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.ICollectorCallback;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.batch.BatchId;
import com.alibaba.jstorm.batch.util.BatchCommon;
import com.alibaba.jstorm.batch.util.BatchDef;
import com.alibaba.jstorm.batch.util.BatchStatus;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.utils.IntervalCheck;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Strong Sequence
 * 
 * @author zhongyan.feng
 * @version
 */

/**
 * 此类是批处理top中的源头，即真正的spout.
 * 运行流程是：
 * 1.此bolt中缓存maxPending个BatchSpoutMsgId,每个msgID代表一个事务的命令，分为computing,commit，postcommit
 * 等状态，
 * 2.当一个msgid的其中一个状态发送给下游，并得到处理后，被ack,在ack方法中forward状态。
 * 3.这样，事务就成流水线式的得到了计算。
 * 4.当状态是commit，被该spout发送到下游（streamId为COMMINT），被CoordinatedBolt处理时，会将batchId
 *              跟新到zk中。
 */
public class BatchSpoutTrigger implements IRichSpout {
    /**
     *  */
    private static final long serialVersionUID = 7215109169247425954L;

    private static final Logger LOG = LoggerFactory.getLogger(BatchSpoutTrigger.class);

    private LinkedBlockingQueue<BatchSpoutMsgId> batchQueue;

    private transient ClusterState zkClient;

    private transient SpoutOutputCollector collector;

    private static final String ZK_NODE_PATH = "/trigger";

    //最小的batchId.每次初始化时，从zk中读取的数字。然后生成batchId对象。
    private static BatchId currentBatchId = null;

    private Map conf;

    private String taskName;

    private IntervalCheck intervalCheck;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        batchQueue = new LinkedBlockingQueue<BatchSpoutMsgId>();
        this.collector = collector;
        this.conf = conf;
        taskName = context.getThisComponentId() + "_" + context.getThisTaskId();

        intervalCheck = new IntervalCheck();

        try {
            zkClient = BatchCommon.getZkClient(conf);
            initMsgId();
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException("Failed to init");
        }
        LOG.info("Successfully open " + taskName);
    }

    /**
     * @throws Exception
     *  1.此处从zk/trigger目录中读取zkMsgId.然后设置到BatchId的全局staticId中。
     *  2.根据maxpending的数值，初始化队列batchQueue。其中的对象为batchId逐渐增大的MsgID.初始状态都是computting.
     */
    public void initMsgId() throws Exception {
        Long zkMsgId = null;
        byte[] data = zkClient.get_data(ZK_NODE_PATH, false);
        if (data != null) {
            String value = new String(data);
            try {
                zkMsgId = Long.valueOf(value);
                LOG.info("ZK msgId:" + zkMsgId);
            } catch (Exception e) {
                LOG.warn("Failed to get msgId ", e);

            }

        }

        if (zkMsgId != null) {
            BatchId.updateId(zkMsgId);
        }

        int max_spout_pending = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), 1);

        for (int i = 0; i < max_spout_pending; i++) {
            BatchSpoutMsgId msgId = BatchSpoutMsgId.mkInstance();
            if (currentBatchId == null) {
                currentBatchId = msgId.getBatchId();
            }
            batchQueue.offer(msgId);
            LOG.info("Push into queue," + msgId);
        }

    }

    @Override
    public void close() {
        zkClient.close();
    }

    @Override
    public void activate() {
        LOG.info("Activate " + taskName);
    }

    @Override
    public void deactivate() {
        LOG.info("Deactivate " + taskName);
    }

    //根据本地缓存队列中batchSpoutMsgId中id最小的batchID的状态，得到应该发射stream的id。
    protected String getStreamId(BatchStatus batchStatus) {
        if (batchStatus == BatchStatus.COMPUTING) {
            return BatchDef.COMPUTING_STREAM_ID;
        } else if (batchStatus == BatchStatus.PREPARE_COMMIT) {
            return BatchDef.PREPARE_STREAM_ID;
        } else if (batchStatus == BatchStatus.COMMIT) {
            return BatchDef.COMMIT_STREAM_ID;
        } else if (batchStatus == BatchStatus.POST_COMMIT) {
            return BatchDef.POST_STREAM_ID;
        } else if (batchStatus == BatchStatus.REVERT_COMMIT) {
            return BatchDef.REVERT_STREAM_ID;
        } else {
            LOG.error("Occur unkonw type BatchStatus " + batchStatus);
            throw new RuntimeException();
        }
    }

    //4/19下午：暂时的理解是revert_commit为重新提交。提交失败（网络等原因）后，重试。
    protected boolean isCommitStatus(BatchStatus batchStatus) {
        if (batchStatus == BatchStatus.COMMIT) {
            return true;
        } else if (batchStatus == BatchStatus.REVERT_COMMIT) {
            return true;
        } else {
            return false;
        }
    }


    protected boolean isCommitWait(BatchSpoutMsgId msgId) {

        if (isCommitStatus(msgId.getBatchStatus()) == false) {
            return false;
        }

        // left status is commit status
        if (currentBatchId.getId() >= msgId.getBatchId().getId()) {
            return false;
        }

        return true;
    }

    @Override
    public void nextTuple() {
        BatchSpoutMsgId msgId = null;
        try {
            msgId = batchQueue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.error("", e);
        }
        if (msgId == null) {
            return;
        }

        //如果由于网络原因，curId = 1,并且还没得到ack。那么此处取出的id>curId。所以，是等待提交状态。重新放回队列。
        if (isCommitWait(msgId)) {

            batchQueue.offer(msgId);
            //默认一秒钟检查一次。
            if (intervalCheck.check()) {
                LOG.info("Current msgId " + msgId + ", but current commit BatchId is " + currentBatchId);
            } else {
                LOG.debug("Current msgId " + msgId + ", but current commit BatchId is " + currentBatchId);
            }

            return;
        }

        String streamId = getStreamId(msgId.getBatchStatus());
        collector.emit(streamId, new Values(msgId.getBatchId()), msgId, new EmitCb(msgId) );
        
        return;

    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof BatchSpoutMsgId) {
            forward((BatchSpoutMsgId) msgId);
            return;
        } else {
            LOG.warn("Unknown type msgId " + msgId.getClass().getName() + ":" + msgId);
            return;
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof BatchSpoutMsgId) {
            handleFail((BatchSpoutMsgId) msgId);
        } else {
            LOG.warn("Unknown type msgId " + msgId.getClass().getName() + ":" + msgId);
            return;
        }
    }

    protected void handleFail(BatchSpoutMsgId msgId) {
        LOG.info("Failed batch " + msgId);
        BatchStatus status = msgId.getBatchStatus();

        BatchStatus newStatus = status.error();
        if (newStatus == BatchStatus.ERROR) {
            // create new status
            mkMsgId(msgId);

        } else {
            //如果因为重试，那么将重新放入到队列中。
            msgId.setBatchStatus(newStatus);
            batchQueue.offer(msgId);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(BatchDef.COMPUTING_STREAM_ID, new Fields("BatchId"));
        declarer.declareStream(BatchDef.PREPARE_STREAM_ID, new Fields("BatchId"));
        declarer.declareStream(BatchDef.COMMIT_STREAM_ID, new Fields("BatchId"));
        declarer.declareStream(BatchDef.REVERT_STREAM_ID, new Fields("BatchId"));
        declarer.declareStream(BatchDef.POST_STREAM_ID, new Fields("BatchId"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = new HashMap<String, Object>();
        ConfigExtension.setSpoutSingleThread(map, true);
        return map;
    }

    //生成新的msgID，并放入队列中。
    protected void mkMsgId(BatchSpoutMsgId oldMsgId) {
        synchronized (BatchSpoutMsgId.class) {
            if (currentBatchId.getId() <= oldMsgId.getBatchId().getId()) {
                // this is normal case

                byte[] data = String.valueOf(currentBatchId.getId()).getBytes();
                try {
                    zkClient.set_data(ZK_NODE_PATH, data);
                } catch (Exception e) {
                    LOG.error("Failed to update to ZK " + oldMsgId, e);
                }

                currentBatchId = BatchId.incBatchId(oldMsgId.getBatchId());

            } else {
                // bigger batchId has been failed, when old msgId finish
                // it will go here

            }

        }

        BatchSpoutMsgId newMsgId = BatchSpoutMsgId.mkInstance();
        batchQueue.offer(newMsgId);
        StringBuilder sb = new StringBuilder();
        sb.append("Create new BatchId,");
        sb.append("old:").append(oldMsgId);
        sb.append("new:").append(newMsgId);
        sb.append("currentBatchId:").append(currentBatchId);
        LOG.info(sb.toString());
    }


    protected void forward(BatchSpoutMsgId msgId) {
        BatchStatus status = msgId.getBatchStatus();

        BatchStatus newStatus = status.forward();
        if (newStatus == null) {
            // create new status
            mkMsgId(msgId);
            LOG.info("Finish old batch " + msgId);

        } else {
            msgId.setBatchStatus(newStatus);
            batchQueue.offer(msgId);
            LOG.info("Forward batch " + msgId);
        }
    }

    class EmitCb implements ICollectorCallback {

        private BatchSpoutMsgId msgId ;
        public EmitCb(BatchSpoutMsgId msgId) {
            this.msgId = msgId;
        }
       
		@Override
		public void execute(String stream, List<Integer> outTasks, List values) {
            /**
             * 如果下游的bolt为空，那么msgId状态转移。进行下一轮的发射。
             */
			if (outTasks.isEmpty()) {
                forward(msgId);
            }
		}
        
    }
}

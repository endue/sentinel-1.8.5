/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.node.OccupyTimeoutProperty;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.PriorityWaitException;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;
import com.alibaba.csp.sentinel.util.TimeUtil;

/**
 * Default throttling controller (immediately reject strategy).
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 *
 * 默认的流(线程、QPS)控制器实现，基于滑动时间窗口实现
 */
public class DefaultController implements TrafficShapingController {

    private static final int DEFAULT_AVG_USED_TOKENS = 0;

    private double count;
    private int grade;

    public DefaultController(double count, int grade) {
        this.count = count;
        this.grade = grade;
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // 获取节点 node 的平均已使用令牌数
        int curCount = avgUsedTokens(node);
        // 当前已使用的令牌数加上要获取的令牌数是否超过了总令牌数 count
        if (curCount + acquireCount > count) {
            // 存在优先级并且是基于QPS的限流策略
            if (prioritized && grade == RuleConstant.FLOW_GRADE_QPS) {
                long currentTime;
                long waitInMs;
                currentTime = TimeUtil.currentTimeMillis();
                // 计算距离下一个窗口需要等待的时间
                waitInMs = node.tryOccupyNext(currentTime, acquireCount, count);
                // 小于OccupyTimeoutProperty.getOccupyTimeout()代表借到了token
                if (waitInMs < OccupyTimeoutProperty.getOccupyTimeout()) {
                    // 给未来时间窗口增加已占用token
                    node.addWaitingRequest(currentTime + waitInMs, acquireCount);
                    // 统计当前时间窗口占用未来的QPS数
                    node.addOccupiedPass(acquireCount);
                    // 等待时间到达指定的窗口
                    sleep(waitInMs);

                    // 等待时间到达抛出PriorityWaitException，该异常在StatisticSlot中被捕获处理
                    // PriorityWaitException indicates that the request will pass after waiting for {@link @waitInMs}.
                    throw new PriorityWaitException(waitInMs);
                }
            }
            return false;
        }
        // 通过流量控制
        return true;
    }


    /**
     * 获取节点 node 的平均已使用令牌数:线程数 或 QPS
     * @param node
     * @return
     */
    private int avgUsedTokens(Node node) {
        if (node == null) {
            return DEFAULT_AVG_USED_TOKENS;
        }
        return grade == RuleConstant.FLOW_GRADE_THREAD ? node.curThreadNum() : (int)(node.passQps());
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }
}

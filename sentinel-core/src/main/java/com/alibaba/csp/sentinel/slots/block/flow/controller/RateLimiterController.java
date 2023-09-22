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

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleUtil;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;

/**
 * 基于漏桶算法实现的流控器实现
 * 创建的地方 {@link FlowRuleUtil#generateRater(FlowRule)}
 *
 * 匀速流控适合用于请求突发性增长后剧降的场景。例如用在有定时任务调用的接口，在定时任务执行时请求量一下子飙高，
 * 但随后又没有请求的情况，这个时候我们不希望一下子让所有请求都通过，避免把系统压垮，但也不想直接拒绝超出阈值的请求，
 * 这种场景下使用匀速流控可以将突增的请求排队到低峰时执行，起到“削峰填谷”的效果
 *
 * @author jialiang.linjl
 */
public class RateLimiterController implements TrafficShapingController {

    /**
     * 请求在虚拟队列中的最大等待时间，默认 500 毫秒
     */
    private final int maxQueueingTimeMs;

    /**
     * 总的token数量
     */
    private final double count;

    /**
     * 最近一个请求通过的时间，用于计算下一个请求的预期通过时间。
     */
    private final AtomicLong latestPassedTime = new AtomicLong(-1);

    public RateLimiterController(int timeOut, double count) {
        this.maxQueueingTimeMs = timeOut;
        this.count = count;
    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        // Pass when acquire count is less or equal than 0.
        if (acquireCount <= 0) {
            return true;
        }
        // Reject when count is less or equal than 0.
        // Otherwise,the costTime will be max of long and waitTime will overflow in some cases.
        if (count <= 0) {
            return false;
        }

        // 计算队列中连续的两个请求的通过时间的间隔时长
        // 假设： 阈值QPS为1000，那么连续的每个请求的通过时间间隔为1毫秒，每1毫秒通过一个请求就是匀速的速率。
        // 注意这里，当acquireCount超过1000时，也就是QPS限流阈值配置超过1000，会存在限流规则失效的问题
        long currentTime = TimeUtil.currentTimeMillis();
        // Calculate the interval between every two requests.
        long costTime = Math.round(1.0 * (acquireCount) / count * 1000);

        // 计算当前请求的期望通过时间
        // Expected pass time of this request.
        long expectedTime = costTime + latestPassedTime.get();

        // 期望通过时间少于等于当前时间则当前请求可通过并且可以立即通过
        if (expectedTime <= currentTime) {
            // 更新最近一个请求通过的时间
            // 如果多线程的情况下，这里会出现多个请求并行通过。理想情况下是请求固定的不重叠的通过
            // Contention may exist here, but it's okay.
            latestPassedTime.set(currentTime);
            return true;
        } else {
            // 期望通过时间大于当前时间则计算当前请求需要等待的时间
            // Calculate the time to wait.
            long waitTime = costTime + latestPassedTime.get() - TimeUtil.currentTimeMillis();
            // 如果等待时间超过队列允许的最大等待时间，则直接拒绝该请求
            if (waitTime > maxQueueingTimeMs) {
                return false;
            } else {
                // 当前请求更新latestPassedTime为自己的期望通过时间
                long oldTime = latestPassedTime.addAndGet(costTime);
                try {
                    // 更新成功后, 计算当前请求需要等待的时间。因为在更新的时候存在并发冲突
                    waitTime = oldTime - TimeUtil.currentTimeMillis();
                    // 如果等待时间超过队列允许的最大等待时间，则直接拒绝该请求。并将latestPassedTime减去当前线程的等待时间
                    if (waitTime > maxQueueingTimeMs) {
                        latestPassedTime.addAndGet(-costTime);
                        return false;
                    }
                    // 等待时间不超过队列允许的最大等待时间，说明排队有效，睡眠等待
                    // in race condition waitTime may <= 0
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                    }
                    return true;
                } catch (InterruptedException e) {
                }
            }
        }
        return false;
    }

}

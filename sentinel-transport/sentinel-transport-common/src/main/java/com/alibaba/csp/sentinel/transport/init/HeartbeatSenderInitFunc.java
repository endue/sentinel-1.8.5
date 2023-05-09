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
package com.alibaba.csp.sentinel.transport.init;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy;
import java.util.concurrent.TimeUnit;

import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.config.SentinelConfig;
import com.alibaba.csp.sentinel.heartbeat.HeartbeatSenderProvider;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.init.InitOrder;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.transport.HeartbeatSender;
import com.alibaba.csp.sentinel.transport.config.TransportConfig;

/**
 * 心跳发送初始化函数，用于初始化心跳发送器。基于SPI机制，默认在static{@link com/alibaba/csp/sentinel/Env.java:34}代码块中被调用。
 * Global init function for heartbeat sender.
 *
 * @author Eric Zhao
 */
@InitOrder(-1)
public class HeartbeatSenderInitFunc implements InitFunc {

    private ScheduledExecutorService pool = null;

    /**
     * 初始化心跳发送器的调度器，如果没有配置心跳发送器，则不会初始化。
     */
    private void initSchedulerIfNeeded() {
        if (pool == null) {
            pool = new ScheduledThreadPoolExecutor(2,
                new NamedThreadFactory("sentinel-heartbeat-send-task", true),
                new DiscardOldestPolicy());
        }
    }

    @Override
    public void init() {
        HeartbeatSender sender = HeartbeatSenderProvider.getHeartbeatSender();
        if (sender == null) {
            RecordLog.warn("[HeartbeatSenderInitFunc] WARN: No HeartbeatSender loaded");
            return;
        }

        // 初始化心跳发送器的调度器
        initSchedulerIfNeeded();
        // 获取心跳发送器的心跳间隔
        long interval = retrieveInterval(sender);
        // 设置心跳发送器的心跳间隔
        setIntervalIfNotExists(interval);
        // 启动心跳发送器的调度任务
        scheduleHeartbeatTask(sender, interval);
    }

    private boolean isValidHeartbeatInterval(Long interval) {
        return interval != null && interval > 0;
    }

    /**
     * 设置心跳发送器的心跳间隔，如果没有配置，则使用默认的心跳间隔。
     * @param interval
     */
    private void setIntervalIfNotExists(long interval) {
        SentinelConfig.setConfig(TransportConfig.HEARTBEAT_INTERVAL_MS, String.valueOf(interval));
    }

    /**
     * 获取心跳发送器的心跳间隔，如果没有配置，则使用默认的心跳间隔。
     * @param sender
     * @return
     */
    long retrieveInterval(/*@NonNull*/ HeartbeatSender sender) {
        Long intervalInConfig = TransportConfig.getHeartbeatIntervalMs();
        if (isValidHeartbeatInterval(intervalInConfig)) {
            RecordLog.info("[HeartbeatSenderInitFunc] Using heartbeat interval "
                + "in Sentinel config property: " + intervalInConfig);
            return intervalInConfig;
        } else {
            long senderInterval = sender.intervalMs();
            RecordLog.info("[HeartbeatSenderInit] Heartbeat interval not configured in "
                + "config property or invalid, using sender default: " + senderInterval);
            return senderInterval;
        }
    }

    /**
     * 启动心跳发送器的调度任务
     * @param sender
     * @param interval
     */
    private void scheduleHeartbeatTask(/*@NonNull*/ final HeartbeatSender sender, /*@Valid*/ long interval) {
        pool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    sender.sendHeartbeat();
                } catch (Throwable e) {
                    RecordLog.warn("[HeartbeatSender] Send heartbeat error", e);
                }
            }
        }, 5000, interval, TimeUnit.MILLISECONDS);
        RecordLog.info("[HeartbeatSenderInit] HeartbeatSender started: "
            + sender.getClass().getCanonicalName());
    }
}

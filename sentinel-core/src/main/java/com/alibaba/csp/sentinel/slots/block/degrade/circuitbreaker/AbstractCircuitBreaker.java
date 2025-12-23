/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
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
package com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker;

import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.util.function.BiConsumer;

/**
 * @author Eric Zhao
 * @since 1.8.0
 */
public abstract class AbstractCircuitBreaker implements CircuitBreaker {

    protected final DegradeRule rule;
    protected final int recoveryTimeoutMs;

    /**
     * 熔断器事件监听
     * Sentinel 支持注册自定义的事件监听器监听熔断器状态变换事件
     */
    private final EventObserverRegistry observerRegistry;

    protected final AtomicReference<State> currentState = new AtomicReference<>(State.CLOSED);
    protected volatile long nextRetryTimestamp;

    public AbstractCircuitBreaker(DegradeRule rule) {
        this(rule, EventObserverRegistry.getInstance());
    }

    AbstractCircuitBreaker(DegradeRule rule, EventObserverRegistry observerRegistry) {
        AssertUtil.notNull(observerRegistry, "observerRegistry cannot be null");
        if (!DegradeRuleManager.isValidRule(rule)) {
            throw new IllegalArgumentException("Invalid DegradeRule: " + rule);
        }
        this.observerRegistry = observerRegistry;
        this.rule = rule;
        this.recoveryTimeoutMs = rule.getTimeWindow() * 1000;
    }

    @Override
    public DegradeRule getRule() {
        return rule;
    }

    @Override
    public State currentState() {
        return currentState.get();
    }

    @Override
    public boolean tryPass(Context context) {
        // Template implementation.
        // 熔断关闭，请求通过
        if (currentState.get() == State.CLOSED) {
            return true;
        }
        // 熔断开启，判断是否可以通过
        if (currentState.get() == State.OPEN) {
            // For half-open state we allow a request for probing.
            // 如果熔断时间已到并且修改熔断状态为半开，则将请求通过
            return retryTimeoutArrived() && fromOpenToHalfOpen(context);
        }
        // 否则不允许通过
        return false;
    }

    /**
     * Reset the statistic data.
     */
    abstract void resetStat();

    protected boolean retryTimeoutArrived() {
        return TimeUtil.currentTimeMillis() >= nextRetryTimestamp;
    }

    protected void updateNextRetryTimestamp() {
        this.nextRetryTimestamp = TimeUtil.currentTimeMillis() + recoveryTimeoutMs;
    }

    protected boolean fromCloseToOpen(double snapshotValue) {
        State prev = State.CLOSED;
        if (currentState.compareAndSet(prev, State.OPEN)) {
            updateNextRetryTimestamp();

            notifyObservers(prev, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }

    // 真的whenTerminate方钩子的说明，在默认顺序下：
    //  FlowSlot -> DegradeSlot
    //场景 A：FlowSlot 拦截
    //  请求进入 FlowSlot -> 抛出 FlowException。
    //  请求结束。DegradeSlot 的 entry 方法从未被调用。熔断器状态不会改变。此时不需要 whenTerminate 回调来处理，因为探测根本没开始。
    //场景 B：FlowSlot 通过，DegradeSlot 放行 (Half-Open)
    //  请求进入 FlowSlot (Pass) -> DegradeSlot (Half-Open Pass) -> 执行业务。
    //  如果业务执行期间抛出异常或超时，DegradeSlot.exit 会处理。
    //  那么，whenTerminate 到底防的是谁？
    //  如果 DegradeSlot 是链中的最后一个校验 Slot，那么它后面就只有业务逻辑了。如果业务逻辑被执行了，那么 DegradeSlot 就可以通过 exit 方法（或者 whenTerminate 捕获业务异常）来判断结果。
    //  但是，ProcessorSlotChain 是一个链表。如果 DegradeSlot 后面还有自定义的 Slot 呢？
    //  或者，如果用户通过 SPI 调整了顺序，把 DegradeSlot 放到了 FlowSlot 前面呢？
    //  Sentinel 作为一个通用框架，必须考虑到 Slot 顺序被调整的可能性。
    //2. 自定义顺序或后续 Slot (DegradeSlot 在前)
    //  假设用户配置了：
    //  DegradeSlot -> FlowSlot
    //  或者：
    //  DegradeSlot -> CustomAuthSlot
    //  在这种情况下：
    //  DegradeSlot: 熔断器处于 Open 状态，时间窗口到了，允许一个请求通过 (Half-Open)。
    //  FlowSlot / CustomSlot: 检查发现限流了/权限不足，抛出 BlockException。
    //  请求终止。
    //  此时如果不做处理：
    //  熔断器认为“我已经放行了一个探测请求”，于是它切换到了 Half-Open 状态，等待这个请求的反馈（成功或失败）。
    //  但是，这个请求死在了半路上（被后续 Slot 杀了），它永远不会执行真正的业务逻辑，也就无法验证下游服务是否恢复。
    //  如果没有 whenTerminate 的兜底机制，熔断器可能会错误地认为探测请求还在运行，或者在某些实现下卡在 Half-Open。
    //  通过 whenTerminate：
    //  当 BlockException 抛出并向上传播时，DegradeSlot 注册的钩子会被触发。它检查 entry.getBlockError()，发现请求被 Block 了（非业务异常），于是它知道“探测失效”，果断将状态回滚到 Open，等待下一次机会。
    protected boolean fromOpenToHalfOpen(Context context) {
        if (currentState.compareAndSet(State.OPEN, State.HALF_OPEN)) {
            // 触发熔断器事件监听
            notifyObservers(State.OPEN, State.HALF_OPEN, null);
            // 注册Entry exis回调，如果有BlockException则将熔断状态再次开启
            Entry entry = context.getCurEntry();
            // 注册完，在这里使用{@link com.alibaba.csp.sentinel.CtEntry.callExitHandlersAndCleanUp}
            entry.whenTerminate(new BiConsumer<Context, Entry>() {
                @Override
                public void accept(Context context, Entry entry) {
                    // Note: This works as a temporary workaround for https://github.com/alibaba/Sentinel/issues/1638
                    // Without the hook, the circuit breaker won't recover from half-open state in some circumstances
                    // when the request is actually blocked by upcoming rules (not only degrade rules).
                    if (entry.getBlockError() != null) {
                        // Fallback to OPEN due to detecting request is blocked
                        currentState.compareAndSet(State.HALF_OPEN, State.OPEN);
                        // 触发熔断器事件监听
                        notifyObservers(State.HALF_OPEN, State.OPEN, 1.0d);
                    }
                }
            });
            return true;
        }
        return false;
    }
    
    private void notifyObservers(CircuitBreaker.State prevState, CircuitBreaker.State newState, Double snapshotValue) {
        for (CircuitBreakerStateChangeObserver observer : observerRegistry.getStateChangeObservers()) {
            observer.onStateChange(prevState, newState, rule, snapshotValue);
        }
    }

    protected boolean fromHalfOpenToOpen(double snapshotValue) {
        if (currentState.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            updateNextRetryTimestamp();
            notifyObservers(State.HALF_OPEN, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }

    protected boolean fromHalfOpenToClose() {
        if (currentState.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            resetStat();
            notifyObservers(State.HALF_OPEN, State.CLOSED, null);
            return true;
        }
        return false;
    }

    protected void transformToOpen(double triggerValue) {
        State cs = currentState.get();
        switch (cs) {
            case CLOSED:
                fromCloseToOpen(triggerValue);
                break;
            case HALF_OPEN:
                fromHalfOpenToOpen(triggerValue);
                break;
            default:
                break;
        }
    }
}

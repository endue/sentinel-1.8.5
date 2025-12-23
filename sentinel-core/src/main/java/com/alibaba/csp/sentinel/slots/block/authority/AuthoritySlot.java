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
package com.alibaba.csp.sentinel.slots.block.authority;

import java.util.Map;
import java.util.Set;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.slotchain.AbstractLinkedProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.spi.Spi;

/**
 * A {@link ProcessorSlot} that dedicates to {@link AuthorityRule} checking.
 *
 * @author leyou
 * @author Eric Zhao
 * 相同Resource共享一个AuthoritySlot
 * 1. 如何判断资源相同，可通过{@link ResourceWrapper#equals(Object)}来判断
 * 2. AuthoritySlot是访问控制规则（黑白名单），根据配置的黑白名单规则，决定当前请求的来源（Origin）是否允许通过。若配置白名单则只有请求来源位于白名单内时才可通过；若配置黑名单则请求来源位于黑名单时不通过，其余的请求通过。
 * 举例：
 * 假设你是一个核心的 "库存服务" (inventory-service)。为了安全和数据一致性，只有 "订单服务" (order-service) 和 "后台管理服务" (admin-service) 允许调用我的 deductStock（扣减库存）接口。其他服务（如“推荐服务”）或者外部未知的调用一律拒绝。
 * AuthorityRule rule = new AuthorityRule();
 * rule.setResource("deductStock");
 * rule.setStrategy(RuleConstant.AUTHORITY_WHITE); // 白名单模式
 * rule.setLimitApp("order-service,admin-service"); // 允许这两个 App
 * AuthorityRuleManager.loadRules(Collections.singletonList(rule));
 */
@Spi(order = Constants.ORDER_AUTHORITY_SLOT)
public class AuthoritySlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count, boolean prioritized, Object... args)
        throws Throwable {
        checkBlackWhiteAuthority(resourceWrapper, context);
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }

    void checkBlackWhiteAuthority(ResourceWrapper resource, Context context) throws AuthorityException {
        Map<String, Set<AuthorityRule>> authorityRules = AuthorityRuleManager.getAuthorityRules();

        if (authorityRules == null) {
            return;
        }

        // 获取资源对应的来源访问控制（黑白名单）规则
        Set<AuthorityRule> rules = authorityRules.get(resource.getName());

        // 如果没有配置授权规则 -> 直接放行。
        if (rules == null) {
            return;
        }

        // 校验规则
        for (AuthorityRule rule : rules) {
            if (!AuthorityRuleChecker.passCheck(rule, context)) {
                throw new AuthorityException(context.getOrigin(), rule);
            }
        }
    }
}

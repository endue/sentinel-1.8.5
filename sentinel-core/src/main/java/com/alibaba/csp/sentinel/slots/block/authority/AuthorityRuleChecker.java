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

import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.util.StringUtil;

/**
 * Rule checker for white/black list authority.
 *
 * @author Eric Zhao
 * @since 0.2.0
 *
 * 来源访问控制（黑白名单）规则校验器
 */
final class AuthorityRuleChecker {

    static boolean passCheck(AuthorityRule rule, Context context) {
        // 1. 获取当前context中origin，默认为""
        String requester = context.getOrigin();

        // Empty origin or empty limitApp will pass.
        if (StringUtil.isEmpty(requester) || StringUtil.isEmpty(rule.getLimitApp())) {
            return true;
        }

        // Do exact match with origin name.
        // 2. 检查规则中的limitApp是否包含context中的origin
        int pos = rule.getLimitApp().indexOf(requester);
        boolean contain = pos > -1;

        // 2.1 当前规则包含context中的origin
        if (contain) {
            // 是否精确匹配
            boolean exactlyMatch = false;
            String[] appArray = rule.getLimitApp().split(",");
            for (String app : appArray) {
                if (requester.equals(app)) {
                    exactlyMatch = true;
                    break;
                }
            }

            contain = exactlyMatch;
        }

        int strategy = rule.getStrategy();
        // 3. 规则策略为黑名单，并且包含当前context中的origin，返回false将抛出AuthorityException
        if (strategy == RuleConstant.AUTHORITY_BLACK && contain) {
            return false;
        }

        // 4. 规则策略为白名单，并且不包含当前context中的origin，返回false将抛出AuthorityException
        if (strategy == RuleConstant.AUTHORITY_WHITE && !contain) {
            return false;
        }

        // 5. 黑名单中不包含、白名单中包含两种情况返回true，请求通过
        return true;
    }

    private AuthorityRuleChecker() {}
}

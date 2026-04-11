/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <vector>

#include "common/common.h"
#include "defs.h"
#include "errors.h"
#include "index/ix_index_handle.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

inline std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
    auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
        return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (pos == rec_cols.end()) {
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return pos;
}

inline bool evaluate_condition(const std::vector<ColMeta> &cols, RmRecord *rec, Condition &cond) {
    auto lhs_col = get_col(cols, cond.lhs_col);
    char *lhs_data = rec->data + lhs_col->offset;

    char *rhs_data = nullptr;
    if (cond.is_rhs_val) {
        auto &val = cond.rhs_val;
        if (!val.raw) {
            val.init_raw(lhs_col->len);
        }
        rhs_data = val.raw->data;
    } else {
        auto rhs_col = get_col(cols, cond.rhs_col);
        rhs_data = rec->data + rhs_col->offset;
    }

    int cmp = ix_compare(lhs_data, rhs_data, lhs_col->type, lhs_col->len);
    switch (cond.op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
    }
    return false;
}

inline bool evaluate_conditions(const std::vector<ColMeta> &cols, RmRecord *rec, std::vector<Condition> &conds) {
    for (auto &cond : conds) {
        if (!evaluate_condition(cols, rec, cond)) {
            return false;
        }
    }
    return true;
}

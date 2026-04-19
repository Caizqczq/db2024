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
#include <cstring>
#include <vector>

#include "defs.h"
#include "errors.h"
#include "common/common.h"
#include "system/sm_meta.h"

inline const ColMeta *find_col_meta(const std::vector<ColMeta> &cols, const TabCol &target) {
    auto it = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
        return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (it == cols.end()) {
        throw ColumnNotFoundError(target.tab_name + "." + target.col_name);
    }
    return &(*it);
}

inline int compare_raw_value(const char *lhs, const char *rhs, ColType type, int len) {
    switch (type) {
        case TYPE_INT: {
            int lv = *reinterpret_cast<const int *>(lhs);
            int rv = *reinterpret_cast<const int *>(rhs);
            if (lv < rv) {
                return -1;
            }
            if (lv > rv) {
                return 1;
            }
            return 0;
        }
        case TYPE_FLOAT: {
            float lv = *reinterpret_cast<const float *>(lhs);
            float rv = *reinterpret_cast<const float *>(rhs);
            if (lv < rv) {
                return -1;
            }
            if (lv > rv) {
                return 1;
            }
            return 0;
        }
        case TYPE_STRING:
            return std::memcmp(lhs, rhs, len);
        default:
            throw InternalError("Unexpected column type");
    }
}

inline bool eval_compare_result(int cmp_res, CompOp op) {
    switch (op) {
        case OP_EQ:
            return cmp_res == 0;
        case OP_NE:
            return cmp_res != 0;
        case OP_LT:
            return cmp_res < 0;
        case OP_GT:
            return cmp_res > 0;
        case OP_LE:
            return cmp_res <= 0;
        case OP_GE:
            return cmp_res >= 0;
        default:
            throw InternalError("Unexpected compare operator");
    }
}

inline bool eval_condition(const Condition &cond, const std::vector<ColMeta> &cols, const char *rec_buf) {
    auto lhs_col = find_col_meta(cols, cond.lhs_col);
    const char *lhs_data = rec_buf + lhs_col->offset;

    const char *rhs_data = nullptr;
    ColType rhs_type;
    int rhs_len;

    if (cond.is_rhs_val) {
        if (cond.rhs_val.raw == nullptr) {
            throw InternalError("RHS value is not initialized");
        }
        rhs_data = cond.rhs_val.raw->data;
        rhs_type = cond.rhs_val.type;
        rhs_len = lhs_col->len;
    } else {
        auto rhs_col = find_col_meta(cols, cond.rhs_col);
        rhs_data = rec_buf + rhs_col->offset;
        rhs_type = rhs_col->type;
        rhs_len = rhs_col->len;
    }

    if (lhs_col->type != rhs_type) {
        throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(rhs_type));
    }

    int cmp_res = compare_raw_value(lhs_data, rhs_data, lhs_col->type, std::min(lhs_col->len, rhs_len));
    return eval_compare_result(cmp_res, cond.op);
}

inline bool eval_conditions(const std::vector<Condition> &conds, const std::vector<ColMeta> &cols, const char *rec_buf) {
    for (const auto &cond : conds) {
        if (!eval_condition(cond, cols, rec_buf)) {
            return false;
        }
    }
    return true;
}

inline void make_index_key(const std::vector<ColMeta> &index_cols, const char *rec_buf, char *key_buf) {
    int key_offset = 0;
    for (const auto &col : index_cols) {
        std::memcpy(key_buf + key_offset, rec_buf + col.offset, col.len);
        key_offset += col.len;
    }
}

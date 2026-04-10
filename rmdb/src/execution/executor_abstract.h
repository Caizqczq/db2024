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
#include <string>
#include <vector>

#include "execution_defs.h"
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

   protected:
    static int compare_value(const char *lhs, const char *rhs, ColType type, int len) {
        switch (type) {
            case TYPE_INT: {
                int lhs_v = *reinterpret_cast<const int *>(lhs);
                int rhs_v = *reinterpret_cast<const int *>(rhs);
                return (lhs_v < rhs_v) ? -1 : ((lhs_v > rhs_v) ? 1 : 0);
            }
            case TYPE_FLOAT: {
                float lhs_v = *reinterpret_cast<const float *>(lhs);
                float rhs_v = *reinterpret_cast<const float *>(rhs);
                return (lhs_v < rhs_v) ? -1 : ((lhs_v > rhs_v) ? 1 : 0);
            }
            case TYPE_STRING:
                return std::memcmp(lhs, rhs, len);
            default:
                throw InternalError("Unexpected data type in comparison");
        }
    }

    static bool eval_cmp(int cmp_res, CompOp op) {
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
                throw InternalError("Unexpected comparison operator");
        }
    }

    bool eval_cond(const Condition &cond, const std::vector<ColMeta> &rec_cols, const char *rec_buf) {
        auto lhs_pos = get_col(rec_cols, cond.lhs_col);
        const ColMeta &lhs_col = *lhs_pos;
        const char *lhs = rec_buf + lhs_col.offset;

        const char *rhs = nullptr;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            if (cond.rhs_val.raw == nullptr) {
                throw InternalError("RHS value is not initialized");
            }
            rhs = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
        } else {
            auto rhs_pos = get_col(rec_cols, cond.rhs_col);
            rhs = rec_buf + rhs_pos->offset;
            rhs_type = rhs_pos->type;
        }
        if (lhs_col.type != rhs_type) {
            bool lhs_numeric = lhs_col.type == TYPE_INT || lhs_col.type == TYPE_FLOAT;
            bool rhs_numeric = rhs_type == TYPE_INT || rhs_type == TYPE_FLOAT;
            if (!(lhs_numeric && rhs_numeric)) {
                throw IncompatibleTypeError(coltype2str(lhs_col.type), coltype2str(rhs_type));
            }

            float lhs_num = lhs_col.type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int *>(lhs))
                                                     : *reinterpret_cast<const float *>(lhs);
            float rhs_num = rhs_type == TYPE_INT ? static_cast<float>(*reinterpret_cast<const int *>(rhs))
                                                 : *reinterpret_cast<const float *>(rhs);
            int cmp_res = (lhs_num < rhs_num) ? -1 : ((lhs_num > rhs_num) ? 1 : 0);
            return eval_cmp(cmp_res, cond.op);
        }

        int cmp_res = compare_value(lhs, rhs, lhs_col.type, lhs_col.len);
        return eval_cmp(cmp_res, cond.op);
    }

    bool eval_conds(const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols, const char *rec_buf) {
        for (auto &cond : conds) {
            if (!eval_cond(cond, rec_cols, rec_buf)) {
                return false;
            }
        }
        return true;
    }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
};

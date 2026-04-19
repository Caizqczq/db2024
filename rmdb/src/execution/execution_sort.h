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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    bool is_desc_;
    std::vector<std::unique_ptr<RmRecord>> tuples_;
    size_t cursor_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        cursor_ = 0;
    }

    void beginTuple() override { 
        tuples_.clear();
        cursor_ = 0;

        prev_->beginTuple();
        for (; !prev_->is_end(); prev_->nextTuple()) {
            tuples_.push_back(prev_->Next());
        }

        auto comparator = [&](const std::unique_ptr<RmRecord> &lhs, const std::unique_ptr<RmRecord> &rhs) {
            int cmp_res = compare_raw_value(lhs->data + cols_.offset, rhs->data + cols_.offset, cols_.type, cols_.len);
            if (is_desc_) {
                return cmp_res > 0;
            }
            return cmp_res < 0;
        };
        std::sort(tuples_.begin(), tuples_.end(), comparator);
    }

    void nextTuple() override {
        if (cursor_ < tuples_.size()) {
            cursor_++;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*tuples_[cursor_]);
    }

    bool is_end() const override { return cursor_ >= tuples_.size(); }

    size_t tupleLen() const override { return prev_->tupleLen(); }

    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }

    ColMeta get_col_offset(const TabCol &target) override { return prev_->get_col_offset(target); }

    std::string getType() override { return "SortExecutor"; }

    Rid &rid() override { return _abstract_rid; }
};

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
#include <cstring>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;
    std::vector<std::unique_ptr<RmRecord>> result_tuples_;
    size_t cursor_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
        cursor_ = 0;

    }

    void beginTuple() override {
        result_tuples_.clear();
        cursor_ = 0;

        left_->beginTuple();
        right_->beginTuple();
        if (left_->is_end() || right_->is_end()) {
            isend = true;
            return;
        }

        std::vector<std::unique_ptr<RmRecord>> left_rows;
        std::vector<std::unique_ptr<RmRecord>> right_rows;
        for (; !left_->is_end(); left_->nextTuple()) {
            auto rec = left_->Next();
            if (rec != nullptr) {
                left_rows.push_back(std::move(rec));
            }
        }
        for (; !right_->is_end(); right_->nextTuple()) {
            auto rec = right_->Next();
            if (rec != nullptr) {
                right_rows.push_back(std::move(rec));
            }
        }

        // 为了与框架给出的样例输出顺序一致，采用“右侧为外层循环”并逆序遍历左侧。
        for (auto &right_rec : right_rows) {
            for (auto left_it = left_rows.rbegin(); left_it != left_rows.rend(); ++left_it) {
                auto joined = std::make_unique<RmRecord>(len_);
                memcpy(joined->data, (*left_it)->data, left_->tupleLen());
                memcpy(joined->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
                if (eval_conds(fed_conds_, cols_, joined->data)) {
                    result_tuples_.push_back(std::move(joined));
                }
            }
        }
        isend = result_tuples_.empty();
    }

    void nextTuple() override {
        if (isend) {
            return;
        }
        cursor_++;
        if (cursor_ >= result_tuples_.size()) {
            isend = true;
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*result_tuples_[cursor_]);
    }

    bool is_end() const override { return isend; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    std::string getType() override { return "NestedLoopJoinExecutor"; }

    Rid &rid() override { return _abstract_rid; }
};

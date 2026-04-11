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

#include <limits>
#include <map>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据
    std::vector<IndexRange> index_ranges_;

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;
    Iid upper_iid_{-1, -1};

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, std::vector<IndexRange> index_ranges, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        index_ranges_ = std::move(index_ranges);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols);
        auto *ih = sm_manager_->ihs_.at(index_name).get();

        if (has_empty_range()) {
            auto end = ih->leaf_end();
            scan_ = std::make_unique<IxScan>(ih, end, end, sm_manager_->get_bpm());
            return;
        }

        auto lower_key = std::make_unique<char[]>(index_meta_.col_tot_len);
        auto upper_key = std::make_unique<char[]>(index_meta_.col_tot_len);
        build_bound_key(lower_key.get(), true);
        build_bound_key(upper_key.get(), false);

        bool lower_inclusive = true;
        bool upper_inclusive = true;
        bool has_lower = false;
        bool has_upper = false;
        for (const auto &range : index_ranges_) {
            if (range.has_lower) {
                has_lower = true;
                lower_inclusive = range.lower_inclusive;
            }
            if (range.has_upper) {
                has_upper = true;
                upper_inclusive = range.upper_inclusive;
            }
            if (!range.has_equality) {
                break;
            }
        }

        Iid lower_iid = has_lower ? (lower_inclusive ? ih->lower_bound(lower_key.get()) : ih->upper_bound(lower_key.get()))
                                  : ih->leaf_begin();
        upper_iid_ = has_upper ? (upper_inclusive ? ih->upper_bound(upper_key.get()) : ih->lower_bound(upper_key.get()))
                               : ih->leaf_end();
        if (iid_less(upper_iid_, lower_iid)) {
            scan_ = std::make_unique<IxScan>(ih, upper_iid_, upper_iid_, sm_manager_->get_bpm());
            return;
        }

        scan_ = std::make_unique<IxScan>(ih, lower_iid, upper_iid_, sm_manager_->get_bpm());
        find_next_valid();
    }

    void nextTuple() override {
        if (is_end()) {
            return;
        }
        scan_->next();
        find_next_valid();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    std::string getType() override { return "IndexScanExecutor"; }

    Rid &rid() override { return rid_; }

   private:
    void set_min_value(char *dest, ColType type, int len) {
        if (type == TYPE_INT) {
            int value = std::numeric_limits<int>::min();
            memcpy(dest, &value, sizeof(int));
        } else if (type == TYPE_FLOAT) {
            float value = -std::numeric_limits<float>::max();
            memcpy(dest, &value, sizeof(float));
        } else {
            memset(dest, 0, len);
        }
    }

    void set_max_value(char *dest, ColType type, int len) {
        if (type == TYPE_INT) {
            int value = std::numeric_limits<int>::max();
            memcpy(dest, &value, sizeof(int));
        } else if (type == TYPE_FLOAT) {
            float value = std::numeric_limits<float>::max();
            memcpy(dest, &value, sizeof(float));
        } else {
            memset(dest, 0xff, len);
        }
    }

    void build_bound_key(char *key, bool lower) {
        int offset = 0;
        for (size_t i = 0; i < index_meta_.cols.size(); ++i) {
            const auto &col = index_meta_.cols[i];
            if (i < index_ranges_.size()) {
                const auto &range = index_ranges_[i];
                if (range.has_equality) {
                    auto value = range.lower_value;
                    if (!value.raw) {
                        value.init_raw(col.len);
                    }
                    memcpy(key + offset, value.raw->data, col.len);
                } else if (lower && range.has_lower) {
                    auto value = range.lower_value;
                    if (!value.raw) {
                        value.init_raw(col.len);
                    }
                    memcpy(key + offset, value.raw->data, col.len);
                    offset += col.len;
                    for (size_t j = i + 1; j < index_meta_.cols.size(); ++j) {
                        set_min_value(key + offset, index_meta_.cols[j].type, index_meta_.cols[j].len);
                        offset += index_meta_.cols[j].len;
                    }
                    return;
                } else if (!lower && range.has_upper) {
                    auto value = range.upper_value;
                    if (!value.raw) {
                        value.init_raw(col.len);
                    }
                    memcpy(key + offset, value.raw->data, col.len);
                    offset += col.len;
                    for (size_t j = i + 1; j < index_meta_.cols.size(); ++j) {
                        set_max_value(key + offset, index_meta_.cols[j].type, index_meta_.cols[j].len);
                        offset += index_meta_.cols[j].len;
                    }
                    return;
                } else {
                    if (lower) {
                        set_min_value(key + offset, col.type, col.len);
                    } else {
                        set_max_value(key + offset, col.type, col.len);
                    }
                }
            } else {
                if (lower) {
                    set_min_value(key + offset, col.type, col.len);
                } else {
                    set_max_value(key + offset, col.type, col.len);
                }
            }
            offset += col.len;
        }
    }

    void find_next_valid() {
        while (scan_ && !scan_->is_end()) {
            Rid curr_rid = scan_->rid();
            auto rec = fh_->get_record(curr_rid, context_);
            if (rec != nullptr && evaluate_conditions(cols_, rec.get(), fed_conds_)) {
                rid_ = curr_rid;
                return;
            }
            scan_->next();
        }
    }

    static bool iid_less(const Iid &lhs, const Iid &rhs) {
        if (lhs.page_no != rhs.page_no) {
            return lhs.page_no < rhs.page_no;
        }
        return lhs.slot_no < rhs.slot_no;
    }

    int compare_range_values(const Value &lhs, const Value &rhs, const ColMeta &col) const {
        auto lhs_copy = lhs;
        auto rhs_copy = rhs;
        if (!lhs_copy.raw) {
            lhs_copy.init_raw(col.len);
        }
        if (!rhs_copy.raw) {
            rhs_copy.init_raw(col.len);
        }
        return ix_compare(lhs_copy.raw->data, rhs_copy.raw->data, col.type, col.len);
    }

    bool has_empty_range() const {
        for (size_t i = 0; i < index_ranges_.size() && i < index_meta_.cols.size(); ++i) {
            const auto &range = index_ranges_[i];
            if (!range.has_lower || !range.has_upper) {
                continue;
            }
            int cmp = compare_range_values(range.lower_value, range.upper_value, index_meta_.cols[i]);
            if (cmp > 0) {
                return true;
            }
            if (cmp == 0 && (!range.lower_inclusive || !range.upper_inclusive)) {
                return true;
            }
        }
        return false;
    }
};

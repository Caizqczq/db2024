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
#include <limits>
#include <map>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    struct BoundCond {
        bool has_eq = false;
        const Value *eq_val = nullptr;

        bool has_lower = false;
        const Value *lower_val = nullptr;
        bool lower_inclusive = true;

        bool has_upper = false;
        const Value *upper_val = nullptr;
        bool upper_inclusive = true;
    };

    std::string tab_name_;
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<Condition> fed_conds_;

    std::vector<std::string> index_col_names_;
    IndexMeta index_meta_;

    Rid rid_;
    std::vector<Rid> matched_rids_;
    size_t rid_pos_{0};

    SmManager *sm_manager_;

   private:
    static void fill_min_component(char *buf, const ColMeta &col) {
        if (col.type == TYPE_INT) {
            int v = std::numeric_limits<int>::lowest();
            std::memcpy(buf, &v, sizeof(int));
        } else if (col.type == TYPE_FLOAT) {
            float v = -std::numeric_limits<float>::infinity();
            std::memcpy(buf, &v, sizeof(float));
        } else {
            std::memset(buf, 0, col.len);
        }
    }

    static void fill_max_component(char *buf, const ColMeta &col) {
        if (col.type == TYPE_INT) {
            int v = std::numeric_limits<int>::max();
            std::memcpy(buf, &v, sizeof(int));
        } else if (col.type == TYPE_FLOAT) {
            float v = std::numeric_limits<float>::infinity();
            std::memcpy(buf, &v, sizeof(float));
        } else {
            std::memset(buf, 0xFF, col.len);
        }
    }

    static bool is_stronger_lower(const Value *cand_val, bool cand_inclusive, const Value *curr_val,
                                  bool curr_inclusive, const ColMeta &col) {
        int cmp = compare_raw_value(cand_val->raw->data, curr_val->raw->data, col.type, col.len);
        if (cmp > 0) {
            return true;
        }
        if (cmp < 0) {
            return false;
        }
        return !cand_inclusive && curr_inclusive;
    }

    static bool is_stronger_upper(const Value *cand_val, bool cand_inclusive, const Value *curr_val,
                                  bool curr_inclusive, const ColMeta &col) {
        int cmp = compare_raw_value(cand_val->raw->data, curr_val->raw->data, col.type, col.len);
        if (cmp < 0) {
            return true;
        }
        if (cmp > 0) {
            return false;
        }
        return !cand_inclusive && curr_inclusive;
    }

    bool collect_bound_for_col(const ColMeta &col, BoundCond &bound, bool &impossible) const {
        bool has_cond = false;
        for (auto &cond : fed_conds_) {
            if (!cond.is_rhs_val) {
                continue;
            }
            if (cond.lhs_col.tab_name != tab_name_ || cond.lhs_col.col_name != col.name) {
                continue;
            }
            const Value *rhs = &cond.rhs_val;
            switch (cond.op) {
                case OP_EQ: {
                    has_cond = true;
                    if (!bound.has_eq) {
                        bound.has_eq = true;
                        bound.eq_val = rhs;
                    } else if (compare_raw_value(rhs->raw->data, bound.eq_val->raw->data, col.type, col.len) != 0) {
                        impossible = true;
                    }
                    break;
                }
                case OP_GT:
                case OP_GE: {
                    has_cond = true;
                    bool inclusive = (cond.op == OP_GE);
                    if (!bound.has_lower ||
                        is_stronger_lower(rhs, inclusive, bound.lower_val, bound.lower_inclusive, col)) {
                        bound.has_lower = true;
                        bound.lower_val = rhs;
                        bound.lower_inclusive = inclusive;
                    }
                    break;
                }
                case OP_LT:
                case OP_LE: {
                    has_cond = true;
                    bool inclusive = (cond.op == OP_LE);
                    if (!bound.has_upper ||
                        is_stronger_upper(rhs, inclusive, bound.upper_val, bound.upper_inclusive, col)) {
                        bound.has_upper = true;
                        bound.upper_val = rhs;
                        bound.upper_inclusive = inclusive;
                    }
                    break;
                }
                case OP_NE:
                    break;
                default:
                    break;
            }
        }

        if (bound.has_eq) {
            bound.has_lower = true;
            bound.lower_val = bound.eq_val;
            bound.lower_inclusive = true;
            bound.has_upper = true;
            bound.upper_val = bound.eq_val;
            bound.upper_inclusive = true;
        }

        if (bound.has_lower && bound.has_upper) {
            int cmp = compare_raw_value(bound.lower_val->raw->data, bound.upper_val->raw->data, col.type, col.len);
            if (cmp > 0 || (cmp == 0 && (!bound.lower_inclusive || !bound.upper_inclusive))) {
                impossible = true;
            }
        }
        return has_cond;
    }

    bool build_scan_range(const IxIndexHandle *ih, Iid &lower_iid, Iid &upper_iid) const {
        std::vector<char> lower_key(index_meta_.col_tot_len, 0);
        std::vector<char> upper_key(index_meta_.col_tot_len, 0);
        bool lower_inclusive = true;
        bool upper_inclusive = true;
        int offset = 0;
        int prefix_len = 0;
        bool stop = false;
        bool impossible = false;

        for (auto &col : index_meta_.cols) {
            BoundCond bound;
            bool has_cond = collect_bound_for_col(col, bound, impossible);
            if (impossible) {
                lower_iid = Iid{0, 0};
                upper_iid = Iid{0, 0};
                return true;
            }

            if (bound.has_eq) {
                std::memcpy(lower_key.data() + offset, bound.eq_val->raw->data, col.len);
                std::memcpy(upper_key.data() + offset, bound.eq_val->raw->data, col.len);
                offset += col.len;
                prefix_len++;
                continue;
            }

            if (has_cond) {
                if (bound.has_lower) {
                    std::memcpy(lower_key.data() + offset, bound.lower_val->raw->data, col.len);
                } else {
                    fill_min_component(lower_key.data() + offset, col);
                }
                if (bound.has_upper) {
                    std::memcpy(upper_key.data() + offset, bound.upper_val->raw->data, col.len);
                } else {
                    fill_max_component(upper_key.data() + offset, col);
                }
                lower_inclusive = bound.has_lower ? bound.lower_inclusive : true;
                upper_inclusive = bound.has_upper ? bound.upper_inclusive : true;
                offset += col.len;
                prefix_len++;
                stop = true;
                break;
            }

            if (prefix_len == 0) {
                return false;
            }
            break;
        }

        if (!stop && prefix_len == 0) {
            return false;
        }

        int fill_offset = 0;
        for (size_t col_idx = 0; col_idx < index_meta_.cols.size(); ++col_idx) {
            auto &col = index_meta_.cols[col_idx];
            if (col_idx < static_cast<size_t>(prefix_len)) {
                fill_offset += col.len;
                continue;
            }
            fill_min_component(lower_key.data() + fill_offset, col);
            fill_max_component(upper_key.data() + fill_offset, col);
            fill_offset += col.len;
        }

        lower_iid = lower_inclusive ? ih->lower_bound(lower_key.data()) : ih->upper_bound(lower_key.data());
        upper_iid = upper_inclusive ? ih->upper_bound(upper_key.data()) : ih->lower_bound(upper_key.data());
        return true;
    }

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = std::move(index_col_names);
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        matched_rids_.clear();
        rid_pos_ = 0;

        IxIndexHandle *ih = sm_manager_->open_index_handle(tab_name_, index_meta_.cols);
        Iid lower_iid{};
        Iid upper_iid{};
        bool has_range = build_scan_range(ih, lower_iid, upper_iid);
        if (!has_range) {
            lower_iid = ih->leaf_begin();
            upper_iid = ih->leaf_end();
        }

        for (int slot = lower_iid.slot_no; slot < upper_iid.slot_no; ++slot) {
            Iid iid{0, slot};
            Rid curr_rid = ih->get_rid(iid);
            auto rec = fh_->get_record(curr_rid, context_);
            if (eval_conditions(fed_conds_, cols_, rec->data)) {
                matched_rids_.push_back(curr_rid);
            }
        }
        if (!matched_rids_.empty()) {
            rid_ = matched_rids_.front();
        }
    }

    void nextTuple() override {
        if (is_end()) {
            return;
        }
        rid_pos_++;
        if (!is_end()) {
            rid_ = matched_rids_[rid_pos_];
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override { return rid_pos_ >= matched_rids_.size(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    std::string getType() override { return "IndexScanExecutor"; }

    Rid &rid() override { return rid_; }
};

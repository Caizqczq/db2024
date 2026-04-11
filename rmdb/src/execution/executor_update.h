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
#include <unordered_map>
#include <utility>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

    struct UpdatePlan {
        Rid rid;
        RmRecord old_rec;
        RmRecord new_rec;
        std::vector<std::vector<char>> old_keys;
        std::vector<std::vector<char>> new_keys;
    };

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        std::vector<UpdatePlan> plans;
        plans.reserve(rids_.size());

        for (auto &rid : rids_) {
            auto old_rec_ptr = fh_->get_record(rid, context_);
            if (old_rec_ptr == nullptr) {
                continue;
            }

            UpdatePlan plan{rid, *old_rec_ptr, *old_rec_ptr, {}, {}};
            for (auto &set_clause : set_clauses_) {
                auto col_meta = tab_.get_col(set_clause.lhs.col_name);
                Value rhs = set_clause.rhs;
                if (rhs.type != col_meta->type) {
                    bool lhs_numeric = col_meta->type == TYPE_INT || col_meta->type == TYPE_FLOAT;
                    bool rhs_numeric = rhs.type == TYPE_INT || rhs.type == TYPE_FLOAT;
                    if (lhs_numeric && rhs_numeric) {
                        if (col_meta->type == TYPE_FLOAT && rhs.type == TYPE_INT) {
                            rhs.set_float(static_cast<float>(rhs.int_val));
                        } else if (col_meta->type == TYPE_INT && rhs.type == TYPE_FLOAT) {
                            rhs.set_int(static_cast<int>(rhs.float_val));
                        }
                        rhs.raw = nullptr;
                    } else {
                        throw IncompatibleTypeError(coltype2str(col_meta->type), coltype2str(rhs.type));
                    }
                }
                if (rhs.raw == nullptr) {
                    rhs.init_raw(col_meta->len);
                }
                memcpy(plan.new_rec.data + col_meta->offset, rhs.raw->data, col_meta->len);
            }

            plan.old_keys.reserve(tab_.indexes.size());
            plan.new_keys.reserve(tab_.indexes.size());
            for (auto &index : tab_.indexes) {
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                int offset = 0;
                for (auto &index_col : index.cols) {
                    memcpy(old_key.data() + offset, plan.old_rec.data + index_col.offset, index_col.len);
                    memcpy(new_key.data() + offset, plan.new_rec.data + index_col.offset, index_col.len);
                    offset += index_col.len;
                }
                plan.old_keys.push_back(std::move(old_key));
                plan.new_keys.push_back(std::move(new_key));
            }
            plans.push_back(std::move(plan));
        }

        for (size_t idx = 0; idx < tab_.indexes.size(); ++idx) {
            const auto &index = tab_.indexes[idx];

            std::unordered_map<std::string, Rid> in_stmt_new_keys;
            for (auto &plan : plans) {
                if (memcmp(plan.old_keys[idx].data(), plan.new_keys[idx].data(), index.col_tot_len) == 0) {
                    continue;
                }
                std::string key_sig(plan.new_keys[idx].data(), plan.new_keys[idx].data() + index.col_tot_len);
                auto it = in_stmt_new_keys.find(key_sig);
                if (it != in_stmt_new_keys.end() && it->second != plan.rid) {
                    throw RMDBError("Duplicate key for unique index");
                }
                in_stmt_new_keys[key_sig] = plan.rid;
            }

            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            for (auto &plan : plans) {
                if (memcmp(plan.old_keys[idx].data(), plan.new_keys[idx].data(), index.col_tot_len) == 0) {
                    continue;
                }
                std::vector<Rid> result;
                if (ih->get_value(plan.new_keys[idx].data(), &result, context_ ? context_->txn_ : nullptr)) {
                    for (const auto &existing_rid : result) {
                        if (existing_rid != plan.rid) {
                            throw RMDBError("Duplicate key for unique index");
                        }
                    }
                }
            }
        }

        for (auto &plan : plans) {
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name_, plan.rid, plan.old_rec));
            }

            for (size_t idx = 0; idx < tab_.indexes.size(); ++idx) {
                auto &index = tab_.indexes[idx];
                if (memcmp(plan.old_keys[idx].data(), plan.new_keys[idx].data(), index.col_tot_len) == 0) {
                    continue;
                }
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->delete_entry(plan.old_keys[idx].data(), context_ ? context_->txn_ : nullptr);
            }

            fh_->update_record(plan.rid, plan.new_rec.data, context_);

            for (size_t idx = 0; idx < tab_.indexes.size(); ++idx) {
                auto &index = tab_.indexes[idx];
                if (memcmp(plan.old_keys[idx].data(), plan.new_keys[idx].data(), index.col_tot_len) == 0) {
                    continue;
                }
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->insert_entry(plan.new_keys[idx].data(), plan.rid, context_ ? context_->txn_ : nullptr);
            }
        }
        return nullptr;
    }

    std::string getType() override { return "UpdateExecutor"; }

    Rid &rid() override { return _abstract_rid; }
};

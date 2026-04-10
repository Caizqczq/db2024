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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

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
        for (auto &rid : rids_) {
            auto old_rec = fh_->get_record(rid, context_);
            auto new_rec = std::make_unique<RmRecord>(*old_rec);

            for (auto &set_clause : set_clauses_) {
                auto col_meta = tab_.get_col(set_clause.lhs.col_name);
                if (set_clause.rhs.type != col_meta->type) {
                    bool lhs_numeric = col_meta->type == TYPE_INT || col_meta->type == TYPE_FLOAT;
                    bool rhs_numeric = set_clause.rhs.type == TYPE_INT || set_clause.rhs.type == TYPE_FLOAT;
                    if (lhs_numeric && rhs_numeric) {
                        if (col_meta->type == TYPE_FLOAT && set_clause.rhs.type == TYPE_INT) {
                            set_clause.rhs.set_float(static_cast<float>(set_clause.rhs.int_val));
                        } else if (col_meta->type == TYPE_INT && set_clause.rhs.type == TYPE_FLOAT) {
                            set_clause.rhs.set_int(static_cast<int>(set_clause.rhs.float_val));
                        }
                        set_clause.rhs.raw = nullptr;
                    } else {
                        throw IncompatibleTypeError(coltype2str(col_meta->type), coltype2str(set_clause.rhs.type));
                    }
                }
                if (set_clause.rhs.raw == nullptr) {
                    set_clause.rhs.init_raw(col_meta->len);
                }
                memcpy(new_rec->data + col_meta->offset, set_clause.rhs.raw->data, col_meta->len);
            }

            for (auto &index : tab_.indexes) {
                auto ih = sm_manager_->ihs_
                              .at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols))
                              .get();
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                int offset = 0;
                for (auto &index_col : index.cols) {
                    memcpy(old_key.data() + offset, old_rec->data + index_col.offset, index_col.len);
                    memcpy(new_key.data() + offset, new_rec->data + index_col.offset, index_col.len);
                    offset += index_col.len;
                }
                if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0) {
                    ih->delete_entry(old_key.data(), context_->txn_);
                    ih->insert_entry(new_key.data(), rid, context_->txn_);
                }
            }

            fh_->update_record(rid, new_rec->data, context_);
        }
        return nullptr;
    }

    std::string getType() override { return "UpdateExecutor"; }

    Rid &rid() override { return _abstract_rid; }
};

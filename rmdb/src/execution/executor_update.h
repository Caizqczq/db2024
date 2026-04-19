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
            RmRecord new_rec(*old_rec);

            for (auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                auto &rhs = set_clause.rhs;
                if (col->type != rhs.type) {
                    if (col->type == TYPE_FLOAT && rhs.type == TYPE_INT) {
                        rhs.set_float(static_cast<float>(rhs.int_val));
                    } else {
                        throw IncompatibleTypeError(coltype2str(col->type), coltype2str(rhs.type));
                    }
                }
                if (rhs.raw == nullptr) {
                    rhs.init_raw(col->len);
                }
                std::memcpy(new_rec.data + col->offset, rhs.raw->data, col->len);
            }

            if (context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *old_rec));
            }

            // 先更新基表，再维护索引；若索引维护异常，由事务回滚恢复
            fh_->update_record(rid, new_rec.data, context_);

            for (auto &index : tab_.indexes) {
                auto *ih = sm_manager_->open_index_handle(tab_name_, index.cols);
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                make_index_key(index.cols, old_rec->data, old_key.data());
                make_index_key(index.cols, new_rec.data, new_key.data());
                if (std::memcmp(old_key.data(), new_key.data(), index.col_tot_len) == 0) {
                    continue;
                }
                ih->delete_entry(old_key.data(), context_ ? context_->txn_ : nullptr);
                ih->insert_entry(new_key.data(), rid, context_ ? context_->txn_ : nullptr);
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};

/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "execution/execution_defs.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    (void)log_manager;
    std::unique_lock<std::mutex> lock(latch_);
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++, IsolationLevel::SERIALIZABLE);
    }
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_++);
    TransactionManager::txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }

    auto write_set = txn->get_write_set();
    for (auto *write_record : *write_set) {
        delete write_record;
    }
    write_set->clear();

    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    lock_set->clear();

    txn->set_state(TransactionState::COMMITTED);
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }

    auto write_set = txn->get_write_set();
    for (auto it = write_set->rbegin(); it != write_set->rend(); ++it) {
        auto *write_record = *it;
        auto &tab_name = write_record->GetTableName();
        auto &rid = write_record->GetRid();
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        auto &tab = sm_manager_->db_.get_table(tab_name);

        if (write_record->GetWriteType() == WType::INSERT_TUPLE) {
            // undo insert: 删除该记录并清理索引项
            auto rec = fh->get_record(rid, nullptr);
            for (auto &index : tab.indexes) {
                auto *ih = sm_manager_->open_index_handle(tab_name, index.cols);
                std::vector<char> key_buf(index.col_tot_len);
                make_index_key(index.cols, rec->data, key_buf.data());
                std::vector<Rid> existing;
                if (ih->get_value(key_buf.data(), &existing, txn) && !existing.empty()) {
                    if (existing.front().page_no == rid.page_no && existing.front().slot_no == rid.slot_no) {
                        ih->delete_entry(key_buf.data(), txn);
                    }
                }
            }
            fh->delete_record(rid, nullptr);
        } else if (write_record->GetWriteType() == WType::DELETE_TUPLE) {
            // undo delete: 把旧记录插回原位置并恢复索引项
            auto &old_rec = write_record->GetRecord();
            fh->insert_record(rid, old_rec.data);
            for (auto &index : tab.indexes) {
                auto *ih = sm_manager_->open_index_handle(tab_name, index.cols);
                std::vector<char> key_buf(index.col_tot_len);
                make_index_key(index.cols, old_rec.data, key_buf.data());
                ih->insert_entry(key_buf.data(), rid, txn);
            }
        } else if (write_record->GetWriteType() == WType::UPDATE_TUPLE) {
            // undo update: 恢复旧值，并更新受影响的索引项
            auto curr_rec = fh->get_record(rid, nullptr);
            auto &old_rec = write_record->GetRecord();
            for (auto &index : tab.indexes) {
                auto *ih = sm_manager_->open_index_handle(tab_name, index.cols);
                std::vector<char> curr_key(index.col_tot_len);
                std::vector<char> old_key(index.col_tot_len);
                make_index_key(index.cols, curr_rec->data, curr_key.data());
                make_index_key(index.cols, old_rec.data, old_key.data());
                if (std::memcmp(curr_key.data(), old_key.data(), index.col_tot_len) != 0) {
                    std::vector<Rid> curr_existing;
                    if (ih->get_value(curr_key.data(), &curr_existing, txn) && !curr_existing.empty()) {
                        if (curr_existing.front().page_no == rid.page_no && curr_existing.front().slot_no == rid.slot_no) {
                            ih->delete_entry(curr_key.data(), txn);
                        }
                    }
                    std::vector<Rid> existing;
                    if (!ih->get_value(old_key.data(), &existing, txn)) {
                        ih->insert_entry(old_key.data(), rid, txn);
                    }
                }
            }
            fh->update_record(rid, old_rec.data, nullptr);
        }

        delete write_record;
    }
    write_set->clear();

    auto lock_set = txn->get_lock_set();
    for (auto &lock_data_id : *lock_set) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    lock_set->clear();

    txn->set_state(TransactionState::ABORTED);
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
}

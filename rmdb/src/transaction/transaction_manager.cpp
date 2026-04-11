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

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

namespace {
void delete_write_records(const std::shared_ptr<std::deque<WriteRecord *>> &write_set) {
    if (!write_set) {
        return;
    }
    for (auto *write_record : *write_set) {
        delete write_record;
    }
    write_set->clear();
}

void release_locks(Transaction *txn, LockManager *lock_manager) {
    if (txn == nullptr || lock_manager == nullptr) {
        return;
    }
    auto lock_set = txn->get_lock_set();
    if (!lock_set) {
        return;
    }
    for (auto it = lock_set->begin(); it != lock_set->end(); ++it) {
        lock_manager->unlock(txn, *it);
    }
    lock_set->clear();
}

void clear_txn_resources(Transaction *txn) {
    if (txn == nullptr) {
        return;
    }
    auto index_latch_page_set = txn->get_index_latch_page_set();
    if (index_latch_page_set) {
        index_latch_page_set->clear();
    }
    auto index_deleted_page_set = txn->get_index_deleted_page_set();
    if (index_deleted_page_set) {
        index_deleted_page_set->clear();
    }
}

std::vector<char> make_index_key(const IndexMeta &index, const RmRecord &record) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (const auto &col : index.cols) {
        memcpy(key.data() + offset, record.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}

void delete_index_entry(SmManager *sm_manager, const std::string &tab_name, const IndexMeta &index,
                        const RmRecord &record, Transaction *txn) {
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    auto key = make_index_key(index, record);
    ih->delete_entry(key.data(), txn);
}

void insert_index_entry(SmManager *sm_manager, const std::string &tab_name, const IndexMeta &index,
                        const RmRecord &record, const Rid &rid, Transaction *txn) {
    auto ih = sm_manager->ihs_.at(sm_manager->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
    auto key = make_index_key(index, record);
    ih->insert_entry(key.data(), rid, txn);
}
}  // namespace

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    (void)log_manager;
    std::lock_guard<std::mutex> guard(latch_);
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_++);
    txn_map[txn->get_transaction_id()] = txn;
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

    delete_write_records(txn->get_write_set());
    release_locks(txn, lock_manager_);
    clear_txn_resources(txn);

    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    txn->set_state(TransactionState::COMMITTED);
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
        auto fh = sm_manager_->fhs_.at(write_record->GetTableName()).get();
        TabMeta &tab = sm_manager_->db_.get_table(write_record->GetTableName());
        switch (write_record->GetWriteType()) {
            case WType::INSERT_TUPLE: {
                try {
                    auto inserted_record = fh->get_record(write_record->GetRid(), nullptr);
                    for (const auto &index : tab.indexes) {
                        delete_index_entry(sm_manager_, write_record->GetTableName(), index, *inserted_record, txn);
                    }
                } catch (RMDBError &) {
                }
                fh->delete_record(write_record->GetRid(), nullptr);
                break;
            }
            case WType::DELETE_TUPLE: {
                fh->insert_record(write_record->GetRid(), write_record->GetRecord().data);
                for (const auto &index : tab.indexes) {
                    insert_index_entry(sm_manager_, write_record->GetTableName(), index, write_record->GetRecord(),
                                       write_record->GetRid(), txn);
                }
                break;
            }
            case WType::UPDATE_TUPLE: {
                try {
                    auto new_record = fh->get_record(write_record->GetRid(), nullptr);
                    for (const auto &index : tab.indexes) {
                        delete_index_entry(sm_manager_, write_record->GetTableName(), index, *new_record, txn);
                    }
                } catch (RMDBError &) {
                }
                fh->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
                for (const auto &index : tab.indexes) {
                    insert_index_entry(sm_manager_, write_record->GetTableName(), index, write_record->GetRecord(),
                                       write_record->GetRid(), txn);
                }
                break;
            }
            default:
                break;
        }
    }

    delete_write_records(write_set);
    release_locks(txn, lock_manager_);
    clear_txn_resources(txn);

    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    txn->set_state(TransactionState::ABORTED);
}

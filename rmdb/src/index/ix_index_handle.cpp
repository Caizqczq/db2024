/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include <vector>

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    int left = 0;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    int left = page_hdr->is_leaf ? 0 : 1;
    int right = page_hdr->num_key;
    while (left < right) {
        int mid = left + (right - left) / 2;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) <= 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    int pos = lower_bound(key);
    if (pos >= get_size() || ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) != 0) {
        return false;
    }
    *value = get_rid(pos);
    return true;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    int child_idx = upper_bound(key) - 1;
    if (child_idx < 0) {
        child_idx = 0;
    }
    return value_at(child_idx);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    assert(pos >= 0 && pos <= get_size());
    int old_size = get_size();
    if (old_size > pos) {
        memmove(keys + (pos + n) * file_hdr->col_tot_len_, keys + pos * file_hdr->col_tot_len_,
                (old_size - pos) * file_hdr->col_tot_len_);
        memmove(rids + pos + n, rids + pos, (old_size - pos) * sizeof(Rid));
    }
    memcpy(keys + pos * file_hdr->col_tot_len_, key, n * file_hdr->col_tot_len_);
    memcpy(rids + pos, rid, n * sizeof(Rid));
    set_size(old_size + n);
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    int pos = lower_bound(key);
    if (pos < get_size() && ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return get_size();
    }
    insert_pair(pos, key, value);
    return get_size();
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    assert(pos >= 0 && pos < get_size());
    int old_size = get_size();
    if (pos < old_size - 1) {
        memmove(keys + pos * file_hdr->col_tot_len_, keys + (pos + 1) * file_hdr->col_tot_len_,
                (old_size - pos - 1) * file_hdr->col_tot_len_);
        memmove(rids + pos, rids + pos + 1, (old_size - pos - 1) * sizeof(Rid));
    }
    set_size(old_size - 1);
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    int pos = lower_bound(key);
    if (pos >= get_size() || ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) != 0) {
        return get_size();
    }
    erase_pair(pos);
    return get_size();
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char *buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);

    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                               Transaction *transaction, bool find_first) {
    if (find_first) {
        return std::make_pair(fetch_node(file_hdr_->first_leaf_), false);
    }

    IxNodeHandle *node = fetch_node(file_hdr_->root_page_);
    while (!node->is_leaf_page()) {
        page_id_t child_page_no = node->internal_lookup(key);
        IxNodeHandle *child = fetch_node(child_page_no);
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        node = child;
    }
    return std::make_pair(node, false);
}

bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, transaction);
    Rid *rid = nullptr;
    bool found = leaf->leaf_lookup(key, &rid);
    if (found) {
        result->push_back(*rid);
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return found;
}

IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->page_hdr->parent = node->get_parent_page_no();
    new_node->page_hdr->num_key = 0;
    new_node->page_hdr->prev_leaf = IX_NO_PAGE;
    new_node->page_hdr->next_leaf = IX_NO_PAGE;

    int total_size = node->get_size();
    int move_start = total_size / 2;
    int move_count = total_size - move_start;
    new_node->insert_pairs(0, node->get_key(move_start), node->get_rid(move_start), move_count);
    node->set_size(move_start);

    if (node->is_leaf_page()) {
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(node->get_next_leaf());
        if (node->get_next_leaf() != IX_NO_PAGE) {
            IxNodeHandle *next_leaf = fetch_node(node->get_next_leaf());
            next_leaf->set_prev_leaf(new_node->get_page_no());
            buffer_pool_manager_->unpin_page(next_leaf->get_page_id(), true);
        }
        node->set_next_leaf(new_node->get_page_no());
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
    } else {
        for (int i = 0; i < new_node->get_size(); ++i) {
            maintain_child(new_node, i);
        }
    }
    return new_node;
}

void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                       Transaction *transaction) {
    if (old_node->is_root_page()) {
        IxNodeHandle *new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->parent = IX_NO_PAGE;
        new_root->page_hdr->num_key = 0;
        new_root->insert_pair(0, old_node->get_key(0), Rid{old_node->get_page_no(), -1});
        new_root->insert_pair(1, key, Rid{new_node->get_page_no(), -1});
        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());
        update_root_page_no(new_root->get_page_no());
        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        return;
    }

    IxNodeHandle *parent = fetch_node(old_node->get_parent_page_no());
    int insert_pos = parent->find_child(old_node) + 1;
    parent->insert_pair(insert_pos, key, Rid{new_node->get_page_no(), -1});
    new_node->set_parent_page_no(parent->get_page_no());

    if (parent->get_size() >= parent->get_max_size()) {
        IxNodeHandle *new_parent = split(parent);
        insert_into_parent(parent, new_parent->get_key(0), new_parent, transaction);
        buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
    }
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
}

page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::INSERT, transaction);
    int old_size = leaf->get_size();
    int new_size = leaf->insert(key, value);
    if (new_size == old_size) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        throw RMDBError("Duplicate key for unique index");
    }

    page_id_t leaf_page_no = leaf->get_page_no();
    if (leaf->get_size() >= leaf->get_max_size()) {
        IxNodeHandle *new_leaf = split(leaf);
        insert_into_parent(leaf, new_leaf->get_key(0), new_leaf, transaction);
        buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);
    }
    maintain_parent(leaf);
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    return leaf_page_no;
}

bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);
    int old_size = leaf->get_size();
    int new_size = leaf->remove(key);
    if (new_size == old_size) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return false;
    }

    bool delete_leaf = coalesce_or_redistribute(leaf, transaction, &root_is_latched);
    PageId page_id = leaf->get_page_id();
    buffer_pool_manager_->unpin_page(page_id, true);
    if (delete_leaf) {
        buffer_pool_manager_->delete_page(page_id);
    }
    return true;
}

bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    if (node->is_root_page()) {
        return adjust_root(node);
    }

    if (node->get_size() >= node->get_min_size()) {
        if (node->get_size() > 0) {
            maintain_parent(node);
        }
        return false;
    }

    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
    int index = parent->find_child(node);
    IxNodeHandle *neighbor = (index == 0) ? fetch_node(parent->value_at(1)) : fetch_node(parent->value_at(index - 1));

    if (neighbor->get_size() + node->get_size() >= node->get_min_size() * 2) {
        redistribute(neighbor, node, parent, index);
        buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return false;
    }

    bool delete_node = coalesce(&neighbor, &node, &parent, index, transaction, root_is_latched);
    bool delete_parent = coalesce_or_redistribute(parent, transaction, root_is_latched);
    PageId parent_page_id = parent->get_page_id();
    buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
    buffer_pool_manager_->unpin_page(parent_page_id, true);
    if (delete_parent) {
        buffer_pool_manager_->delete_page(parent_page_id);
    }
    return delete_node;
}

bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        page_id_t child_page_no = old_root_node->remove_and_return_only_child();
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(IX_NO_PAGE);
        update_root_page_no(child_page_no);
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        return true;
    }
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        file_hdr_->first_leaf_ = old_root_node->get_page_no();
        file_hdr_->last_leaf_ = old_root_node->get_page_no();
    }
    return false;
}

void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    if (index == 0) {
        Rid moved_rid = *neighbor_node->get_rid(0);
        std::vector<char> moved_key(file_hdr_->col_tot_len_);
        memcpy(moved_key.data(), neighbor_node->get_key(0), file_hdr_->col_tot_len_);
        node->insert_pair(node->get_size(), moved_key.data(), moved_rid);
        neighbor_node->erase_pair(0);
        if (!node->is_leaf_page()) {
            maintain_child(node, node->get_size() - 1);
        }
        memcpy(parent->get_key(1), neighbor_node->get_key(0), file_hdr_->col_tot_len_);
    } else {
        int last_idx = neighbor_node->get_size() - 1;
        Rid moved_rid = *neighbor_node->get_rid(last_idx);
        std::vector<char> moved_key(file_hdr_->col_tot_len_);
        memcpy(moved_key.data(), neighbor_node->get_key(last_idx), file_hdr_->col_tot_len_);
        node->insert_pair(0, moved_key.data(), moved_rid);
        neighbor_node->erase_pair(last_idx);
        if (!node->is_leaf_page()) {
            maintain_child(node, 0);
        }
        memcpy(parent->get_key(index), node->get_key(0), file_hdr_->col_tot_len_);
    }
    maintain_parent(node);
}

bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    if (index == 0) {
        std::swap(*neighbor_node, *node);
        index = 1;
    }

    int neighbor_size = (*neighbor_node)->get_size();
    (*neighbor_node)
        ->insert_pairs(neighbor_size, (*node)->get_key(0), (*node)->get_rid(0), (*node)->get_size());

    if (!(*neighbor_node)->is_leaf_page()) {
        for (int i = neighbor_size; i < (*neighbor_node)->get_size(); ++i) {
            maintain_child((*neighbor_node), i);
        }
    } else {
        if (file_hdr_->last_leaf_ == (*node)->get_page_no()) {
            file_hdr_->last_leaf_ = (*neighbor_node)->get_page_no();
        }
        erase_leaf(*node);
    }

    (*parent)->erase_pair(index);
    if ((*neighbor_node)->get_size() > 0) {
        maintain_parent(*neighbor_node);
    }
    return true;
}

Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    return *node->get_rid(iid.slot_no);
}

Iid IxIndexHandle::lower_bound(const char *key) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    int slot_no = leaf->lower_bound(key);
    Iid iid{leaf->get_page_no(), slot_no};
    if (slot_no == leaf->get_size() && leaf->get_page_no() != file_hdr_->last_leaf_) {
        iid.page_no = leaf->get_next_leaf();
        iid.slot_no = 0;
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return iid;
}

Iid IxIndexHandle::upper_bound(const char *key) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    int slot_no = leaf->upper_bound(key);
    Iid iid{leaf->get_page_no(), slot_no};
    if (slot_no == leaf->get_size() && leaf->get_page_no() != file_hdr_->last_leaf_) {
        iid.page_no = leaf->get_next_leaf();
        iid.slot_no = 0;
    }
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return iid;
}

Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    return iid;
}

Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    return node;
}

IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    node->page_hdr->next_free_page_no = IX_NO_PAGE;
    node->page_hdr->parent = IX_NO_PAGE;
    node->page_hdr->num_key = 0;
    node->page_hdr->is_leaf = false;
    node->page_hdr->prev_leaf = IX_NO_PAGE;
    node->page_hdr->next_leaf = IX_NO_PAGE;
    return node;
}

void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    if (node->get_size() == 0) {
        return;
    }
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);
        curr = parent;
        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}

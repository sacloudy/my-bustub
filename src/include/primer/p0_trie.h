//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept {
    key_char_ = other_trie_node.key_char_;
    is_end_ = other_trie_node.is_end_;
    children_ = std::move(other_trie_node.children_);
  }

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  bool HasChildren() const { return !children_.empty(); }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  bool IsEndNode() const { return is_end_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  char GetKeyChar() const { return key_char_; }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr. 这最后一句有点意思, 为什么要这么设计Insert函数签名呢,这不是冗余了？
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.指向unique_ptr的指针可以在不拥有unique_ptr所有权的情况下访问底层数据
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) {  // 去掉&&也一样~
    if (key_char != child->key_char_ || HasChild(key_char)) {
      return nullptr;
    }
    return &(children_[key_char] = std::move(child));  // 获取绑定到左值child上的右值引用
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist. 和Insert返回值类型一样
   */
  std::unique_ptr<TrieNode> *GetChildNode(char key_char) {
    if (!HasChild(key_char)) {
      return nullptr;
    }
    return &children_[key_char];  // 直接这句的话你不存在返回的是map.end()和不是nullptr,所以上面判空不能少
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    if (HasChild(key_char)) {
      children_.erase(key_char);
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that marks the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::forward<TrieNode>(trieNode)) {  // 直接move应该也行
    value_ = value;
    is_end_ = true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with the given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char) {
    value_ = value;
    is_end_ = true;
  }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  T GetValue() const { return value_; }
};

class RLock {
  ReaderWriterLatch *latch_;

 public:
  explicit RLock(ReaderWriterLatch *latch) : latch_(latch) { latch_->RLock(); }
  ~RLock() { latch_->RUnlock(); }
};

class WLock {
  ReaderWriterLatch *latch_;

 public:
  explicit WLock(ReaderWriterLatch *latch) : latch_(latch) { latch_->WLock(); }
  ~WLock() { latch_->WUnlock(); }
};

/**
 * Trie is a concurrent key-value store. Each key is a string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

 public:
  /**
   * TODO(P0): Add implementation
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() : root_(std::make_unique<TrieNode>('\0')) {}

  /**
   * TODO(P0): Add implementation
   *
   * @brief Insert key-value pair into the trie.
   *
   * If the key is an empty string, return false immediately.
   *
   * If the key already exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and returns false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if the key already exists
   */
  template <typename T>
  bool Insert(const std::string &key, T value) {
    WLock w(&latch_);

    if (key.empty() || Exist(key)) {
      return false;
    }
    std::unique_ptr<TrieNode> *curr = &root_;
    size_t i = 0;
    while (i < key.size() - 1) {  // -1是为了留出最后一次操作单独处理~~
      if (!(*curr)->HasChild(key[i])) {
        curr = (*curr)->InsertChildNode(key[i], std::make_unique<TrieNode>(key[i]));
      } else {
        curr = (*curr)->GetChildNode(key[i]);
      }
      i++;
    }
    if (!(*curr)->HasChild(key[i])) {
      (*curr)->InsertChildNode(key[i], std::make_unique<TrieNodeWithValue<T>>(key[i], value));
    } else {
      curr = (*curr)->GetChildNode(key[i]);
      if ((*curr)->IsEndNode()) {  // key完全重复了，不允许再执行插入的
        return false;
      }
      (*curr) = std::make_unique<TrieNodeWithValue<T>>(std::move(*(*curr)), value);  // constructor中改is_end
    }
    return true;
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and are not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @return True if the key exists and is removed, false otherwise
   */
  bool Remove(const std::string &key) {
    WLock w(&latch_);

    if (!Exist(key)) {  // 不存在直接返回,所以下面RemoveInner的key是一定存在于trie上的
      return false;
    }
    bool success = false;
    RemoveInner(key, 0, &root_, &success);
    return success;
  }

  bool RemoveInner(const std::string &key, size_t i, std::unique_ptr<TrieNode> *curr, bool *success) {
    // 处理key[i]号字符，当然此时处于其父节点curr的位置，递归调用处理下一个字符(可能移除)，递归调用RemoveInner(子节点)
    // 递归函数返回其子节点是否能被移除来进行操作, 最后返回自身(curr)能否被移除
    if (i == key.size()) {
      *success = true;
      (*curr)->SetEndNode(false);  // 无论有没有孩子节点，先设置成false
      return !(*curr)->HasChildren() &&
             !(*curr)->IsEndNode();  // 没有孩子才将传入节点删除(后面这个不是终端节点可以去掉吧)
    }

    bool can_remove = RemoveInner(key, i + 1, (*curr)->GetChildNode(key[i]), success);

    if (can_remove) {
      (*curr)->RemoveChildNode(key[i]);
    }

    return !(*curr)->HasChildren() &&
           !(*curr)->IsEndNode();  // 不是终端节点也不能移除，上面递归终止条件当然不用加这个判断
  }

  /**
   * TODO(P0): Add implementation
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If the given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find the correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  T GetValue(const std::string &key, bool *success) {
    RLock w(&latch_);

    *success = false;
    if (key.empty()) {
      return T();
    }
    std::unique_ptr<TrieNode> *curr = &root_;
    size_t i = 0;
    while (i + 1 < key.size()) {
      curr = (*curr)->GetChildNode(key[i]);
      if (curr == nullptr) {
        return T();
      }
      i++;
    }
    curr = (*curr)->GetChildNode(key[i]);
    if (curr == nullptr || !(*curr)->IsEndNode()) {
      return T();
    }
    *success = true;
    //    LOG_INFO("# Pages: %d", *(*curr))); // Call to implicitly-deleted copy constructor of 'bustub::TrieNode'
    //    p0_trie.h:51:3: note: copy constructor is implicitly deleted because 'TrieNode' has a user-declared move
    //    constructor
    auto tobe_converted = &(*(*curr));  // &和*不能抵消, 其实直接curr->get()就行了
    auto ret = dynamic_cast<TrieNodeWithValue<T> *>(tobe_converted);
    if (ret == nullptr) {  // 这里确实容易出错呢, 因为正常情况叶节点的children_的指针就是空的!
      *success = false;
      return T();
    }
    return ret->GetValue();
  }

  bool Exist(const std::string &key) {
    if (key.empty()) {
      return false;
    }
    std::unique_ptr<TrieNode> *curr = &root_;
    size_t i = 0;
    while (i + 1 < key.size()) {
      curr = (*curr)->GetChildNode(key[i]);
      if (curr == nullptr) {
        return false;
      }
      i++;
    }
    curr = (*curr)->GetChildNode(key[i]);
    return curr != nullptr && (*curr)->IsEndNode();
  }
};
}  // namespace bustub

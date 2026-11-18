#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace l3kv {

template <typename Key, typename Value, typename Compare = std::less<>>
class BTreeMap {
public:
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const Key, Value>;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using key_compare = Compare;

  static constexpr uint16_t MAX_KEYS = 32;
  static constexpr uint16_t MIN_KEYS = MAX_KEYS / 2;

private:
  struct Node {
    bool is_leaf;
    uint16_t count = 0;
    Node *parent = nullptr;

    explicit Node(bool leaf) : is_leaf(leaf) {}
    virtual ~Node() = default;
  };

  struct LeafNode final : Node {
    LeafNode *prev = nullptr;
    LeafNode *next = nullptr;
    alignas(Key) std::byte keys_buf[MAX_KEYS * sizeof(Key)];
    alignas(Value) std::byte vals_buf[MAX_KEYS * sizeof(Value)];

    LeafNode() : Node(true) {}

    ~LeafNode() override {
      for (uint16_t i = 0; i < this->count; ++i) {
        key_ptr(i)->~Key();
        val_ptr(i)->~Value();
      }
    }

    Key *key_ptr(uint16_t i) {
      return std::launder(reinterpret_cast<Key *>(keys_buf) + i);
    }
    const Key *key_ptr(uint16_t i) const {
      return std::launder(reinterpret_cast<const Key *>(keys_buf) + i);
    }
    Value *val_ptr(uint16_t i) {
      return std::launder(reinterpret_cast<Value *>(vals_buf) + i);
    }
    const Value *val_ptr(uint16_t i) const {
      return std::launder(reinterpret_cast<const Value *>(vals_buf) + i);
    }

    Key &key(uint16_t i) { return *key_ptr(i); }
    const Key &key(uint16_t i) const { return *key_ptr(i); }
    Value &val(uint16_t i) { return *val_ptr(i); }
    const Value &val(uint16_t i) const { return *val_ptr(i); }

    template <typename K, typename V>
    void insert_at(uint16_t idx, K &&k, V &&v) {
      if (idx < this->count) {
        new (key_ptr(this->count)) Key(std::move(key(this->count - 1)));
        new (val_ptr(this->count)) Value(std::move(val(this->count - 1)));
        for (uint16_t i = this->count - 1; i > idx; --i) {
          key(i) = std::move(key(i - 1));
          val(i) = std::move(val(i - 1));
        }
        key(idx) = std::forward<K>(k);
        val(idx) = std::forward<V>(v);
      } else {
        new (key_ptr(idx)) Key(std::forward<K>(k));
        new (val_ptr(idx)) Value(std::forward<V>(v));
      }
      ++this->count;
    }

    void erase_at(uint16_t idx) {
      for (uint16_t i = idx; i + 1 < this->count; ++i) {
        key(i) = std::move(key(i + 1));
        val(i) = std::move(val(i + 1));
      }
      --this->count;
      key_ptr(this->count)->~Key();
      val_ptr(this->count)->~Value();
    }
  };

  struct InternalNode final : Node {
    alignas(Key) std::byte keys_buf[MAX_KEYS * sizeof(Key)];
    Node *children[MAX_KEYS + 1]{};

    InternalNode() : Node(false) {}

    ~InternalNode() override {
      for (uint16_t i = 0; i < this->count; ++i) {
        key_ptr(i)->~Key();
      }
    }

    Key *key_ptr(uint16_t i) {
      return std::launder(reinterpret_cast<Key *>(keys_buf) + i);
    }
    const Key *key_ptr(uint16_t i) const {
      return std::launder(reinterpret_cast<const Key *>(keys_buf) + i);
    }
    Key &key(uint16_t i) { return *key_ptr(i); }
    const Key &key(uint16_t i) const { return *key_ptr(i); }

    template <typename K>
    void insert_at(uint16_t idx, K &&k, Node *right_child) {
      if (idx < this->count) {
        new (key_ptr(this->count)) Key(std::move(key(this->count - 1)));
        children[this->count + 1] = children[this->count];
        for (uint16_t i = this->count - 1; i > idx; --i) {
          key(i) = std::move(key(i - 1));
          children[i + 1] = children[i];
        }
        key(idx) = std::forward<K>(k);
        children[idx + 1] = right_child;
      } else {
        new (key_ptr(idx)) Key(std::forward<K>(k));
        children[idx + 1] = right_child;
      }
      right_child->parent = this;
      ++this->count;
    }

    void erase_at(uint16_t key_idx, uint16_t child_idx) {
      for (uint16_t i = key_idx; i + 1 < this->count; ++i) {
        key(i) = std::move(key(i + 1));
      }
      for (uint16_t i = child_idx; i < this->count; ++i) {
        children[i] = children[i + 1];
      }
      children[this->count] = nullptr;
      --this->count;
      key_ptr(this->count)->~Key();
    }
  };

  struct PairRef {
    const Key &first;
    Value &second;
  };

  struct ConstPairRef {
    const Key &first;
    const Value &second;
  };

  template <typename Ref>
  struct ArrowProxy {
    Ref ref;
    Ref *operator->() { return &ref; }
  };

public:
  class const_iterator;

  class iterator {
    friend class BTreeMap;
    friend class const_iterator;

    const BTreeMap *tree_ = nullptr;
    LeafNode *leaf_ = nullptr;
    uint16_t idx_ = 0;

  public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = std::pair<const Key, Value>;
    using difference_type = std::ptrdiff_t;
    using reference = PairRef;
    using pointer = ArrowProxy<PairRef>;

    iterator() = default;
    iterator(const BTreeMap *tree, LeafNode *leaf, uint16_t idx)
        : tree_(tree), leaf_(leaf), idx_(idx) {}

    reference operator*() const {
      return PairRef{leaf_->key(idx_), leaf_->val(idx_)};
    }

    pointer operator->() const {
      return pointer{PairRef{leaf_->key(idx_), leaf_->val(idx_)}};
    }

    iterator &operator++() {
      if (leaf_) {
        ++idx_;
        if (idx_ >= leaf_->count) {
          leaf_ = leaf_->next;
          idx_ = 0;
        }
      }
      return *this;
    }

    iterator operator++(int) {
      iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    iterator &operator--() {
      if (!leaf_) {
        if (tree_ && tree_->tail_ && tree_->tail_->count > 0) {
          leaf_ = tree_->tail_;
          idx_ = leaf_->count - 1;
        }
      } else if (idx_ > 0) {
        --idx_;
      } else {
        leaf_ = leaf_->prev;
        idx_ = leaf_ ? static_cast<uint16_t>(leaf_->count - 1) : 0;
      }
      return *this;
    }

    iterator operator--(int) {
      iterator tmp = *this;
      --(*this);
      return tmp;
    }

    bool operator==(const iterator &other) const {
      return leaf_ == other.leaf_ && idx_ == other.idx_;
    }

    bool operator!=(const iterator &other) const { return !(*this == other); }
  };

  class const_iterator {
    friend class BTreeMap;

    const BTreeMap *tree_ = nullptr;
    const LeafNode *leaf_ = nullptr;
    uint16_t idx_ = 0;

  public:
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = std::pair<const Key, Value>;
    using difference_type = std::ptrdiff_t;
    using reference = ConstPairRef;
    using pointer = ArrowProxy<ConstPairRef>;

    const_iterator() = default;
    const_iterator(const BTreeMap *tree, const LeafNode *leaf, uint16_t idx)
        : tree_(tree), leaf_(leaf), idx_(idx) {}
    const_iterator(const iterator &it)
        : tree_(it.tree_), leaf_(it.leaf_), idx_(it.idx_) {}

    reference operator*() const {
      return ConstPairRef{leaf_->key(idx_), leaf_->val(idx_)};
    }

    pointer operator->() const {
      return pointer{ConstPairRef{leaf_->key(idx_), leaf_->val(idx_)}};
    }

    const_iterator &operator++() {
      if (leaf_) {
        ++idx_;
        if (idx_ >= leaf_->count) {
          leaf_ = leaf_->next;
          idx_ = 0;
        }
      }
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    const_iterator &operator--() {
      if (!leaf_) {
        if (tree_ && tree_->tail_ && tree_->tail_->count > 0) {
          leaf_ = tree_->tail_;
          idx_ = leaf_->count - 1;
        }
      } else if (idx_ > 0) {
        --idx_;
      } else {
        leaf_ = leaf_->prev;
        idx_ = leaf_ ? static_cast<uint16_t>(leaf_->count - 1) : 0;
      }
      return *this;
    }

    const_iterator operator--(int) {
      const_iterator tmp = *this;
      --(*this);
      return tmp;
    }

    bool operator==(const const_iterator &other) const {
      return leaf_ == other.leaf_ && idx_ == other.idx_;
    }

    bool operator!=(const const_iterator &other) const {
      return !(*this == other);
    }
  };

private:
  Node *root_ = nullptr;
  LeafNode *head_ = nullptr;
  LeafNode *tail_ = nullptr;
  size_type size_ = 0;
  [[no_unique_address]] Compare comp_{};

  void destroy_subtree(Node *node) {
    if (!node)
      return;
    if (!node->is_leaf) {
      auto *in = static_cast<InternalNode *>(node);
      for (uint16_t i = 0; i <= in->count; ++i) {
        destroy_subtree(in->children[i]);
      }
    }
    delete node;
  }

  template <typename K>
  uint16_t leaf_lower_bound(const LeafNode *leaf, const K &k) const {
    uint16_t lo = 0, hi = leaf->count;
    while (lo < hi) {
      uint16_t mid = lo + (hi - lo) / 2;
      if (comp_(leaf->key(mid), k)) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  template <typename K>
  uint16_t leaf_upper_bound(const LeafNode *leaf, const K &k) const {
    uint16_t lo = 0, hi = leaf->count;
    while (lo < hi) {
      uint16_t mid = lo + (hi - lo) / 2;
      if (!comp_(k, leaf->key(mid))) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  template <typename K>
  LeafNode *find_leaf(const K &k) const {
    Node *cur = root_;
    while (cur && !cur->is_leaf) {
      auto *in = static_cast<InternalNode *>(cur);
      uint16_t lo = 0, hi = in->count;
      while (lo < hi) {
        uint16_t mid = lo + (hi - lo) / 2;
        if (!comp_(k, in->key(mid))) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      cur = in->children[lo];
    }
    return static_cast<LeafNode *>(cur);
  }

  void insert_into_parent(Node *left, Key sep, Node *right) {
    if (!left->parent) {
      auto *new_root = new InternalNode();
      new (new_root->key_ptr(0)) Key(std::move(sep));
      new_root->children[0] = left;
      new_root->children[1] = right;
      new_root->count = 1;
      left->parent = new_root;
      right->parent = new_root;
      root_ = new_root;
      return;
    }

    auto *parent = static_cast<InternalNode *>(left->parent);
    uint16_t child_idx = 0;
    while (parent->children[child_idx] != left) {
      ++child_idx;
    }

    if (parent->count < MAX_KEYS) {
      parent->insert_at(child_idx, std::move(sep), right);
      return;
    }

    // Split InternalNode: temporary arrays of size MAX_KEYS + 1 keys, MAX_KEYS + 2 children
    alignas(Key) std::byte tmp_keys_buf[(MAX_KEYS + 1) * sizeof(Key)];
    Key *tmp_keys = reinterpret_cast<Key *>(tmp_keys_buf);
    Node *tmp_children[MAX_KEYS + 2];

    for (uint16_t i = 0; i < child_idx; ++i) {
      new (tmp_keys + i) Key(std::move(parent->key(i)));
    }
    new (tmp_keys + child_idx) Key(std::move(sep));
    for (uint16_t i = child_idx; i < parent->count; ++i) {
      new (tmp_keys + i + 1) Key(std::move(parent->key(i)));
    }

    for (uint16_t i = 0; i <= child_idx; ++i) {
      tmp_children[i] = parent->children[i];
    }
    tmp_children[child_idx + 1] = right;
    for (uint16_t i = child_idx + 1; i <= parent->count; ++i) {
      tmp_children[i + 1] = parent->children[i];
    }

    uint16_t total_keys = MAX_KEYS + 1;
    uint16_t mid = total_keys / 2;

    // Destroy old keys in parent
    for (uint16_t i = 0; i < parent->count; ++i) {
      parent->key_ptr(i)->~Key();
    }
    parent->count = 0;

    for (uint16_t i = 0; i < mid; ++i) {
      new (parent->key_ptr(i)) Key(std::move(tmp_keys[i]));
      parent->children[i] = tmp_children[i];
      parent->children[i]->parent = parent;
    }
    parent->children[mid] = tmp_children[mid];
    parent->children[mid]->parent = parent;
    parent->count = mid;

    Key up_key(std::move(tmp_keys[mid]));

    auto *new_internal = new InternalNode();
    uint16_t right_count = total_keys - mid - 1;
    for (uint16_t i = 0; i < right_count; ++i) {
      new (new_internal->key_ptr(i)) Key(std::move(tmp_keys[mid + 1 + i]));
      new_internal->children[i] = tmp_children[mid + 1 + i];
      new_internal->children[i]->parent = new_internal;
    }
    new_internal->children[right_count] = tmp_children[total_keys];
    new_internal->children[right_count]->parent = new_internal;
    new_internal->count = right_count;

    for (uint16_t i = 0; i < total_keys; ++i) {
      tmp_keys[i].~Key();
    }

    insert_into_parent(parent, std::move(up_key), new_internal);
  }

  template <typename K, typename V>
  std::pair<iterator, bool> insert_into_leaf(LeafNode *leaf, uint16_t idx, K &&k,
                                             V &&v, bool assign_if_exists) {
    if (idx < leaf->count && !comp_(k, leaf->key(idx)) &&
        !comp_(leaf->key(idx), k)) {
      if (assign_if_exists) {
        leaf->val(idx) = std::forward<V>(v);
      }
      return {iterator(this, leaf, idx), false};
    }

    if (leaf->count < MAX_KEYS) {
      leaf->insert_at(idx, std::forward<K>(k), std::forward<V>(v));
      ++size_;
      return {iterator(this, leaf, idx), true};
    }

    // Split LeafNode
    auto *new_leaf = new LeafNode();
    new_leaf->next = leaf->next;
    new_leaf->prev = leaf;
    if (leaf->next) {
      leaf->next->prev = new_leaf;
    } else {
      tail_ = new_leaf;
    }
    leaf->next = new_leaf;

    alignas(Key) std::byte tmp_keys_buf[(MAX_KEYS + 1) * sizeof(Key)];
    alignas(Value) std::byte tmp_vals_buf[(MAX_KEYS + 1) * sizeof(Value)];
    Key *tmp_keys = reinterpret_cast<Key *>(tmp_keys_buf);
    Value *tmp_vals = reinterpret_cast<Value *>(tmp_vals_buf);

    for (uint16_t i = 0; i < idx; ++i) {
      new (tmp_keys + i) Key(std::move(leaf->key(i)));
      new (tmp_vals + i) Value(std::move(leaf->val(i)));
    }
    new (tmp_keys + idx) Key(std::forward<K>(k));
    new (tmp_vals + idx) Value(std::forward<V>(v));
    for (uint16_t i = idx; i < leaf->count; ++i) {
      new (tmp_keys + i + 1) Key(std::move(leaf->key(i)));
      new (tmp_vals + i + 1) Value(std::move(leaf->val(i)));
    }

    uint16_t total = MAX_KEYS + 1;
    uint16_t split = total / 2;

    for (uint16_t i = 0; i < leaf->count; ++i) {
      leaf->key_ptr(i)->~Key();
      leaf->val_ptr(i)->~Value();
    }
    leaf->count = 0;

    for (uint16_t i = 0; i < split; ++i) {
      new (leaf->key_ptr(i)) Key(std::move(tmp_keys[i]));
      new (leaf->val_ptr(i)) Value(std::move(tmp_vals[i]));
    }
    leaf->count = split;

    for (uint16_t i = split; i < total; ++i) {
      new (new_leaf->key_ptr(i - split)) Key(std::move(tmp_keys[i]));
      new (new_leaf->val_ptr(i - split)) Value(std::move(tmp_vals[i]));
    }
    new_leaf->count = total - split;

    for (uint16_t i = 0; i < total; ++i) {
      tmp_keys[i].~Key();
      tmp_vals[i].~Value();
    }

    ++size_;
    iterator ret = (idx < split)
                       ? iterator(this, leaf, idx)
                       : iterator(this, new_leaf, static_cast<uint16_t>(idx - split));

    insert_into_parent(leaf, Key(new_leaf->key(0)), new_leaf);
    return {ret, true};
  }

  uint16_t find_child_index(InternalNode *parent, Node *child) const {
    for (uint16_t i = 0; i <= parent->count; ++i) {
      if (parent->children[i] == child)
        return i;
    }
    return 0;
  }

  void rebalance_internal(InternalNode *node) {
    if (node == root_) {
      if (node->count == 0) {
        Node *new_root = node->children[0];
        new_root->parent = nullptr;
        root_ = new_root;
        node->children[0] = nullptr;
        delete node;
      }
      return;
    }
    if (node->count >= MIN_KEYS)
      return;

    auto *parent = static_cast<InternalNode *>(node->parent);
    uint16_t cidx = find_child_index(parent, node);

    // Try borrow from left sibling
    if (cidx > 0) {
      auto *left = static_cast<InternalNode *>(parent->children[cidx - 1]);
      if (left->count > MIN_KEYS) {
        // Shift node's keys and children right by 1
        if (node->count > 0) {
          new (node->key_ptr(node->count)) Key(std::move(node->key(node->count - 1)));
          for (uint16_t i = node->count - 1; i > 0; --i) {
            node->key(i) = std::move(node->key(i - 1));
          }
          node->key(0) = std::move(parent->key(cidx - 1));
        } else {
          new (node->key_ptr(0)) Key(std::move(parent->key(cidx - 1)));
        }
        for (uint16_t i = node->count + 1; i > 0; --i) {
          node->children[i] = node->children[i - 1];
        }
        node->children[0] = left->children[left->count];
        node->children[0]->parent = node;
        ++node->count;

        parent->key(cidx - 1) = std::move(left->key(left->count - 1));
        left->children[left->count] = nullptr;
        --left->count;
        left->key_ptr(left->count)->~Key();
        return;
      }
    }

    // Try borrow from right sibling
    if (cidx < parent->count) {
      auto *right = static_cast<InternalNode *>(parent->children[cidx + 1]);
      if (right->count > MIN_KEYS) {
        new (node->key_ptr(node->count)) Key(std::move(parent->key(cidx)));
        node->children[node->count + 1] = right->children[0];
        node->children[node->count + 1]->parent = node;
        ++node->count;

        parent->key(cidx) = std::move(right->key(0));
        right->erase_at(0, 0);
        return;
      }
    }

    // Merge with left or right sibling
    if (cidx > 0) {
      auto *left = static_cast<InternalNode *>(parent->children[cidx - 1]);
      new (left->key_ptr(left->count)) Key(std::move(parent->key(cidx - 1)));
      ++left->count;
      for (uint16_t i = 0; i < node->count; ++i) {
        new (left->key_ptr(left->count)) Key(std::move(node->key(i)));
        left->children[left->count] = node->children[i];
        left->children[left->count]->parent = left;
        ++left->count;
      }
      left->children[left->count] = node->children[node->count];
      left->children[left->count]->parent = left;

      parent->erase_at(cidx - 1, cidx);
      delete node;
    } else {
      auto *right = static_cast<InternalNode *>(parent->children[cidx + 1]);
      new (node->key_ptr(node->count)) Key(std::move(parent->key(cidx)));
      ++node->count;
      for (uint16_t i = 0; i < right->count; ++i) {
        new (node->key_ptr(node->count)) Key(std::move(right->key(i)));
        node->children[node->count] = right->children[i];
        node->children[node->count]->parent = node;
        ++node->count;
      }
      node->children[node->count] = right->children[right->count];
      node->children[node->count]->parent = node;

      parent->erase_at(cidx, cidx + 1);
      delete right;
    }

    rebalance_internal(parent);
  }

public:
  BTreeMap() {
    auto *leaf = new LeafNode();
    root_ = leaf;
    head_ = leaf;
    tail_ = leaf;
  }

  ~BTreeMap() { destroy_subtree(root_); }

  BTreeMap(const BTreeMap &) = delete;
  BTreeMap &operator=(const BTreeMap &) = delete;

  BTreeMap(BTreeMap &&other) noexcept
      : root_(other.root_), head_(other.head_), tail_(other.tail_),
        size_(other.size_), comp_(std::move(other.comp_)) {
    auto *leaf = new LeafNode();
    other.root_ = leaf;
    other.head_ = leaf;
    other.tail_ = leaf;
    other.size_ = 0;
  }

  BTreeMap &operator=(BTreeMap &&other) noexcept {
    if (this != &other) {
      destroy_subtree(root_);
      root_ = other.root_;
      head_ = other.head_;
      tail_ = other.tail_;
      size_ = other.size_;
      comp_ = std::move(other.comp_);
      auto *leaf = new LeafNode();
      other.root_ = leaf;
      other.head_ = leaf;
      other.tail_ = leaf;
      other.size_ = 0;
    }
    return *this;
  }

  bool empty() const noexcept { return size_ == 0; }
  size_type size() const noexcept { return size_; }

  void clear() {
    destroy_subtree(root_);
    auto *leaf = new LeafNode();
    root_ = leaf;
    head_ = leaf;
    tail_ = leaf;
    size_ = 0;
  }

  iterator begin() noexcept {
    return size_ == 0 ? end() : iterator(this, head_, 0);
  }
  const_iterator begin() const noexcept {
    return size_ == 0 ? end() : const_iterator(this, head_, 0);
  }
  const_iterator cbegin() const noexcept { return begin(); }

  iterator end() noexcept { return iterator(this, nullptr, 0); }
  const_iterator end() const noexcept { return const_iterator(this, nullptr, 0); }
  const_iterator cend() const noexcept { return end(); }

  template <typename K>
  iterator find(const K &k) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    if (idx < leaf->count && !comp_(k, leaf->key(idx)) &&
        !comp_(leaf->key(idx), k)) {
      return iterator(this, leaf, idx);
    }
    return end();
  }

  template <typename K>
  const_iterator find(const K &k) const {
    const LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    if (idx < leaf->count && !comp_(k, leaf->key(idx)) &&
        !comp_(leaf->key(idx), k)) {
      return const_iterator(this, leaf, idx);
    }
    return end();
  }

  template <typename K>
  iterator lower_bound(const K &k) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    if (idx >= leaf->count) {
      leaf = leaf->next;
      idx = 0;
    }
    return leaf ? iterator(this, leaf, idx) : end();
  }

  template <typename K>
  const_iterator lower_bound(const K &k) const {
    const LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    if (idx >= leaf->count) {
      leaf = leaf->next;
      idx = 0;
    }
    return leaf ? const_iterator(this, leaf, idx) : end();
  }

  template <typename K>
  iterator upper_bound(const K &k) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_upper_bound(leaf, k);
    if (idx >= leaf->count) {
      leaf = leaf->next;
      idx = 0;
    }
    return leaf ? iterator(this, leaf, idx) : end();
  }

  template <typename K>
  const_iterator upper_bound(const K &k) const {
    const LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_upper_bound(leaf, k);
    if (idx >= leaf->count) {
      leaf = leaf->next;
      idx = 0;
    }
    return leaf ? const_iterator(this, leaf, idx) : end();
  }

  template <typename K>
  Value &operator[](K &&k) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    if (idx < leaf->count && !comp_(k, leaf->key(idx)) &&
        !comp_(leaf->key(idx), k)) {
      return leaf->val(idx);
    }
    return insert_into_leaf(leaf, idx, Key(std::forward<K>(k)), Value{}, false)
        .first->second;
  }

  template <typename K, typename V>
  std::pair<iterator, bool> insert_or_assign(K &&k, V &&v) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    return insert_into_leaf(leaf, idx, std::forward<K>(k), std::forward<V>(v),
                            true);
  }

  template <typename K, typename V>
  std::pair<iterator, bool> emplace(K &&k, V &&v) {
    LeafNode *leaf = find_leaf(k);
    uint16_t idx = leaf_lower_bound(leaf, k);
    return insert_into_leaf(leaf, idx, std::forward<K>(k), std::forward<V>(v),
                            false);
  }

  template <typename K, typename V>
  iterator emplace_hint(const_iterator hint, K &&k, V &&v) {
    if (hint.leaf_ != nullptr) {
      LeafNode *leaf = const_cast<LeafNode *>(hint.leaf_);
      // Fast path when hint points to the right leaf
      bool ge_first = (leaf->count == 0) || !comp_(k, leaf->key(0));
      bool lt_next = (!leaf->next) || comp_(k, leaf->next->key(0));
      if (ge_first && lt_next) {
        uint16_t idx = leaf_lower_bound(leaf, k);
        return insert_into_leaf(leaf, idx, std::forward<K>(k), std::forward<V>(v),
                                false)
            .first;
      }
    }
    return emplace(std::forward<K>(k), std::forward<V>(v)).first;
  }

  iterator erase(iterator pos) { return erase(const_iterator(pos)); }

  iterator erase(const_iterator pos) {
    if (!pos.leaf_)
      return end();

    LeafNode *leaf = const_cast<LeafNode *>(pos.leaf_);
    uint16_t idx = pos.idx_;

    leaf->erase_at(idx);
    --size_;

    if (leaf == root_ || leaf->count >= MIN_KEYS) {
      if (idx < leaf->count) {
        return iterator(this, leaf, idx);
      }
      return leaf->next ? iterator(this, leaf->next, 0) : end();
    }

    auto *parent = static_cast<InternalNode *>(leaf->parent);
    uint16_t cidx = find_child_index(parent, leaf);

    // Track next element across borrow/merge
    LeafNode *next_leaf = (idx < leaf->count) ? leaf : leaf->next;
    uint16_t next_idx = (idx < leaf->count) ? idx : 0;

    // Try borrow from left sibling
    if (cidx > 0) {
      auto *left = static_cast<LeafNode *>(parent->children[cidx - 1]);
      if (left->count > MIN_KEYS) {
        leaf->insert_at(0, std::move(left->key(left->count - 1)),
                        std::move(left->val(left->count - 1)));
        left->erase_at(left->count - 1);
        parent->key(cidx - 1) = leaf->key(0);
        if (next_leaf == leaf) {
          ++next_idx;
        }
        return next_leaf ? iterator(this, next_leaf, next_idx) : end();
      }
    }

    // Try borrow from right sibling
    if (cidx < parent->count) {
      auto *right = static_cast<LeafNode *>(parent->children[cidx + 1]);
      if (right->count > MIN_KEYS) {
        leaf->insert_at(leaf->count, std::move(right->key(0)),
                        std::move(right->val(0)));
        right->erase_at(0);
        parent->key(cidx) = right->key(0);
        if (next_leaf == right) {
          // The element that was right->key(0) moved to leaf->key(leaf->count - 1)
          next_leaf = leaf;
          next_idx = leaf->count - 1;
        }
        return next_leaf ? iterator(this, next_leaf, next_idx) : end();
      }
    }

    // Merge with left or right sibling
    if (cidx > 0) {
      auto *left = static_cast<LeafNode *>(parent->children[cidx - 1]);
      uint16_t left_old_count = left->count;
      for (uint16_t i = 0; i < leaf->count; ++i) {
        new (left->key_ptr(left->count)) Key(std::move(leaf->key(i)));
        new (left->val_ptr(left->count)) Value(std::move(leaf->val(i)));
        ++left->count;
      }
      if (next_leaf == leaf) {
        next_leaf = left;
        next_idx = static_cast<uint16_t>(left_old_count + next_idx);
      }
      left->next = leaf->next;
      if (leaf->next) {
        leaf->next->prev = left;
      } else {
        tail_ = left;
      }
      parent->erase_at(cidx - 1, cidx);
      delete leaf;
    } else {
      auto *right = static_cast<LeafNode *>(parent->children[cidx + 1]);
      uint16_t leaf_old_count = leaf->count;
      for (uint16_t i = 0; i < right->count; ++i) {
        new (leaf->key_ptr(leaf->count)) Key(std::move(right->key(i)));
        new (leaf->val_ptr(leaf->count)) Value(std::move(right->val(i)));
        ++leaf->count;
      }
      if (next_leaf == right) {
        next_leaf = leaf;
        next_idx = static_cast<uint16_t>(leaf_old_count + next_idx);
      }
      leaf->next = right->next;
      if (right->next) {
        right->next->prev = leaf;
      } else {
        tail_ = leaf;
      }
      parent->erase_at(cidx, cidx + 1);
      delete right;
    }

    rebalance_internal(parent);
    return next_leaf ? iterator(this, next_leaf, next_idx) : end();
  }

  template <typename K,
            typename = std::enable_if_t<!std::is_convertible_v<K, const_iterator> &&
                                        !std::is_convertible_v<K, iterator>>>
  size_type erase(const K &k) {
    auto it = find(k);
    if (it == end())
      return 0;
    erase(it);
    return 1;
  }
};

} // namespace l3kv

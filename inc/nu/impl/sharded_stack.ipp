#include <iostream>
#include <ranges>

namespace nu {

template <typename T>
inline Stack<T>::Stack() : l_key_(0) {}

template <typename T>
inline std::size_t Stack<T>::size() const {
  return stack_.size();
}

template <typename T>
inline bool Stack<T>::empty() const {
  return stack_.empty();
}

template <typename T>
inline Stack<T>::Val Stack<T>::back() const {
  return stack_.top();
}

template <typename T>
inline std::size_t Stack<T>::push_back(Val v) {
  stack_.push(std::move(v));
  return stack_.size();
}

template <typename T>
inline std::size_t Stack<T>::push_back_batch(std::vector<Val> vec) {
  for (auto &v : vec) {
    stack_.push(std::move(v));
  }

  return stack_.size();
}

template <typename T>
inline std::optional<T> Stack<T>::pop_back() {
  if (unlikely(stack_.empty())) {
    return std::nullopt;
  }
  auto ret = std::move(stack_.top());
  stack_.pop();
  return ret;
}

template <typename T>
template <typename... S0s, typename... S1s>
inline void Stack<T>::for_all(void (*fn)(const Key &key, Val &val, S0s...),
                              S1s &&... states) {
  BUG();
}

template <typename T>
inline void Stack<T>::split(Key *mid_k, Stack *latter_half) {
  latter_half->l_key_ = l_key_ + stack_.size();
  *mid_k = latter_half->l_key_;
}

template <typename T>
inline void Stack<T>::merge(Stack stack) {
  // Needs temporary storage to preserve LIFO order
  std::vector<T> t(stack.size());
  while (!stack.stack_.empty()) {
    t.push_back(stack.stack_.top());
    stack.stack_.pop();
  }

  for (T &v : std::ranges::views::reverse(t)) {
    stack_.push(std::move(v));
  }
}

template <typename T>
template <class Archive>
inline void Stack<T>::save(Archive &ar) const {
  ar(stack_);
}

template <typename T>
template <class Archive>
inline void Stack<T>::load(Archive &ar) {
  ar(stack_);
}

template <typename T, typename LL>
inline ShardedStack<T, LL>::ShardedStack() {}

template <typename T, typename LL>
inline ShardedStack<T, LL>::ShardedStack(
    std::optional<typename Base::Hint> hint)
    : Base(hint, /* size_bound = */ std::nullopt) {}

template <typename T, typename LL>
inline void ShardedStack<T, LL>::push(const T &value) {
  Base::push_back(value);
}

template <typename T, typename LL>
inline void ShardedStack<T, LL>::push(T &&value) {
  Base::push_back(std::move(value));
}

template <typename T, typename LL>
inline T ShardedStack<T, LL>::top() const {
  return Base::back();
}

template <typename T, typename LL>
inline T ShardedStack<T, LL>::pop() {
  return Base::pop_back();
}

template <typename T, typename LL>
inline ShardedStack<T, LL> make_sharded_stack() {
  return ShardedStack<T, LL>(std::nullopt);
}

}  // namespace nu

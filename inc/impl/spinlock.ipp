namespace nu {

inline SpinLock::SpinLock() {}

inline SpinLock::~SpinLock() {}

inline void SpinLock::Lock() { spin_lock(&spinlock_); }

inline void SpinLock::Unlock() { spin_unlock(&spinlock_); }

inline bool SpinLock::TryLock() { return spin_try_lock(&spinlock_); }

} // namespace nu

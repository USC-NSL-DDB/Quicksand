namespace nu {

inline SpinLock::SpinLock() {}

inline SpinLock::~SpinLock() {}

inline void SpinLock::lock() { spin_lock(&spinlock_); }

inline void SpinLock::unlock() { spin_unlock(&spinlock_); }

inline bool SpinLock::try_lock() { return spin_try_lock(&spinlock_); }

} // namespace nu

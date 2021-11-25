namespace nu {

inline SpinLock::SpinLock() { spin_lock_init(&spinlock_); }

inline SpinLock::~SpinLock() { assert(!spin_lock_held(&spinlock_)); }

inline void SpinLock::lock() { spin_lock_np(&spinlock_); }

inline void SpinLock::unlock() { spin_unlock_np(&spinlock_); }

inline bool SpinLock::try_lock() { return spin_try_lock(&spinlock_); }

} // namespace nu

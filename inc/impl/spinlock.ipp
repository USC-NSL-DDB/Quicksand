namespace nu {

inline SpinLock::SpinLock() {}

inline SpinLock::~SpinLock() {}

inline void SpinLock::lock() { spinlock_.Lock(); }

inline void SpinLock::unlock() { spinlock_.Unlock(); }

inline bool SpinLock::try_lock() { return spinlock_.TryLock(); }

} // namespace nu

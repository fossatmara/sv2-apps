use std::{
    hash::Hash,
    sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use dashmap::DashMap;

type SharedLockResult<'a, T, R> = Result<R, PoisonError<MutexGuard<'a, T>>>;
type SharedRwReadResult<'a, T, R> = Result<R, PoisonError<RwLockReadGuard<'a, T>>>;
type SharedRwWriteResult<'a, T, R> = Result<R, PoisonError<RwLockWriteGuard<'a, T>>>;

/// Thread-safe shared mutable value using `Mutex` for exclusive access.
#[derive(Debug)]
pub struct SharedLock<T>(Arc<Mutex<T>>);

impl<T> Clone for SharedLock<T> {
    fn clone(&self) -> Self {
        SharedLock(Arc::clone(&self.0))
    }
}

impl<T> SharedLock<T> {
    /// Create a new shared value.
    pub fn new(v: T) -> Self {
        SharedLock(Arc::new(Mutex::new(v)))
    }

    /// Execute a closure with mutable access to the inner value.
    ///
    /// Caution: `f` runs while the mutex is held. Avoid re-entering this
    /// `SharedLock` from inside the closure.
    pub fn with<F, R>(&self, f: F) -> SharedLockResult<'_, T, R>
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut lock = self.0.lock()?;
        Ok(f(&mut *lock))
    }

    /// Get a cloned snapshot of the value.
    pub fn get(&self) -> SharedLockResult<'_, T, T>
    where
        T: Clone,
    {
        self.with(|v| v.clone())
    }

    /// Replace the inner value.
    pub fn set(&self, value: T) -> SharedLockResult<'_, T, ()> {
        self.with(|v| *v = value)?;
        Ok(())
    }
}

/// Thread-safe shared value using `RwLock` for concurrent reads and exclusive writes.
#[derive(Debug)]
pub struct SharedRw<T>(Arc<RwLock<T>>);

impl<T> Clone for SharedRw<T> {
    fn clone(&self) -> Self {
        SharedRw(Arc::clone(&self.0))
    }
}

impl<T> SharedRw<T> {
    /// Create a new shared value.
    pub fn new(v: T) -> Self {
        SharedRw(Arc::new(RwLock::new(v)))
    }

    /// Execute a closure with read-only access.
    ///
    /// Caution: `f` runs while the read lock is held. Avoid re-entering this
    /// `SharedRw` from inside the closure, especially for writes.
    pub fn read<F, R>(&self, f: F) -> SharedRwReadResult<'_, T, R>
    where
        F: FnOnce(&T) -> R,
    {
        let guard = self.0.read()?;
        Ok(f(&*guard))
    }

    /// Execute a closure with mutable access.
    ///
    /// Caution: `f` runs while the write lock is held. Avoid re-entering this
    /// `SharedRw` from inside the closure.
    pub fn write<F, R>(&self, f: F) -> SharedRwWriteResult<'_, T, R>
    where
        F: FnOnce(&mut T) -> R,
    {
        let mut guard = self.0.write()?;
        Ok(f(&mut *guard))
    }

    /// Get a cloned snapshot of the value.
    pub fn get(&self) -> SharedRwReadResult<'_, T, T>
    where
        T: Clone,
    {
        self.read(|v| v.clone())
    }

    /// Replace the inner value.
    pub fn set(&self, value: T) -> SharedRwWriteResult<'_, T, ()> {
        self.write(|v| *v = value)?;
        Ok(())
    }
}

/// Concurrent map wrapper over `DashMap` providing ergonomic scoped access.
#[derive(Debug)]
pub struct SharedMap<K: Eq + Clone + Hash, V>(Arc<DashMap<K, V>>);

impl<K: Eq + Clone + Hash, V> Clone for SharedMap<K, V> {
    fn clone(&self) -> Self {
        SharedMap(Arc::clone(&self.0))
    }
}

impl<K: Eq + Hash + Clone, V> SharedMap<K, V> {
    /// Create a new concurrent map.
    pub fn new() -> Self {
        SharedMap(Arc::new(DashMap::new()))
    }

    /// Get an owned clone of a value.
    ///
    /// This releases the map entry guard immediately, so the entry may be removed
    /// or replaced while the caller is still using the clone. Use this only when
    /// stale clones are acceptable. Prefer [`Self::with`] or [`Self::with_mut`]
    /// when later work depends on the entry still being present.
    pub fn get_cloned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.0.get(key).map(|refc| refc.value().clone())
    }

    /// Read a value for a key using a closure.
    ///
    /// Caution: `f` runs while the entry guard is held. Avoid re-entering this
    /// `SharedMap` from inside the closure.
    pub fn with<F, R>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let guard = self.0.get(key)?;
        let result = f(guard.value());
        drop(guard);
        Some(result)
    }

    /// Mutate a value for a key using a closure.
    ///
    /// Caution: `f` runs while the entry guard is held. Avoid re-entering this
    /// `SharedMap` from inside the closure.
    pub fn with_mut<F, R>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&mut V) -> R,
    {
        let mut guard = self.0.get_mut(key)?;
        let result = f(guard.value_mut());
        Some(result)
    }

    /// Mutate an entry, inserting a value produced by `default` when absent.
    pub fn with_mut_or_insert_with<F, D, R>(&self, key: K, default: D, f: F) -> R
    where
        F: FnOnce(&mut V) -> R,
        D: FnOnce() -> V,
    {
        let mut entry = self.0.entry(key).or_insert_with(default);
        f(entry.value_mut())
    }

    /// Mutate an entry, inserting its default value when absent.
    pub fn with_mut_or_default<F, R>(&self, key: K, f: F) -> R
    where
        V: Default,
        F: FnOnce(&mut V) -> R,
    {
        self.with_mut_or_insert_with(key, V::default, f)
    }

    /// Iterate over all entries immutably.
    ///
    /// Caution: `f` runs while an iterator entry guard is held. Avoid
    /// re-entering this `SharedMap` from inside the closure.
    pub fn for_each<F, Ret>(&self, mut f: F)
    where
        F: FnMut(K, &V) -> Ret,
    {
        for entry in self.0.iter() {
            f(entry.key().clone(), entry.value());
        }
    }

    /// Iterate over all entries mutably.
    ///
    /// Caution: `f` runs while an iterator entry guard is held. Avoid
    /// re-entering this `SharedMap` from inside the closure.
    pub fn for_each_mut<F, Ret>(&self, mut f: F)
    where
        F: FnMut(K, &mut V) -> Ret,
    {
        for mut entry in self.0.iter_mut() {
            f(entry.key().clone(), entry.value_mut());
        }
    }

    /// Fallible mutable iteration over all entries.
    ///
    /// Caution: `f` runs while an iterator entry guard is held. Avoid
    /// re-entering this `SharedMap` from inside the closure.
    pub fn try_for_each_mut<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(K, &mut V) -> Result<(), E>,
    {
        for mut entry in self.0.iter_mut() {
            f(entry.key().clone(), entry.value_mut())?;
        }
        Ok(())
    }

    /// Fallible iteration over all entries.
    ///
    /// Caution: `f` runs while an iterator entry guard is held. Avoid
    /// re-entering this `SharedMap` from inside the closure.
    pub fn try_for_each<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(K, &V) -> Result<(), E>,
    {
        for entry in self.0.iter() {
            f(entry.key().clone(), entry.value())?;
        }
        Ok(())
    }

    /// Insert a key-value pair.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.0.insert(key, value)
    }

    /// Remove a key.
    pub fn remove(&self, key: &K) -> Option<(K, V)> {
        self.0.remove(key)
    }

    /// Check if a key exists.
    pub fn contains_key(&self, key: &K) -> bool {
        self.0.contains_key(key)
    }

    /// Retain entries matching predicate.
    ///
    /// Caution: `f` runs while `DashMap` is mutating internal shards. Avoid
    /// re-entering this `SharedMap` from inside the predicate.
    pub fn retain<F>(&self, f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        self.0.retain(f);
    }

    /// Collect all keys.
    pub fn keys(&self) -> Vec<K>
    where
        K: Clone,
    {
        self.0.iter().map(|e| e.key().clone()).collect()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Clears the collection.
    pub fn clear(&self) {
        self.0.clear()
    }
}

impl<K: Eq + Hash + Clone, V> Default for SharedMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_basic_usage() {
        let v = SharedLock::new(10);

        let _ = v.with(|x| *x += 5);
        assert_eq!(v.get().unwrap(), 15);

        let _ = v.set(42);
        assert_eq!(v.get().unwrap(), 42);
    }

    #[test]
    fn shared_rw_usage() {
        let v = SharedRw::new(100);

        let a = v.read(|x| *x).unwrap();
        let b = v.read(|x| *x).unwrap();
        assert_eq!(a, b);

        let _ = v.write(|x| *x += 1);
        assert_eq!(v.get().unwrap(), 101);
    }

    #[test]
    fn shared_map_usage() {
        let map = SharedMap::new();

        map.insert("a", 1);
        map.insert("b", 2);

        let val = map.with(&"a", |v| *v).unwrap();
        assert_eq!(val, 1);

        map.with_mut(&"a", |v| *v += 10);
        assert_eq!(map.with(&"a", |v| *v).unwrap(), 11);

        map.with_mut_or_default("c", |v| *v += 3);
        assert_eq!(map.get_cloned(&"c"), Some(3));

        let mut sum = 0;
        map.for_each(|_, v| sum += v);
        assert_eq!(sum, 16);

        map.remove(&"a");
        assert!(!map.contains_key(&"a"));
    }
}

use dashmap::DashMap;
use std::sync::Arc;

/// Interns pubkey strings to share allocations across the graph.
/// Each unique pubkey is stored once, with Arc<str> references shared.
pub struct PubkeyInterner {
    interned: DashMap<Arc<str>, ()>, // Acts as a concurrent set
}

impl PubkeyInterner {
    pub fn new() -> Self {
        Self {
            interned: DashMap::new(),
        }
    }

    /// Intern a pubkey string, returning a shared Arc<str>.
    /// If the string was already interned, returns the existing Arc.
    /// Thread-safe and lock-free for reads of existing strings.
    pub fn intern(&self, s: &str) -> Arc<str> {
        // Fast path: check if already interned
        if let Some(entry) = self.interned.get(s) {
            return entry.key().clone();
        }

        // Slow path: intern new string
        let arc: Arc<str> = Arc::from(s);

        // Use entry API to handle race condition
        self.interned.entry(arc.clone()).or_insert(());

        // Return the arc we created (or the one that won the race)
        if let Some(entry) = self.interned.get(s) {
            entry.key().clone()
        } else {
            arc
        }
    }

    /// Number of unique strings interned
    #[allow(dead_code)] // Public API for memory diagnostics
    pub fn len(&self) -> usize {
        self.interned.len()
    }

    #[allow(dead_code)] // Required by clippy when len() exists
    pub fn is_empty(&self) -> bool {
        self.interned.is_empty()
    }
}

impl Default for PubkeyInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_same_string() {
        let interner = PubkeyInterner::new();

        let s1 = interner.intern("hello");
        let s2 = interner.intern("hello");

        // Should be the same Arc (pointer equality)
        assert!(Arc::ptr_eq(&s1, &s2));
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn test_intern_different_strings() {
        let interner = PubkeyInterner::new();

        let s1 = interner.intern("hello");
        let s2 = interner.intern("world");

        assert!(!Arc::ptr_eq(&s1, &s2));
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_intern_returns_correct_content() {
        let interner = PubkeyInterner::new();

        let pubkey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let interned = interner.intern(pubkey);

        assert_eq!(&*interned, pubkey);
    }
}

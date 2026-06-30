use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Debug, Clone)]
pub struct TPHashSet<T> {
    items_by_key: BTreeMap<String, T>,
    reverse_key_to_key: BTreeMap<String, String>,
}

impl<T> Default for TPHashSet<T> {
    fn default() -> Self {
        Self {
            items_by_key: BTreeMap::new(),
            reverse_key_to_key: BTreeMap::new(),
        }
    }
}

impl<T> TPHashSet<T> {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.items_by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items_by_key.is_empty()
    }

    pub fn insert(&mut self, key: String, value: T) -> Option<T> {
        let reverse_key = Self::reverse_key(&key);
        self.reverse_key_to_key.insert(reverse_key, key.clone());
        self.items_by_key.insert(key, value)
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        self.items_by_key.get(key)
    }

    pub fn get_by_reverse_key(&self, reverse_key: &str) -> Option<&T> {
        self.reverse_key_to_key
            .get(reverse_key)
            .and_then(|key| self.items_by_key.get(key))
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.items_by_key.contains_key(key)
    }

    pub fn contains_reverse_key(&self, reverse_key: &str) -> bool {
        self.reverse_key_to_key.contains_key(reverse_key)
    }

    pub fn search_prefix(&self, prefix: &str) -> Vec<(&String, &T)> {
        let upper_bound = Self::prefix_upper_bound(prefix);
        self.items_by_key
            .range::<String, _>((Bound::Included(prefix.to_string()), Bound::Excluded(upper_bound)))
            .filter(|(key, _)| key.starts_with(prefix))
            .collect()
    }

    pub fn search_suffix(&self, suffix: &str) -> Vec<(&String, &T)> {
        let reversed_suffix = Self::reverse_key(suffix);
        let upper_bound = Self::prefix_upper_bound(&reversed_suffix);
        self.reverse_key_to_key
            .range::<String, _>((Bound::Included(reversed_suffix), Bound::Excluded(upper_bound)))
            .filter_map(|(reverse_key, key)| {
                if reverse_key.starts_with(&Self::reverse_key(suffix)) {
                    self.items_by_key.get(key).map(|value| (key, value))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn search_like(&self, pattern: &str) -> Vec<(&String, &T)> {
        
        if pattern.is_empty() {
            return self.items_by_key.iter().collect();
        }

        if !pattern.contains('%') && !pattern.contains('_') {
            return self
                .items_by_key
                .get_key_value(pattern)
                .map(|(key, value)| vec![(key, value)])
                .unwrap_or_default();
        }

        if pattern.starts_with('%')
            && !pattern[1..].contains('%')
            && !pattern[1..].contains('_')
        {
            return self.search_suffix(&pattern[1..]);
        }

        if pattern.ends_with('%')
            && !pattern[..pattern.len().saturating_sub(1)].contains('%')
            && !pattern.contains('_')
        {
            return self.search_prefix(&pattern[..pattern.len().saturating_sub(1)]);
        }

        self.items_by_key
            .iter()
            .filter(|(key, _)| Self::like_matches(key, pattern))
            .collect()

    }

    pub fn search_like_keys(&self, pattern: &str) -> Vec<String> {
        self.search_like(pattern)
            .into_iter()
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn remove(&mut self, key: &str) -> Option<T> {
        let removed = self.items_by_key.remove(key)?;
        let reverse_key = Self::reverse_key(key);
        self.reverse_key_to_key.remove(&reverse_key);
        Some(removed)
    }

    pub fn clear(&mut self) {
        self.items_by_key.clear();
        self.reverse_key_to_key.clear();
    }

    fn reverse_key(key: &str) -> String {
        key.chars().rev().collect()
    }

    fn prefix_upper_bound(prefix: &str) -> String {
        let mut upper = String::with_capacity(prefix.len() + 1);
        upper.push_str(prefix);
        upper.push(char::MAX);
        upper
    }

    fn like_matches(actual: &str, pattern: &str) -> bool {
        let actual_chars = actual.chars().collect::<Vec<_>>();
        let pattern_chars = pattern.chars().collect::<Vec<_>>();
        Self::like_matches_chars(&actual_chars, &pattern_chars)
    }

    fn like_matches_chars(actual: &[char], pattern: &[char]) -> bool {
        let mut actual_index = 0usize;
        let mut pattern_index = 0usize;
        let mut last_percent_index: Option<usize> = None;
        let mut retry_index = 0usize;

        while actual_index < actual.len() {
            if pattern_index < pattern.len() {
                match pattern[pattern_index] {
                    '_' => {
                        actual_index += 1;
                        pattern_index += 1;
                        continue;
                    }

                    '%' => {
                        last_percent_index = Some(pattern_index);
                        pattern_index += 1;
                        retry_index = actual_index;
                        continue;
                    }

                    expected => {
                        if actual[actual_index] == expected {
                            actual_index += 1;
                            pattern_index += 1;
                            continue;
                        }
                    }
                }
            }

            if let Some(percent_index) = last_percent_index {
                pattern_index = percent_index + 1;
                retry_index += 1;
                actual_index = retry_index;
                continue;
            }

            return false;
        }

        while pattern_index < pattern.len() && pattern[pattern_index] == '%' {
            pattern_index += 1;
        }

        pattern_index == pattern.len()

    }

}

#[cfg(test)]
mod tests {
    use super::TPHashSet;

    #[test]
    fn insert_and_get_roundtrip() {
        let mut set = TPHashSet::new();

        assert!(set.insert("amsterdam".to_string(), 7).is_none());
        assert_eq!(set.get("amsterdam"), Some(&7));
        assert!(set.contains_key("amsterdam"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn reverse_lookup_works() {
        let mut set = TPHashSet::new();

        set.insert("amsterdam".to_string(), 7);

        assert_eq!(set.get_by_reverse_key("madretsma"), Some(&7));
        assert!(set.contains_reverse_key("madretsma"));
    }

    #[test]
    fn prefix_search_returns_sorted_matches() {
        let mut set = TPHashSet::new();
        set.insert("amsterdam".to_string(), 1);
        set.insert("amstel".to_string(), 2);
        set.insert("rotterdam".to_string(), 3);

        let keys = set.search_like_keys("ams%");
        assert_eq!(keys, vec!["amstel".to_string(), "amsterdam".to_string()]);
    }

    #[test]
    fn suffix_search_uses_reverse_index() {
        let mut set = TPHashSet::new();
        set.insert("amsterdam".to_string(), 1);
        set.insert("rotterdam".to_string(), 2);
        set.insert("dam".to_string(), 3);

        let keys = set.search_like_keys("%dam");
        assert_eq!(keys, vec!["dam".to_string(), "amsterdam".to_string(), "rotterdam".to_string()]);
    }

    #[test]
    fn gap_search_refines_candidates() {
        let mut set = TPHashSet::new();
        set.insert("amsterdam".to_string(), 1);
        set.insert("amstel".to_string(), 2);
        set.insert("madteram".to_string(), 3);

        let keys = set.search_like_keys("%ter%am");
        assert_eq!(keys, vec!["amsterdam".to_string(), "madteram".to_string()]);
    }

    #[test]
    fn remove_clears_both_indexes() {
        let mut set = TPHashSet::new();

        set.insert("dam".to_string(), 11);
        assert_eq!(set.remove("dam"), Some(11));
        assert!(set.get("dam").is_none());
        assert!(set.get_by_reverse_key("mad").is_none());
        assert!(set.is_empty());
    }

}

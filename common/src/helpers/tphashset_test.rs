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
    fn prefix_search_can_ignore_ascii_case() {
        let mut set = TPHashSet::new();
        set.insert("Amsterdam".to_string(), 1);
        set.insert("amstel".to_string(), 2);
        set.insert("Rotterdam".to_string(), 3);

        let keys = set
            .search_prefix("ams", true)
            .into_iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["Amsterdam".to_string(), "amstel".to_string()]);
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
    fn suffix_search_can_ignore_ascii_case() {
        let mut set = TPHashSet::new();
        set.insert("AmsterDAM".to_string(), 1);
        set.insert("rotterDam".to_string(), 2);
        set.insert("Case".to_string(), 3);

        let keys = set
            .search_suffix("dam", true)
            .into_iter()
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        assert_eq!(keys, vec!["AmsterDAM".to_string(), "rotterDam".to_string()]);
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

    #[test]
    fn get_mut_updates_value_in_place() {
        let mut set = TPHashSet::new();
        set.insert("sam".to_string(), vec![1u64]);

        let values = set.get_mut("sam").expect("value should exist");
        values.push(2);

        assert_eq!(set.get("sam"), Some(&vec![1, 2]));
    }

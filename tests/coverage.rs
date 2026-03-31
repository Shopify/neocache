//! Comprehensive coverage tests.
//!
//! Each section targets a specific file or group of methods that had zero or
//! near-zero coverage in the baseline report.  Together these tests are
//! designed to push line coverage to ≥92%.

use neocache::mapref::entry::Entry;
use neocache::t::Map;
use neocache::try_result::TryResult;
use neocache::{NeoCache, ReadOnlyView};
use std::collections::hash_map::RandomState;

// ── Constructors and metadata ─────────────────────────────────────────────────

#[test]
fn default_constructor_is_unbounded() {
    // ahash::RandomState does not impl Default, so we use std's RandomState.
    let map: NeoCache<u64, u64, std::collections::hash_map::RandomState> = NeoCache::default();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
    assert_eq!(map.cache_capacity(), 0);
    map.insert(1, 2);
    assert_eq!(map.len(), 1);
}

#[test]
fn clone_copies_all_entries() {
    let map: NeoCache<String, u32> = NeoCache::new(100);
    map.insert("a".to_string(), 1);
    map.insert("b".to_string(), 2);

    let cloned = map.clone();
    assert_eq!(cloned.len(), 2);
    assert_eq!(*cloned.get("a").unwrap(), 1);
    assert_eq!(*cloned.get("b").unwrap(), 2);

    // Original is unaffected
    assert_eq!(map.len(), 2);
}

#[test]
fn with_hasher_creates_unbounded_map() {
    let map: NeoCache<u64, u64, RandomState> = NeoCache::with_hasher(RandomState::new());
    assert_eq!(map.cache_capacity(), 0);
    map.insert(1, 10);
    assert_eq!(*map.get(&1).unwrap(), 10);
}

#[test]
fn with_hasher_and_shard_amount_creates_unbounded_map() {
    let map: NeoCache<u64, u64, RandomState> =
        NeoCache::with_hasher_and_shard_amount(RandomState::new(), 4);
    assert_eq!(map.cache_capacity(), 0);
    map.insert(42, 99);
    assert_eq!(*map.get(&42).unwrap(), 99);
}

#[test]
fn with_capacity_and_hasher_and_shard_amount_full_constructor() {
    let map: NeoCache<u64, u64, RandomState> =
        NeoCache::with_capacity_and_hasher_and_shard_amount(64, RandomState::new(), 4);
    assert_eq!(map.cache_capacity(), 64);
    map.insert(1, 2);
    assert_eq!(*map.get(&1).unwrap(), 2);
}

#[test]
fn is_empty_capacity_cache_capacity() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    assert!(map.is_empty());
    assert_eq!(map.cache_capacity(), 100);

    let _ = map.capacity(); // may be 0 before any insert

    map.insert(1, 1);
    assert!(!map.is_empty());
    let _ = map.capacity();
}

#[test]
fn hasher_and_hash_usize_are_deterministic() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    let _h = map.hasher();

    let h1 = map.hash_usize(&42u64);
    let h2 = map.hash_usize(&42u64);
    assert_eq!(h1, h2);

    // Distinct keys almost always hash differently
    assert_ne!(map.hash_usize(&0u64), map.hash_usize(&u64::MAX));
}

// ── API methods ───────────────────────────────────────────────────────────────

#[test]
fn remove_if_mut_removes_when_predicate_true() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let removed = map.remove_if_mut(&1, |_k, v| {
        *v += 5;
        true
    });
    assert!(removed.is_some());
    assert!(map.get(&1).is_none());
}

#[test]
fn remove_if_mut_keeps_when_predicate_false() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let removed = map.remove_if_mut(&1, |_k, _v| false);
    assert!(removed.is_none());
    assert!(map.get(&1).is_some());
}

#[test]
fn remove_if_mut_absent_key_returns_none() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    assert!(map.remove_if_mut(&999, |_k, _v| true).is_none());
}

#[test]
fn try_get_present_absent() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let r = map.try_get(&1);
    assert!(r.is_present());
    assert!(!r.is_absent());
    assert!(!r.is_locked());
    assert_eq!(*r.unwrap(), 10);

    let r2 = map.try_get(&999u64);
    assert!(r2.is_absent());
    assert!(!r2.is_present());
    assert!(r2.try_unwrap().is_none());
}

#[test]
fn try_get_mut_present_absent() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let r = map.try_get_mut(&1);
    assert!(r.is_present());
    {
        let mut guard = r.unwrap();
        *guard += 5;
    }
    assert_eq!(*map.get(&1).unwrap(), 15);

    let r2 = map.try_get_mut(&999u64);
    assert!(r2.is_absent());
    assert!(r2.try_unwrap().is_none());
}

#[test]
fn try_entry_occupied_and_vacant() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    // Occupied branch
    match map.try_entry(1) {
        Some(Entry::Occupied(occ)) => {
            assert_eq!(*occ.get(), 10);
            let _ = occ.into_ref();
        }
        _ => panic!("expected Some(Occupied)"),
    }

    // Vacant branch
    match map.try_entry(999) {
        Some(Entry::Vacant(v)) => {
            v.insert(42);
        }
        _ => panic!("expected Some(Vacant)"),
    }
    assert_eq!(*map.get(&999).unwrap(), 42);
}

#[test]
fn shrink_to_fit_works() {
    let map: NeoCache<u64, u64> = NeoCache::new(1000);
    for i in 0..50u64 {
        map.insert(i, i);
    }
    for i in 0..25u64 {
        map.remove(&i);
    }
    map.shrink_to_fit();
    assert_eq!(map.len(), 25);
}

#[test]
fn try_reserve_succeeds() {
    let mut map: NeoCache<u64, u64> = NeoCache::new(100);
    assert!(map.try_reserve(10).is_ok());
    map.insert(1, 2);
    assert_eq!(*map.get(&1).unwrap(), 2);
}

#[test]
fn alter_modifies_existing_value() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);
    map.alter(&1, |_k, v| v * 2);
    assert_eq!(*map.get(&1).unwrap(), 20);
}

#[test]
fn alter_absent_key_is_noop() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.alter(&999, |_k, v| v + 1); // must not panic
}

#[test]
fn alter_all_updates_every_entry() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..10u64 {
        map.insert(i, i);
    }
    map.alter_all(|_k, v| v + 100);
    for i in 0..10u64 {
        assert_eq!(*map.get(&i).unwrap(), i + 100);
    }
}

#[test]
fn view_returns_computed_value_or_none() {
    let map: NeoCache<u64, String> = NeoCache::new(100);
    map.insert(1, "hello".to_string());

    assert_eq!(map.view(&1, |_k, v| v.len()), Some(5));
    assert_eq!(map.view(&999u64, |_k, v| v.len()), None);
}

#[test]
fn iter_mut_allows_in_place_mutation() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..5u64 {
        map.insert(i, i);
    }
    for mut r in map.iter_mut() {
        *r += 100;
    }
    for i in 0..5u64 {
        assert_eq!(*map.get(&i).unwrap(), i + 100);
    }
}

// ── Operators ─────────────────────────────────────────────────────────────────

#[test]
fn operator_shl_inserts() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    let old = &map << (1u64, 10u64);
    assert!(old.is_none());
    let old2 = &map << (1u64, 20u64);
    assert_eq!(old2, Some(10));
}

#[test]
fn operator_shr_get_unwrap() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 42);
    let r = &map >> &1u64;
    assert_eq!(*r, 42);
}

#[test]
fn operator_bitor_get_mut_unwrap() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);
    let mut r = &map | &1u64;
    *r += 5;
    drop(r);
    assert_eq!(*map.get(&1).unwrap(), 15);
}

#[test]
fn operator_sub_remove() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 99);
    let removed = &map - &1u64;
    assert_eq!(removed, Some((1, 99)));
    assert!(map.get(&1).is_none());

    let none = &map - &1u64;
    assert!(none.is_none());
}

#[test]
fn operator_bitand_contains_key() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 1);
    assert!(&map & &1u64);
    assert!(!(&map & &999u64));
}

// ── FromIterator and Extend ───────────────────────────────────────────────────

#[test]
fn from_iterator() {
    // FromIterator requires S: Default; use std RandomState which impls Default.
    let pairs: Vec<(u64, u64)> = (0..10u64).map(|i| (i, i * 2)).collect();
    let map: NeoCache<u64, u64, std::collections::hash_map::RandomState> =
        pairs.into_iter().collect();
    assert_eq!(map.len(), 10);
    assert_eq!(*map.get(&5).unwrap(), 10);
}

#[test]
fn extend_map() {
    let mut map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(0, 0);
    map.extend(vec![(1u64, 10u64), (2u64, 20u64)]);
    assert_eq!(map.len(), 3);
    assert_eq!(*map.get(&1).unwrap(), 10);
    assert_eq!(*map.get(&2).unwrap(), 20);
}

// ── Debug ─────────────────────────────────────────────────────────────────────

#[test]
fn debug_format_contains_values() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);
    let s = format!("{:?}", map);
    assert!(s.contains("10"));
}

// ── ReadOnlyView ─────────────────────────────────────────────────────────────

#[test]
fn read_only_view_all_methods() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..5u64 {
        map.insert(i, i * 10);
    }

    let view: ReadOnlyView<u64, u64> = map.into_read_only();

    assert_eq!(view.len(), 5);
    assert!(!view.is_empty());
    let _ = view.capacity();

    assert!(view.contains_key(&0));
    assert!(!view.contains_key(&999));

    assert_eq!(*view.get(&1).unwrap(), 10);
    assert!(view.get(&999).is_none());

    let (k, v) = view.get_key_value(&2).unwrap();
    assert_eq!(*k, 2);
    assert_eq!(*v, 20);
    assert!(view.get_key_value(&999).is_none());

    assert_eq!(view.iter().count(), 5);
    assert_eq!(view.keys().count(), 5);
    assert_eq!(view.values().sum::<u64>(), 0 + 10 + 20 + 30 + 40);

    let recovered: NeoCache<u64, u64> = view.into_inner();
    assert_eq!(recovered.len(), 5);
}

#[test]
fn read_only_view_empty() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    let view = map.into_read_only();
    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
    assert_eq!(view.iter().count(), 0);
}

#[test]
fn read_only_view_clone() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 100);
    let view = map.into_read_only();
    let view2 = view.clone();
    assert_eq!(view2.len(), 1);
    assert_eq!(*view2.get(&1).unwrap(), 100);
}

#[test]
fn read_only_view_debug() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);
    let view = map.into_read_only();
    let s = format!("{:?}", view);
    assert!(s.contains("10"));
}

// ── Ref methods ───────────────────────────────────────────────────────────────

#[test]
fn ref_key_value_pair_debug() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(42, 100);

    let r = map.get(&42).unwrap();
    assert_eq!(*r.key(), 42);
    assert_eq!(*r.value(), 100);
    let (k, v) = r.pair();
    assert_eq!(*k, 42);
    assert_eq!(*v, 100);

    let s = format!("{:?}", r);
    assert!(s.contains("42"));
}

#[test]
fn ref_map_projects_to_sub_value() {
    // map() requires F: FnOnce(&V) -> &T where T: Sized.
    let map: NeoCache<u64, (String, u32)> = NeoCache::new(100);
    map.insert(1, ("hello".to_string(), 7u32));

    let r = map.get(&1).unwrap();
    // Project from &(String, u32) to &u32.
    let mapped = r.map(|t: &(String, u32)| &t.1);
    assert_eq!(*mapped.value(), 7u32);
    assert_eq!(*mapped.key(), 1);
    let (k, v) = mapped.pair();
    assert_eq!(*k, 1);
    assert_eq!(*v, 7u32);
}

#[test]
fn ref_try_map_success() {
    let map: NeoCache<u64, Option<u32>> = NeoCache::new(100);
    map.insert(1, Some(42));

    let r = map.get(&1).unwrap();
    let result = r.try_map(|opt| opt.as_ref());
    assert!(result.is_ok());
    assert_eq!(*result.unwrap(), 42);
}

#[test]
fn ref_try_map_failure_returns_original() {
    let map: NeoCache<u64, Option<u32>> = NeoCache::new(100);
    map.insert(1, None);

    let r = map.get(&1).unwrap();
    let result = r.try_map(|opt| opt.as_ref());
    assert!(result.is_err());
    let original = result.err().unwrap();
    assert_eq!(*original.key(), 1);
}

// ── RefMut methods ────────────────────────────────────────────────────────────

#[test]
fn refmut_key_value_value_mut_pair_pair_mut_debug() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let mut r = map.get_mut(&1).unwrap();
    assert_eq!(*r.key(), 1);
    assert_eq!(*r.value(), 10);

    *r.value_mut() += 5;

    let (k, v) = r.pair();
    assert_eq!(*k, 1);
    assert_eq!(*v, 15);

    let (_k, v2) = r.pair_mut();
    *v2 += 1;

    let s = format!("{:?}", r);
    assert!(s.contains("1"));
}

#[test]
fn refmut_downgrade_to_ref() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 10);

    let r = map.get_mut(&1).unwrap();
    let r_read = r.downgrade();
    assert_eq!(*r_read, 10);
    assert_eq!(*r_read.key(), 1);
}

#[test]
fn refmut_map_projects_mutably() {
    let map: NeoCache<u64, Vec<u64>> = NeoCache::new(100);
    map.insert(1, vec![1, 2, 3]);

    let r = map.get_mut(&1).unwrap();
    let mut mapped = r.map(|v| &mut v[0]);
    *mapped += 100;
    drop(mapped);

    assert_eq!((*map.get(&1).unwrap())[0], 101);
}

#[test]
fn refmut_try_map_success() {
    let map: NeoCache<u64, Option<u32>> = NeoCache::new(100);
    map.insert(1, Some(42));

    let r = map.get_mut(&1).unwrap();
    let result = r.try_map(|opt| opt.as_mut());
    assert!(result.is_ok());
    let mut mapped = result.unwrap();
    *mapped += 1;
    drop(mapped);

    assert_eq!(*map.get(&1).unwrap(), Some(43));
}

#[test]
fn refmut_try_map_failure_returns_original() {
    let map: NeoCache<u64, Option<u32>> = NeoCache::new(100);
    map.insert(1, None);

    let r = map.get_mut(&1).unwrap();
    let result = r.try_map(|opt| opt.as_mut());
    assert!(result.is_err());
    let original = result.err().unwrap();
    assert_eq!(*original.key(), 1);
}

// ── MappedRef ─────────────────────────────────────────────────────────────────

#[test]
fn mapped_ref_key_value_pair_debug() {
    // key/value/pair/Debug on MappedRef<K, u32, u32>
    let map: NeoCache<u64, u32> = NeoCache::new(100);
    map.insert(1, 42u32);

    let r = map.get(&1).unwrap();
    let mapped = r.map(|n| n); // MappedRef<K, u32, u32>

    assert_eq!(*mapped.key(), 1);
    assert_eq!(*mapped.value(), 42u32);
    let (k, v) = mapped.pair();
    assert_eq!(*k, 1);
    assert_eq!(*v, 42u32);

    let debug_str = format!("{:?}", mapped);
    assert!(debug_str.contains("42"));
}

#[test]
fn mapped_ref_display() {
    // Display: T must implement Display. Use u64.
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1, 99u64);
    let r = map.get(&1).unwrap();
    let mapped = r.map(|n| n);
    let disp = format!("{}", mapped);
    assert_eq!(disp, "99");
}

#[test]
fn mapped_ref_as_ref() {
    // AsRef: use String which implements AsRef<str>.
    let map: NeoCache<u64, String> = NeoCache::new(100);
    map.insert(1, "hello".to_string());
    let r = map.get(&1).unwrap();
    // Project &String → &String (identity), T = String : AsRef<str>
    let mapped: neocache::mapref::one::MappedRef<'_, u64, String, String> =
        r.map(|s: &String| s);
    let as_str: &str = mapped.as_ref();
    assert_eq!(as_str, "hello");
}

#[test]
fn mapped_ref_map_chain() {
    // Chain: MappedRef<K, (String, u32), String> → MappedRef<K, (String, u32), String>
    let map: NeoCache<u64, (String, u32)> = NeoCache::new(100);
    map.insert(1, ("hello".to_string(), 99u32));

    let r = map.get(&1).unwrap();
    let m1 = r.map(|t: &(String, u32)| &t.0);   // → &String
    let m2 = m1.map(|s: &String| s);             // identity chain
    assert_eq!(*m2.value(), "hello");
}

#[test]
fn mapped_ref_try_map_success_and_failure() {
    let map: NeoCache<u64, Option<u32>> = NeoCache::new(100);
    map.insert(1, Some(42u32));
    map.insert(2, None);

    // success: project to inner u32
    let r = map.get(&1).unwrap();
    let m = r.map(|opt| opt); // MappedRef<K, Option<u32>, Option<u32>>
    let ok = m.try_map(|opt: &Option<u32>| opt.as_ref());
    assert!(ok.is_ok());
    assert_eq!(*ok.unwrap(), 42u32);

    // failure: None → returns Err(original)
    let r2 = map.get(&2).unwrap();
    let m2 = r2.map(|opt| opt);
    let err = m2.try_map(|opt: &Option<u32>| opt.as_ref());
    assert!(err.is_err());
    assert_eq!(*err.err().unwrap().key(), 2);
}

// ── MappedRefMut ──────────────────────────────────────────────────────────────

#[test]
fn mapped_refmut_all_methods() {
    let map: NeoCache<u64, Vec<u64>> = NeoCache::new(100);
    map.insert(1, vec![10, 20, 30]);

    let r = map.get_mut(&1).unwrap();
    let mut mapped = r.map(|v| &mut v[0]);

    assert_eq!(*mapped.key(), 1);
    assert_eq!(*mapped.value(), 10);
    let (k, v) = mapped.pair();
    assert_eq!(*k, 1);
    assert_eq!(*v, 10);

    *mapped.value_mut() = 999;
    let (_k2, v2) = mapped.pair_mut();
    *v2 += 1;

    // Deref
    assert_eq!(*mapped, 1000);
    // DerefMut
    *mapped += 0;

    let dbg = format!("{:?}", mapped);
    assert!(dbg.contains("1000"));

    drop(mapped);
    assert_eq!((*map.get(&1).unwrap())[0], 1000);
}

#[test]
fn mapped_refmut_map_chain() {
    // map() chain: MappedRefMut<K, Vec<u64>, u64> → MappedRefMut<K, Vec<u64>, u64>
    // Keep T = u64 (Sized) throughout — don't use slices ([T] is !Sized).
    let map: NeoCache<u64, Vec<u64>> = NeoCache::new(100);
    map.insert(1, vec![10, 20]);

    let r = map.get_mut(&1).unwrap();
    // First map: project Vec<u64> → first element (u64)
    let m1 = r.map(|v: &mut Vec<u64>| &mut v[0]);
    // Chain map: identity
    let mut m2 = m1.map(|n: &mut u64| n);
    *m2 = 999;
    drop(m2);

    assert_eq!((*map.get(&1).unwrap())[0], 999);
}

#[test]
fn mapped_refmut_try_map_success_and_failure() {
    // Use Option<u64> as value; project to inner &mut u64.
    let map: NeoCache<u64, Option<u64>> = NeoCache::new(100);
    map.insert(1, Some(10u64));
    map.insert(2, None);

    // success: Some → &mut u64
    let r = map.get_mut(&1).unwrap();
    let m = r.map(|opt: &mut Option<u64>| opt); // MappedRefMut<K, Option<u64>, Option<u64>>
    let ok = m.try_map(|opt: &mut Option<u64>| opt.as_mut());
    assert!(ok.is_ok());
    drop(ok);

    // failure: None → Err(original)
    let r2 = map.get_mut(&2).unwrap();
    let m2 = r2.map(|opt: &mut Option<u64>| opt);
    let err = m2.try_map(|opt: &mut Option<u64>| opt.as_mut());
    assert!(err.is_err());
    assert_eq!(*err.err().unwrap().key(), 2);
}

// ── RefMulti / RefMutMulti ────────────────────────────────────────────────────

#[test]
fn refmulti_key_value_pair_deref() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..5u64 {
        map.insert(i, i * 10);
    }

    for r in map.iter() {
        let k = *r.key();
        let v = *r.value();
        let (k2, v2) = r.pair();
        assert_eq!(k, *k2);
        assert_eq!(v, *v2);
        // Deref
        assert_eq!(*r, k * 10);
    }
}

#[test]
fn refmutmulti_key_value_pair_pair_mut_value_mut_deref_derefmut() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..5u64 {
        map.insert(i, i * 10);
    }

    for mut r in map.iter_mut() {
        let k = *r.key();
        let v = *r.value();
        assert_eq!(v, k * 10);

        let (k2, v2) = r.pair();
        assert_eq!(*k2, k);
        assert_eq!(*v2, v);

        let (_k3, v3) = r.pair_mut();
        *v3 += 1;

        *r.value_mut() += 1;

        // Deref
        assert_eq!(*r, k * 10 + 2);
        // DerefMut (no-op but exercises the impl)
        *r += 0;
    }

    // verify mutations applied
    for i in 0..5u64 {
        assert_eq!(*map.get(&i).unwrap(), i * 10 + 2);
    }
}

// ── TryResult ─────────────────────────────────────────────────────────────────

#[test]
fn try_result_all_variants_and_methods() {
    // Present
    let present: TryResult<u32> = TryResult::Present(42);
    assert!(present.is_present());
    assert!(!present.is_absent());
    assert!(!present.is_locked());
    assert_eq!(present.unwrap(), 42);

    let present2: TryResult<u32> = TryResult::Present(99);
    assert_eq!(present2.try_unwrap(), Some(99));

    // Absent
    let absent: TryResult<u32> = TryResult::Absent;
    assert!(!absent.is_present());
    assert!(absent.is_absent());
    assert!(!absent.is_locked());
    assert!(absent.try_unwrap().is_none());

    // Locked
    let locked: TryResult<u32> = TryResult::Locked;
    assert!(!locked.is_present());
    assert!(!locked.is_absent());
    assert!(locked.is_locked());
    assert!(locked.try_unwrap().is_none());
}

#[test]
#[should_panic(expected = "TryResult::Locked")]
fn try_result_unwrap_locked_panics() {
    let _: u32 = TryResult::Locked.unwrap();
}

#[test]
#[should_panic(expected = "TryResult::Absent")]
fn try_result_unwrap_absent_panics() {
    let _: u32 = TryResult::Absent.unwrap();
}

// ── Entry API ─────────────────────────────────────────────────────────────────

#[test]
fn entry_key_occupied() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("hello".to_string(), 1);
    let entry = map.entry("hello".to_string());
    assert_eq!(entry.key(), "hello");
    let k = entry.into_key();
    assert_eq!(k, "hello");
}

#[test]
fn entry_key_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let entry = map.entry("world".to_string());
    assert_eq!(entry.key(), "world");
    let k = entry.into_key();
    assert_eq!(k, "world");
}

#[test]
fn entry_or_default_vacant_inserts_default() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let r = map.entry("k".to_string()).or_default();
    assert_eq!(*r, 0u64);
    drop(r);
    assert_eq!(*map.get("k").unwrap(), 0);
}

#[test]
fn entry_or_default_occupied_returns_existing() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 42);
    let r = map.entry("k".to_string()).or_default();
    assert_eq!(*r, 42);
}

#[test]
fn entry_or_insert_with_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let r = map.entry("k".to_string()).or_insert_with(|| 99);
    assert_eq!(*r, 99);
}

#[test]
fn entry_or_insert_with_occupied() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 42);
    let r = map.entry("k".to_string()).or_insert_with(|| 99);
    assert_eq!(*r, 42); // existing value
}

#[test]
fn entry_or_try_insert_with_ok_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let result: Result<_, &str> = map.entry("k".to_string()).or_try_insert_with(|| Ok(77));
    assert!(result.is_ok());
    assert_eq!(*result.unwrap(), 77);
}

#[test]
fn entry_or_try_insert_with_err_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let result: Result<_, &str> = map
        .entry("k".to_string())
        .or_try_insert_with(|| Err("oops"));
    assert_eq!(result.err(), Some("oops"));
    // Key was not inserted
    assert!(map.get("k").is_none());
}

#[test]
fn entry_or_try_insert_with_ok_occupied() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 42);
    let result: Result<_, &str> = map.entry("k".to_string()).or_try_insert_with(|| Ok(77));
    assert_eq!(*result.unwrap(), 42);
}

#[test]
fn entry_insert_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let r = map.entry("k".to_string()).insert(55);
    assert_eq!(*r, 55);
    drop(r);
    assert_eq!(*map.get("k").unwrap(), 55);
}

#[test]
fn entry_insert_occupied() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 10);
    let r = map.entry("k".to_string()).insert(55);
    assert_eq!(*r, 55);
    drop(r);
    assert_eq!(*map.get("k").unwrap(), 55);
}

#[test]
fn entry_insert_entry_vacant() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    let occ = map.entry("k".to_string()).insert_entry(77);
    assert_eq!(*occ.get(), 77);
    assert_eq!(occ.key(), "k");
}

#[test]
fn entry_insert_entry_occupied() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 10);
    let occ = map.entry("k".to_string()).insert_entry(77);
    assert_eq!(*occ.get(), 77);
    assert_eq!(occ.key(), "k");
}

#[test]
fn vacant_entry_key_and_into_key() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    if let Entry::Vacant(v) = map.entry("hello".to_string()) {
        assert_eq!(v.key(), "hello");
        let k = v.into_key();
        assert_eq!(k, "hello");
    } else {
        panic!("expected Vacant");
    }
}

#[test]
fn vacant_entry_insert_entry() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    if let Entry::Vacant(v) = map.entry("k".to_string()) {
        let occ = v.insert_entry(42);
        assert_eq!(*occ.get(), 42);
        assert_eq!(occ.key(), "k");
    } else {
        panic!("expected Vacant");
    }
}

#[test]
fn occupied_entry_get_get_mut_insert_into_ref() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 10);

    if let Entry::Occupied(mut occ) = map.entry("k".to_string()) {
        assert_eq!(*occ.get(), 10);
        assert_eq!(occ.key(), "k");

        *occ.get_mut() += 5;
        assert_eq!(*occ.get(), 15);

        let old = occ.insert(100);
        assert_eq!(old, 15);
        assert_eq!(*occ.get(), 100);

        let r = occ.into_ref();
        assert_eq!(*r, 100);
    } else {
        panic!("expected Occupied");
    }
}

#[test]
fn occupied_entry_into_key() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 10);

    if let Entry::Occupied(occ) = map.entry("k".to_string()) {
        let k = occ.into_key();
        assert_eq!(k, "k");
    } else {
        panic!("expected Occupied");
    }
}

#[test]
fn occupied_entry_remove() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 200);

    if let Entry::Occupied(occ) = map.entry("k".to_string()) {
        let val = occ.remove();
        assert_eq!(val, 200);
    } else {
        panic!("expected Occupied");
    }
    assert!(map.get("k").is_none());
}

#[test]
fn occupied_entry_remove_entry() {
    let map: NeoCache<String, u64> = NeoCache::new(100);
    map.insert("k".to_string(), 300);

    if let Entry::Occupied(occ) = map.entry("k".to_string()) {
        let (k, v) = occ.remove_entry();
        assert_eq!(k, "k");
        assert_eq!(v, 300);
    } else {
        panic!("expected Occupied");
    }
    assert!(map.get("k").is_none());
}

/// Test OccupiedEntry::remove on an entry that was promoted to the Main queue
/// (loc == LOC_MAIN), so the main_live branch in remove() is exercised.
#[test]
fn occupied_entry_remove_main_loc_entry() {
    // Small capacity: shard_cap=4, small_cap=1, main_cap=3; 2 shards
    let map: NeoCache<u64, u64> = NeoCache::with_shard_amount(8, 2);

    // Insert hot entries with high freq to get them promoted to main
    for i in 0..8u64 {
        map.insert(i, i);
        let _ = map.get(&i);
        let _ = map.get(&i);
    }
    // Trigger eviction to promote entries from small → main
    for i in 8..50u64 {
        map.insert(i, i);
    }

    // Now try to remove a key via OccupiedEntry — if it's still present,
    // it may be in main (loc == LOC_MAIN); the live-count branch will fire.
    if let Some(entry) = map.try_entry(0) {
        if let Entry::Occupied(occ) = entry {
            let _ = occ.remove();
        }
    }
}

// ── evict_from_main path ──────────────────────────────────────────────────────

/// Forces `evict_from_main` second-chance (freq>0 → decrement+re-enqueue)
/// and eviction (freq==0 → remove) branches.
///
/// Strategy:
///   shard_cap=4, small_cap=1, main_cap=3 (2 shards, total cap=8).
///   Phase 1: insert 8 hot entries with freq=3 → they promote small→main.
///   Phase 2: insert 200 cold entries → main fills, evict_from_main fires.
#[test]
fn evict_from_main_second_chance_and_eviction() {
    let map: NeoCache<u64, u64> = NeoCache::with_shard_amount(8, 2);

    for i in 0..8u64 {
        map.insert(i, i);
        let _ = map.get(&i);
        let _ = map.get(&i);
        let _ = map.get(&i);
    }

    for i in 8..300u64 {
        map.insert(i, i);
    }

    assert!(map.len() <= 10, "len = {}", map.len());
}

/// Test that stale entries in the main queue (from explicit `remove()`) are
/// correctly skipped by the eviction sweep.
#[test]
fn evict_from_main_skips_stale_entries() {
    let map: NeoCache<u64, u64> = NeoCache::with_shard_amount(8, 2);

    for i in 0..8u64 {
        map.insert(i, i);
        let _ = map.get(&i);
    }

    // Remove half — creates stale queue tuples
    for i in 0..4u64 {
        map.remove(&i);
    }

    for i in 8..200u64 {
        map.insert(i, i);
    }

    assert!(map.len() <= 10, "len = {}", map.len());
}

/// Exercises `evict_from_main` when `small_live < small_cap` so the `else`
/// branch in `evict_one` directly calls `evict_from_main`.
#[test]
fn evict_one_falls_through_to_evict_from_main_when_small_empty() {
    // shard_cap=10, small_cap=1, main_cap=9; 2 shards, total=20
    let map: NeoCache<u64, u64> = NeoCache::with_shard_amount(20, 2);

    // Flood with hot entries; they promote small→main quickly
    for i in 0..20u64 {
        map.insert(i, i);
        let _ = map.get(&i);
        let _ = map.get(&i);
        let _ = map.get(&i);
    }

    // Many cold entries to keep triggering eviction
    for i in 20..500u64 {
        map.insert(i, i);
    }

    assert!(map.len() <= 22, "len = {}", map.len());
}

// ── Ghost hit path (entry goes directly to Main on re-insert) ─────────────────

#[test]
fn ghost_hit_promotes_to_main_on_reinsertion() {
    // shard_cap=2, small_cap=1, main_cap=1; 2 shards, total cap=4
    let map: NeoCache<u64, u64> = NeoCache::with_shard_amount(4, 2);

    // Flood to evict key 0 into the ghost set
    for i in 0..30u64 {
        map.insert(i, i);
    }

    // Re-insert key 0: ghost hit → LOC_MAIN path in VacantEntry::insert
    map.insert(0, 999);

    assert!(map.len() <= 6);
}

// ── Map trait provided methods (_clear, _is_empty) ───────────────────────────

#[test]
fn map_trait_is_empty() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    // _is_empty is exercised by NeoCache::is_empty()
    assert!(map.is_empty());
    map.insert(1, 1);
    assert!(!map.is_empty());
}

#[test]
fn map_trait_clear_via_retain() {
    // The Map trait's `_clear` calls `_retain(|_, _| false)`.
    // NeoCache::clear() calls clear_all() directly, so we invoke _clear()
    // through the Map trait to hit that code path.
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..10u64 {
        map.insert(i, i);
    }
    assert_eq!(map.len(), 10);
    map._clear();
    assert_eq!(map.len(), 0);
}

// ── Iter::clone ───────────────────────────────────────────────────────────────

#[test]
fn iter_clone_independent_from_original() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..5u64 {
        map.insert(i, i);
    }

    let iter1 = map.iter();
    let iter2 = iter1.clone();

    assert_eq!(iter1.count(), 5);
    assert_eq!(iter2.count(), 5);
}

// ── ShardData::default via NeoCache::default ──────────────────────────────────

#[test]
fn shard_data_default_via_neocache_default() {
    // NeoCache::default() creates shards via ShardData::default().
    // ahash::RandomState doesn't impl Default, so use std's RandomState.
    let map: NeoCache<u64, u64, std::collections::hash_map::RandomState> = NeoCache::default();
    assert_eq!(map.cache_capacity(), 0);
    map.insert(1, 1);
    assert_eq!(*map.get(&1).unwrap(), 1);
}

// ── entry.rs:26 — and_modify on a Vacant entry ────────────────────────────────

#[test]
fn and_modify_vacant_branch() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    // Key 999 does not exist; and_modify should pass through as Vacant.
    let entry = map.entry(999u64).and_modify(|v| *v += 1);
    assert!(matches!(entry, Entry::Vacant(_)));
    drop(entry);
}

// ── lib.rs:505 — _remove_if predicate returns false ──────────────────────────

#[test]
fn remove_if_predicate_false() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    map.insert(1u64, 10u64);
    // Key exists but predicate is false → inner None at lib.rs:505.
    let result = map.remove_if(&1u64, |_, &v| v > 100);
    assert!(result.is_none());
    // Entry still present.
    assert!(map.contains_key(&1u64));
}

// ── lib.rs:731-733 — Map trait _hasher() ─────────────────────────────────────

#[test]
fn map_trait_hasher_method() {
    // Map is pub-exported; calling _hasher() hits lib.rs:731-733.
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    let _h = map._hasher();
}

// ── Public API surface: methods not yet directly called ───────────────────────

// ── lib.rs:134-136 — NeoCache::new_unbounded()
#[test]
fn new_unbounded_grows_without_limit() {
    let map: NeoCache<u64, u64> = NeoCache::new_unbounded();
    for i in 0..1000u64 {
        map.insert(i, i);
    }
    assert_eq!(map.len(), 1000);
    assert_eq!(map.cache_capacity(), 0);
}

// ── lib.rs:320-322 — NeoCache::retain() (inherent method, not Map::_retain)
#[test]
fn retain_keeps_matching_entries() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..10u64 {
        map.insert(i, i);
    }
    map.retain(|k, _v| k % 2 == 0);
    assert_eq!(map.len(), 5);
    assert!(map.contains_key(&0u64));
    assert!(!map.contains_key(&1u64));
    assert!(map.contains_key(&8u64));
    assert!(!map.contains_key(&9u64));
}

// ── lib.rs:325-329 — NeoCache::clear() (inherent method, not Map::_clear / clear_all)
#[test]
fn clear_empties_the_map() {
    let map: NeoCache<u64, u64> = NeoCache::new(100);
    for i in 0..10u64 {
        map.insert(i, i);
    }
    assert_eq!(map.len(), 10);
    map.clear();
    assert_eq!(map.len(), 0);
    assert!(map.is_empty());
}

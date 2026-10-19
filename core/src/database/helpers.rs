//! Low-level routines over sorted slices of weighted tuples.
use super::instance::{Batch, Keyed};
use crate::Tuple;
use std::rc::Rc;

/// Moves an ordered `slice` forward until `cmp` is false on its first element.
///
/// **Note**: `gallop` is directly borrowed from [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[inline(always)]
pub(crate) fn gallop<T>(mut slice: &[T], mut cmp: impl FnMut(&T) -> bool) -> &[T] {
    if !slice.is_empty() && cmp(&slice[0]) {
        let mut step = 1;
        while step < slice.len() && cmp(&slice[step]) {
            slice = &slice[step..];
            step <<= 1;
        }

        step >>= 1;
        while step > 0 {
            if step < slice.len() && cmp(&slice[step]) {
                slice = &slice[step..];
            }
            step >>= 1;
        }

        slice = &slice[1..];
    }
    slice
}

/// Adds the weight that `batch` assigns to each of the sorted `keys` to `counts`.
pub(crate) fn add_counts<T: Tuple>(batch: &Batch<T>, keys: &[T], counts: &mut [isize]) {
    let mut items = batch.items();
    for (i, key) in keys.iter().enumerate() {
        items = gallop(items, |x| x.0 < *key);
        if let Some((t, w)) = items.first() {
            if t == key {
                counts[i] += *w;
            }
        }
    }
}

/// Returns the total weight of each of the sorted `keys` over all `batches`.
pub(crate) fn counts_in<T: Tuple>(batches: &[Rc<Batch<T>>], keys: &[T]) -> Vec<isize> {
    let mut counts = vec![0; keys.len()];
    for batch in batches {
        add_counts(batch, keys, &mut counts);
    }
    counts
}

/// Applies `result` on every pair of `left` and `right` with the product of their weights.
#[inline(always)]
pub(crate) fn product_helper<L, R>(
    left: &[(L, isize)],
    right: &[(R, isize)],
    mut result: impl FnMut(&L, &R, isize),
) {
    for (l, wl) in left {
        for (r, wr) in right {
            result(l, r, wl * wr);
        }
    }
}

/// For two keyed batches, applies `result` on the pairs of tuples that agree on their key,
/// with the product of their weights.
///
/// **Note**: `join_helper` is adapted from [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[inline(always)]
pub(crate) fn join_helper<K: Ord, L, R>(
    left: &Keyed<'_, K, L>,
    right: &Keyed<'_, K, R>,
    mut result: impl FnMut(&K, &L, &R, isize),
) {
    let mut slice1 = left.items();
    let mut slice2 = right.items();

    while !slice1.is_empty() && !slice2.is_empty() {
        use std::cmp::Ordering;

        match slice1[0].0.cmp(&slice2[0].0) {
            Ordering::Less => slice1 = gallop(slice1, |x| x.0 < slice2[0].0),
            Ordering::Equal => {
                let count1 = slice1.iter().take_while(|x| x.0 == slice1[0].0).count();
                let count2 = slice2.iter().take_while(|x| x.0 == slice2[0].0).count();

                for item1 in slice1.iter().take(count1) {
                    for item2 in slice2.iter().take(count2) {
                        result(&item1.0, item1.1, item2.1, item1.2 * item2.2);
                    }
                }

                slice1 = &slice1[count1..];
                slice2 = &slice2[count2..];
            }
            Ordering::Greater => slice2 = gallop(slice2, |x| x.0 < slice1[0].0),
        }
    }
}

/// For two sorted weighted slices, applies `result` on the common tuples with the product of
/// their weights.
#[inline(always)]
pub(crate) fn intersect_helper<T: Ord>(
    left: &[(T, isize)],
    right: &[(T, isize)],
    mut result: impl FnMut(&T, isize),
) {
    let mut left = left;
    let mut right = right;

    while !left.is_empty() && !right.is_empty() {
        use std::cmp::Ordering;

        match left[0].0.cmp(&right[0].0) {
            Ordering::Less => left = gallop(left, |x| x.0 < right[0].0),
            Ordering::Equal => {
                result(&left[0].0, left[0].1 * right[0].1);
                left = &left[1..];
                right = &right[1..];
            }
            Ordering::Greater => right = gallop(right, |x| x.0 < left[0].0),
        }
    }
}

/// Merges two sorted lists of tuples into one sorted list without duplicates.
pub(crate) fn merge_keys<T: Tuple>(left: &[T], right: &[T]) -> Vec<T> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        use std::cmp::Ordering;
        match left[i].cmp(&right[j]) {
            Ordering::Less => {
                result.push(left[i].clone());
                i += 1;
            }
            Ordering::Greater => {
                result.push(right[j].clone());
                j += 1;
            }
            Ordering::Equal => {
                result.push(left[i].clone());
                i += 1;
                j += 1;
            }
        }
    }
    result.extend(left[i..].iter().cloned());
    result.extend(right[j..].iter().cloned());
    result
}

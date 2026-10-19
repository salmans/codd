/// Moves an ordered `slice` forward until `cmp` is true on the elements of `slice`.
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

/// Applies `result` on elements of `slice`.
#[inline(always)]
pub(crate) fn project_helper<T>(slice: &[T], mut result: impl FnMut(&T)) {
    let slice = &slice[..];
    for tuple in slice {
        result(tuple);
    }
}

/// Applies `result` on every pair of `left` and `right` slices.
#[inline(always)]
pub(crate) fn product_helper<L, R>(left: &[L], right: &[R], mut result: impl FnMut(&L, &R)) {
    let left = &left[..];
    let right = &right[..];

    for l in left {
        for r in right {
            result(&l, &r);
        }
    }
}

/// For two slices `left` and `right` that are sorted by the first element of their tuples,
/// applies `result` on those pairs of `left` and `right` that agree on their first
/// element as the key.
///
/// **Note**: `join_helper` is directly borrowed from [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[inline(always)]
pub(crate) fn join_helper<Key: Ord, L, R>(
    left: &[(Key, L)],
    right: &[(Key, R)],
    mut result: impl FnMut(&Key, &L, &R),
) {
    let mut slice1 = &left[..];
    let mut slice2 = &right[..];

    while !slice1.is_empty() && !slice2.is_empty() {
        use std::cmp::Ordering;

        match slice1[0].0.cmp(&slice2[0].0) {
            Ordering::Less => slice1 = gallop(slice1, |x| x.0 < slice2[0].0),
            Ordering::Equal => {
                let count1 = slice1.iter().take_while(|x| x.0 == slice1[0].0).count();
                let count2 = slice2.iter().take_while(|x| x.0 == slice2[0].0).count();

                for index1 in 0..count1 {
                    for item in slice2.iter().take(count2) {
                        result(&slice1[0].0, &slice1[index1].1, &item.1);
                    }
                }

                slice1 = &slice1[count1..];
                slice2 = &slice2[count2..];
            }
            Ordering::Greater => slice2 = gallop(slice2, |x| x.0 < slice1[0].0),
        }
    }
}

/// For two sorted slices `left` and `right`, applies `result` on those elements of `left` and `right`
/// that are equal.
#[inline(always)]
pub(crate) fn intersect_helper<T: Ord>(left: &[T], right: &[T], mut result: impl FnMut(&T)) {
    let mut left = &left[..];
    let mut right = &right[..];

    while !left.is_empty() && !right.is_empty() {
        use std::cmp::Ordering;

        match left[0].cmp(&right[0]) {
            Ordering::Less => left = gallop(left, |x| x < &right[0]),
            Ordering::Equal => {
                result(&left[0]);
                left = &left[1..];
                right = &right[1..];
            }
            Ordering::Greater => right = gallop(right, |x| x < &left[0]),
        }
    }
}

/// For two sorted slices `left` and `right`, applies `result` on those elements of `left` that appear
/// in none of the slices of `right`.
#[inline(always)]
pub(crate) fn diff_helper<T: Ord>(left: &[T], right: &[&[T]], mut result: impl FnMut(&T)) {
    let left = &left[..];
    let mut right = right.iter().map(|sl| &sl[..]).collect::<Vec<&[T]>>();

    for tuple in left {
        let mut to_add = true;
        for mut to_find in &mut right {
            use std::cmp::Ordering;

            if !to_find.is_empty() {
                match tuple.cmp(&to_find[0]) {
                    Ordering::Less => {}
                    Ordering::Equal => {
                        to_add = false;
                    }
                    Ordering::Greater => {
                        let mut temp = gallop(to_find, |x| x < tuple);
                        to_find = &mut temp;
                        if !to_find.is_empty() && tuple == &to_find[0] {
                            to_add = false;
                        }
                    }
                }
            }
        }

        if to_add {
            result(tuple);
        }
    }
}

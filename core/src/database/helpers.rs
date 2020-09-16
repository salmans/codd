use crate::{database::Tuples, Tuple};

pub(crate) fn gallop<T>(mut slice: &[T], mut cmp: impl FnMut(&T) -> bool) -> &[T] {
    if slice.len() > 0 && cmp(&slice[0]) {
        let mut step = 1;
        while step < slice.len() && cmp(&slice[step]) {
            slice = &slice[step..];
            step = step << 1;
        }

        step = step >> 1;
        while step > 0 {
            if step < slice.len() && cmp(&slice[step]) {
                slice = &slice[step..];
            }
            step = step >> 1;
        }

        slice = &slice[1..];
    }
    slice
}

pub(crate) fn project_helper<T: Tuple>(tuples: &Tuples<T>, mut result: impl FnMut(&T)) {
    let slice = &tuples[..];
    for tuple in slice {
        result(tuple);
    }
}

pub(crate) fn product_helper<L: Tuple, R: Tuple>(
    left: &Tuples<L>,
    right: &Tuples<R>,
    mut result: impl FnMut(&L, &R),
) {
    let left = &left[..];
    let right = &right[..];

    for l in left {
        for r in right {
            result(&l, &r);
        }
    }
}

pub(crate) fn join_helper<Key: Tuple, L: Tuple, R: Tuple>(
    left: &Tuples<(Key, L)>,
    right: &Tuples<(Key, R)>,
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
                    for index2 in 0..count2 {
                        result(&slice1[0].0, &slice1[index1].1, &slice2[index2].1);
                    }
                }

                slice1 = &slice1[count1..];
                slice2 = &slice2[count2..];
            }
            Ordering::Greater => slice2 = gallop(slice2, |x| x.0 < slice1[0].0),
        }
    }
}

pub(crate) fn intersect_helper<T: Tuple>(left: &Tuples<T>, right: &Tuples<T>, result: &mut Vec<T>) {
    let mut left = &left[..];
    let mut right = &right[..];

    while !left.is_empty() && !right.is_empty() {
        use std::cmp::Ordering;

        match left[0].cmp(&right[0]) {
            Ordering::Less => left = gallop(left, |x| x < &right[0]),
            Ordering::Equal => {
                result.push(left[0].clone());
                left = &left[1..];
                right = &right[1..];
            }
            Ordering::Greater => right = gallop(right, |x| x < &left[0]),
        }
    }
}

pub(crate) fn diff_helper<T: Tuple>(left: &Tuples<T>, right: &Vec<Tuples<T>>, result: &mut Vec<T>) {
    let left = &left[..];
    let mut right = right.iter().map(|sl| &sl[..]).collect::<Vec<&[T]>>();

    for tuple in left {
        let mut add = true;
        for i in 0..right.len() {
            use std::cmp::Ordering;

            if right[i].is_empty() {
                continue;
            }

            match tuple.cmp(&right[i][0]) {
                Ordering::Less => {}
                Ordering::Equal => {
                    right[i] = &right[i][1..];
                    add = false;
                }
                Ordering::Greater => {
                    right[i] = &gallop(right[i], |x| x < &tuple);
                }
            }
        }

        if add {
            result.push(tuple.clone());
        }
    }
}

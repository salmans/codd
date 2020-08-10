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

pub(crate) fn project_helper<T: Tuple>(input: &Tuples<T>, mut result: impl FnMut(&T)) {
    let slice = &input[..];
    for tuple in slice {
        result(tuple);
    }
}

pub(crate) fn join_helper<Key: Tuple, Val1: Tuple, Val2: Tuple>(
    input1: &Tuples<(Key, Val1)>,
    input2: &Tuples<(Key, Val2)>,
    mut result: impl FnMut(&Key, &Val1, &Val2),
) {
    let mut slice1 = &input1[..];
    let mut slice2 = &input2[..];

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

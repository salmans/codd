#[macro_export(loca_inner_macros)]
macro_rules! relalg {
    (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) @proj -> [$proj] $(@pred -> [$pred])?)
    };
    (select * from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$pred])?)
    };
}

#[macro_export(local_inner_macros)]
macro_rules! relexp {
    ($r:ident) => {
        &$r
    };
    (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) @proj -> [$proj] $(@pred -> [$pred])?)
    };
    (select * from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$pred])?)
    };
    (($($left:tt)*) join ($($right:tt)*) on [$mapper:expr]) => {
        $crate::relexp!(@join ($($left)*) ($($right)*) @mapper -> [$mapper])
    };
    (@select ($($rel_exp:tt)*) @proj -> [$proj:expr] @pred -> [$pred:expr]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        let sel_exp = $crate::Select::new(&rel_exp, $pred);
        $crate::Project::new(&sel_exp, $proj)
    }};
    (@select ($($rel_exp:tt)*) @proj -> [$proj:expr]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        $crate::Project::new(&rel_exp, $proj)
    }};
    (@select ($($rel_exp:tt)*) @pred -> [$pred:expr]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        $crate::Select::new(&rel_exp, $pred)
    }};
    (@select ($($rel_exp:tt)*)) => {{
        $crate::relexp!($($rel_exp)*)
    }};
    (@join ($($left:tt)*) ($($right:tt)*) @mapper -> [$mapper:expr]) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
       $crate::Join::new(&left, &right, $mapper)
    }};
}

#[cfg(test)]
mod tests {
    use crate::{relalg, relexp};
    use crate::{Database, Expression, Tuples};

    #[test]
    fn test_relalg() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relalg!(select * from (r) where [|t| t % 2 == 0]);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relalg!(select * from
                                 (select * from (r) where [|&t| t > 2])
                where [|t| t % 2 == 0]);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relalg!(select [|t| t + 1] from
                                 (select * from (r) where [|&t| t > 2]));
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
    }

    #[test]
    fn test_relexp() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(r);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(select * from (r) where [|t| t % 2 == 0]);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(select * from
                                 (select * from (r) where [|&t| t > 2])
                where [|t| t % 2 == 0]);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(select [|t| t + 1] from (r));
            r.insert(vec![3, 4, 5, 6].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5, 6, 7]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(select [|t| t + 1] from
                                 (select * from (r) where [|&t| t > 2]));
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let exp = relexp!(select * from(r));
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, String)>("r");
            let s = database.new_relation::<(i32, String)>("s");
            let exp = relexp!((r) join (s) on [|_, x, y| {
                let mut s = x.clone(); s.push_str(y); s
            }]);
            r.insert(
                vec![
                    (1, "a".to_string()),
                    (2, "b".to_string()),
                    (1, "a".to_string()),
                    (4, "b".to_string()),
                ]
                .into(),
                &database,
            )
            .unwrap();
            s.insert(
                vec![(1, "x".to_string()), (2, "y".to_string())].into(),
                &database,
            )
            .unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(
                Tuples::from(vec!["ax".to_string(), "by".to_string()]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v = database.new_view(&r);
            let exp = relexp!(select * from(v));
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            let result = exp.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
    }
}

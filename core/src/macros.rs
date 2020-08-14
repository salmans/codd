#[macro_export]
macro_rules! relalg {
    (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) @proj -> [$proj] $(@pred -> [$pred])?)
    };
    (select * from ($($rel_exp:tt)*) $(where [$pred:expr])?) => {
        $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$pred])?)
    };
    (create relation $name:literal [$schema:ty] in $db:ident) => {
        $db.add_relation::<$schema>($name);
    };
    (create view as
     (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$pred:expr])?)
     in $db:ident) => {
        {
            let inner_exp = $crate::relexp!(@select ($($rel_exp)*)
                                            @proj -> [$proj]
                                            $(@pred -> [$pred])?);
            $db.store_view(&inner_exp)
        }
    };
    (create view as
     (select * from ($($rel_exp:tt)*) $(where [$pred:expr])?)
     in $db:ident) => {
        {
            let inner_exp = $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$pred])?);
            $db.store_view(&inner_exp)
        }
    };
    (insert into ($relation:ident) values [$($value:expr),*] in $db:ident) => {
        {
            $relation.insert(vec![$($value,)*].into(), &$db)
        }
    };
    (insert into ($relation:ident) values [$($value:expr),+,] in $db:ident) => {
        {
            $relation.insert(vec![$($value,)+].into(), &$db)
        }
    };
}

#[macro_export]
macro_rules! relexp {
    ($r:ident) => {
        (&$r).clone()
    };
    ([$s:expr]) => {
        $crate::Singleton($s)
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
    (($($left:tt)*) union ($($right:tt)*)) => {
        $crate::relexp!(@union ($($left)*) ($($right)*))
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
    (@union ($($left:tt)*) ($($right:tt)*)) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::Union::new(&left, &right)
    }};
}

#[cfg(test)]
mod tests {
    use crate::{relalg, relexp};
    use crate::{Database, Tuples};

    #[test]
    fn test_relalg() {
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            assert!(database.relation_instance(&r).is_ok());
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let exp = relalg! { select * from(r) };
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relalg!(select * from (r) where [|t| t % 2 == 0]);
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relalg!(select * from
                                 (select * from (r) where [|&t| t > 2])
                where [|t| t % 2 == 0]);
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relalg!(select [|t| t + 1] from
                                 (select * from (r) where [|&t| t > 2]));
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let v = relalg! { create view as (select * from (r)) in database};
            assert!(database.view_instance(&v).is_ok());
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let v = relalg! { create view as (select [|&x| x > 0] from (r)) in database};
            assert!(database.view_instance(&v).is_ok());
        }
        {
            let database = Database::new();
            let exp = relalg! { select * from (([42]) union ([43]))};
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
    }

    #[test]
    fn test_relexp() {
        {
            let database = Database::new();
            let exp = relexp!([42]);
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(r);
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(select * from (r) where [|t| t % 2 == 0]);
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(select * from
                                 (select * from (r) where [|&t| t > 2])
                where [|t| t % 2 == 0]);
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(select [|t| t + 1] from (r));
            relalg! (insert into (r) values [3, 4, 5, 6] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5, 6, 7]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(select [|t| t + 1] from
                                 (select * from (r) where [|&t| t > 2]));
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let exp = relexp!(select * from(r));
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [(i32, String)] in database};
            let s = relalg! { create relation "s" [(i32, String)] in database};
            let exp = relexp!((r) join (s) on [|_, x, y| {
                let mut s = x.clone(); s.push_str(y); s
            }]);
            relalg! (insert into (r) values [
                (1, "a".to_string()),
                (2, "b".to_string()),
                (1, "a".to_string()),
                (4, "b".to_string()),
            ] in database)
            .unwrap();
            relalg! (insert into (s) values [
                (1, "x".to_string()), (2, "y".to_string())                
            ] in database)
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(
                Tuples::from(vec!["ax".to_string(), "by".to_string()]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [String] in database};
            let s = relalg! { create relation "s" [String] in database};
            let exp = relexp!((r) union (s));
            relalg! (insert into (r) values [
                "a".to_string(),
                "b".to_string(),
            ] in database)
            .unwrap();
            relalg! (insert into (s) values [
                "x".to_string(), "b".to_string(), "y".to_string() 
            ] in database)
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(
                Tuples::from(vec![
                    "a".to_string(),
                    "b".to_string(),
                    "x".to_string(),
                    "y".to_string()
                ]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = relalg! { create relation "r" [i32] in database};
            let v = relalg! { create view as (select * from (r)) in database};
            let exp = relexp!(select * from(v));
            relalg! (insert into (r) values [1, 2, 3, 4] in database).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);

            // updating the view
            relalg! (insert into (r) values [100, 200, 300] in database).unwrap();
            let exp = relexp!(select [|&x| x + 1] from (v) where [|&x| x >= 100]);
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![101, 201, 301]), result);
        }
    }
}

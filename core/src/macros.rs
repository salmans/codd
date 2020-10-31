#[macro_export]
macro_rules! query {
    (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?) => {
        $crate::relexp!(@select ($($rel_exp)*) @proj -> [$proj] $(@pred -> [$($pred)*])?)
    };
    (select * from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?) => {
        $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$($pred)*])?)
    };
    ($db:ident, create relation $name:literal:<$schema:ty>) => {
        $db.add_relation::<$schema>($name);
    };
    ($db:ident, create view as
     (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?)) => {
        {
            let inner_exp = $crate::relexp!(@select ($($rel_exp)*)
                                            @proj -> [$proj]
                                            $(@pred -> [$($pred)*])?);
            $db.store_view(inner_exp.clone())
        }
    };
    ($db:ident, create view as
     (select * from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?)) => {
        {
            let inner_exp = $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$($pred)*])?);
            $db.store_view(inner_exp.clone())
        }
    };
    ($db:ident, insert into ($relation:ident) values [$($value:expr),*]) => {
        {
            $db.insert(&$relation, vec![$($value,)*].into())
        }
    };
    ($db:ident, insert into ($relation:ident) values [$($value:expr),+,]) => {
        {
            $db.insert(&$relation, vec![$($value,)+].into())
        }
    };
}

#[macro_export]
macro_rules! relexp {
    ($r:ident) => {
        (&$r).clone()
    };
    ([$s:expr]) => {
        $crate::expression::Singleton::new($s)
    };
    (select [$proj:expr] from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?) => {
        $crate::relexp!(@select ($($rel_exp)*) @proj -> [$proj] $(@pred -> [$($pred)*])?)
    };
    (select * from ($($rel_exp:tt)*) $(where [$($pred:tt)*])?) => {
        $crate::relexp!(@select ($($rel_exp)*) $(@pred -> [$($pred)*])?)
    };
    (($($left:tt)*) cross ($($right:tt)*) on [$mapper:expr]) => {
        $crate::relexp!(@cross ($($left)*) ($($right)*) @mapper -> [$mapper])
    };
    (($($left:tt)*) join ($($right:tt)*) on [$lkey:expr ; $rkey:expr] with [$mapper:expr]) => {
        $crate::relexp!(@join ($($left)*) @lkey -> [$lkey] ($($right)*) @rkey -> [$rkey] @mapper -> [$mapper])
    };
    (($($left:tt)*) union ($($right:tt)*)) => {
        $crate::relexp!(@union ($($left)*) ($($right)*))
    };
    (($($left:tt)*) intersect ($($right:tt)*)) => {
        $crate::relexp!(@intersect ($($left)*) ($($right)*))
    };
    (($($left:tt)*) minus ($($right:tt)*)) => {
        $crate::relexp!(@minus ($($left)*) ($($right)*))
    };
    (@select ($($rel_exp:tt)*) @proj -> [$proj:expr] @pred -> [$($pred:tt)*]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        let sel_exp = $crate::expression::Select::new(rel_exp, $($pred)*);
        $crate::expression::Project::new(sel_exp, $proj)
    }};
    (@select ($($rel_exp:tt)*) @proj -> [$proj:expr]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        $crate::expression::Project::new(rel_exp, $proj)
    }};
    (@select ($($rel_exp:tt)*) @pred -> [$($pred:tt)*]) => {{
        let rel_exp = $crate::relexp!($($rel_exp)*);
        $crate::expression::Select::new(rel_exp, $($pred)*)
    }};
    (@select ($($rel_exp:tt)*)) => {{
        $crate::relexp!($($rel_exp)*)
    }};
    (@cross ($($left:tt)*) ($($right:tt)*) @mapper -> [$mapper:expr]) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::expression::Product::new(left, right, $mapper)
    }};
    (@join ($($left:tt)*) @lkey -> [$lkey:expr] ($($right:tt)*) @rkey -> [$rkey:expr] @mapper -> [$mapper:expr]) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::expression::Join::new(left, right, $lkey, $rkey, $mapper)
    }};
    (@union ($($left:tt)*) ($($right:tt)*)) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::expression::Union::new(left, right)
    }};
    (@intersect ($($left:tt)*) ($($right:tt)*)) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::expression::Intersect::new(left, right)
    }};
    (@minus ($($left:tt)*) ($($right:tt)*)) => {{
        let left = $crate::relexp!($($left)*);
        let right = $crate::relexp!($($right)*);
        $crate::expression::Difference::new(left, right)
    }};
}

#[cfg(test)]
mod tests {
    use crate::{query, relexp};
    use crate::{Database, Tuples};

    macro_rules! create_relation {
        ($db: ident, $n:literal, $t: ty) => {{
            let relation = query! {$db, create relation $n:<$t>};
            relation.unwrap()
        }};
    }

    #[test]
    fn test_query() {
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            assert!(database.evaluate(&r).is_ok());
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let exp = query! { select * from(r) };
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = query!(select * from (r) where [|tuple| tuple % 2 == 0]);
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = query!(
                select * from
                    (select * from (r) where [|&tuple| tuple > 2])
                where [|tuple| tuple % 2 == 0]
            );
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = query!(select [|t| t + 1] from
                                 (select * from (r) where [|&tuple| tuple > 2]));
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let v = query! { database, create view as (select * from (r))}.unwrap();
            assert!(database.evaluate(&v).is_ok());
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let v = query! { database, create view as (select [|&x| x > 0] from (r))}.unwrap();
            assert!(database.evaluate(&v).is_ok());
        }
        {
            let database = Database::new();
            let exp = query! { select * from (([42]) union ([43]))};
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let database = Database::new();
            let exp = query! { select * from (([42]) intersect ([42]))};
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let database = Database::new();
            let exp = query! { select * from (([42]) minus ([43]))};
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
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
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(r);
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(select * from (r) where [|tuple| tuple % 2 == 0]);
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(select * from
                                 (select * from (r) where [|&tuple| tuple > 2])
                where [|tuple| tuple % 2 == 0]);
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(select [|t| t + 1] from (r));
            query! (database, insert into (r) values [3, 4, 5, 6]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5, 6, 7]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(select [|t| t + 1] from
                                 (select * from (r) where [|&tuple| tuple > 2]));
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let exp = relexp!(select * from(r));
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let s = create_relation!(database, "s", i32);
            let exp = relexp!((r) cross (s) on [|&l, &r| l + r]);
            query! (database, insert into (r) values [
                1, 2, 3
            ])
            .unwrap();
            query! (database, insert into (s) values [
                10, 20, 30
            ])
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(
                Tuples::from(vec![11, 12, 13, 21, 22, 23, 31, 32, 33]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", (i32, String));
            let s = create_relation!(database, "s", (i32, String));
            let exp = relexp!((r) join (s) on [|t| t.0; |t| t.0] with [|_, x, y| {
                let mut s = x.1.clone(); s.push_str(&y.1); s
            }]);
            query! (database, insert into (r) values [
                (1, "a".to_string()),
                (2, "b".to_string()),
                (1, "a".to_string()),
                (4, "b".to_string()),
            ])
            .unwrap();
            query! (database, insert into (s) values [
                (1, "x".to_string()), (2, "y".to_string())
            ])
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(
                Tuples::from(vec!["ax".to_string(), "by".to_string()]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", String);
            let s = create_relation!(database, "s", String);
            let exp = relexp!((r) union (s));
            query! (database, insert into (r) values [
                "a".to_string(),
                "b".to_string(),
            ])
            .unwrap();
            query! (database, insert into (s) values [
                "x".to_string(), "b".to_string(), "y".to_string()
            ])
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
            let r = create_relation!(database, "r", String);
            let s = create_relation!(database, "s", String);
            let exp = relexp!((r) intersect (s));
            query! (database, insert into (r) values [
                "a".to_string(),
                "b".to_string(),
            ])
            .unwrap();
            query! (database, insert into (s) values [
                "x".to_string(), "b".to_string(), "y".to_string()
            ])
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::from(vec!["b".to_string(),]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", String);
            let s = create_relation!(database, "s", String);
            let exp = relexp!((r) minus (s));
            query! (database, insert into (r) values [
                "a".to_string(),
                "b".to_string(),
            ])
            .unwrap();
            query! (database, insert into (s) values [
                "x".to_string(), "b".to_string(), "y".to_string()
            ])
            .unwrap();

            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::from(vec!["a".to_string(),]), result);
        }
        {
            let mut database = Database::new();
            let r = create_relation!(database, "r", i32);
            let v = query! { database, create view as (select * from (r))}.unwrap();
            let exp = relexp!(select * from(v));
            query! (database, insert into (r) values [1, 2, 3, 4]).unwrap();
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3, 4]), result);

            // updating the view
            query! (database, insert into (r) values [100, 200, 300]).unwrap();
            let exp = relexp!(select [|&x| x + 1] from (v) where [|&tuple| tuple >= 100]);
            let result = database.evaluate(&exp).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![101, 201, 301]), result);
        }
    }
}

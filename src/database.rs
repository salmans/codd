use crate::{
    expression::{Expression, Relation, View},
    tools::gallop,
    Tuple,
};
use anyhow::{anyhow, Result};
use std::{any::Any, cell::RefCell, collections::HashMap, ops::Deref, rc::Rc};

#[derive(Clone, Debug, PartialEq)]
pub struct Tuples<T: Tuple> {
    pub items: Vec<T>,
}

impl<T: Tuple, I: IntoIterator<Item = T>> From<I> for Tuples<T> {
    fn from(iterator: I) -> Self {
        let mut items: Vec<T> = iterator.into_iter().collect();
        items.sort_unstable();
        items.dedup();
        Tuples { items }
    }
}

impl<T: Tuple> Tuples<T> {
    pub fn merge(self, other: Self) -> Self {
        let mut tuples = Vec::with_capacity(self.items.len() + other.items.len());
        tuples.extend(self.items.into_iter());
        tuples.extend(other.items.into_iter());
        tuples.into()
    }
}

impl<T: Tuple> Deref for Tuples<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

pub trait TableTrait {
    fn table(&self) -> &dyn Any;

    fn changed(&self) -> bool;

    fn deep_clone(&self) -> Box<dyn TableTrait>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct Table<T: Tuple> {
    pub(crate) stable: Rc<RefCell<Vec<Tuples<T>>>>,
    pub(crate) recent: Rc<RefCell<Tuples<T>>>,
    pub(crate) to_add: Rc<RefCell<Vec<Tuples<T>>>>,
}

impl<T: Tuple> Table<T> {
    pub(crate) fn insert(&self, tuples: Tuples<T>) {
        if !tuples.is_empty() {
            self.to_add.borrow_mut().push(tuples);
        }
    }

    pub(crate) fn tuples(&self) -> Tuples<T> {
        assert!(self.recent.borrow().is_empty());
        assert!(self.to_add.borrow().is_empty());

        let mut result: Tuples<T> = Vec::new().into();

        for batch in self.stable.borrow().iter() {
            result = result.merge(batch.clone());
        }

        result
    }
}

impl<T: Tuple + 'static> TableTrait for Table<T> {
    fn table(&self) -> &dyn Any {
        self
    }

    fn changed(&self) -> bool {
        if !self.recent.borrow().is_empty() {
            let mut recent =
                ::std::mem::replace(&mut (*self.recent.borrow_mut()), Vec::new().into());
            while self
                .stable
                .borrow()
                .last()
                .map(|x| x.len() <= 2 * recent.len())
                == Some(true)
            {
                let last = self.stable.borrow_mut().pop().unwrap();
                recent = recent.merge(last);
            }
            self.stable.borrow_mut().push(recent);
        }

        let to_add = self.to_add.borrow_mut().pop();
        if let Some(mut to_add) = to_add {
            while let Some(to_add_more) = self.to_add.borrow_mut().pop() {
                to_add = to_add.merge(to_add_more);
            }
            for batch in self.stable.borrow().iter() {
                let mut slice = &batch[..];
                to_add.items.retain(|x| {
                    slice = gallop(slice, |y| y < x);
                    slice.len() == 0 || &slice[0] != x
                });
            }
            *self.recent.borrow_mut() = to_add;
        }

        !self.recent.borrow().is_empty()
    }

    fn deep_clone(&self) -> Box<dyn TableTrait> {
        let mut stable: Vec<Tuples<T>> = Vec::new();
        for batch in self.stable.borrow().iter() {
            stable.push(batch.clone());
        }
        let mut to_add = Vec::new();
        for batch in self.to_add.borrow().iter() {
            to_add.push(batch.clone());
        }
        let recent = (*self.recent.borrow()).clone();
        Box::new(Self {
            stable: Rc::new(RefCell::new(stable)),
            recent: Rc::new(RefCell::new(recent)),
            to_add: Rc::new(RefCell::new(to_add)),
        })
    }
}

#[derive(PartialEq, Eq, Clone, Hash)]
pub struct ViewRef(i32);

trait ViewEntryTrait {
    fn view(&self) -> &dyn Any;

    fn table(&self) -> &dyn TableTrait;

    fn update(&self, db: &Database) -> Result<()>;

    fn deep_clone(&self) -> Box<dyn ViewEntryTrait>;
}

struct ViewEntry<T: Tuple> {
    table: Table<T>,
    expression: Box<dyn Expression<T>>,
}

impl<T: Tuple + 'static> ViewEntryTrait for ViewEntry<T> {
    fn view(&self) -> &dyn Any {
        self
    }

    fn table(&self) -> &dyn TableTrait {
        &self.table
    }

    fn update(&self, db: &Database) -> Result<()> {
        self.expression.update_to(db, &self.table)
    }

    fn deep_clone(&self) -> Box<dyn ViewEntryTrait> {
        Box::new(Self {
            table: self.table.clone(),
            expression: (*self.expression).duplicate(),
        })
    }
}

pub struct Database {
    relations: HashMap<String, Box<dyn TableTrait>>,
    views: HashMap<ViewRef, Box<dyn ViewEntryTrait>>,
    view_counter: i32,
}

impl Database {
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            views: HashMap::new(),
            view_counter: 0,
        }
    }

    pub fn deep_clone(&self) -> Self {
        let mut relations = HashMap::new();
        let mut views = HashMap::new();

        self.relations.iter().for_each(|(k, v)| {
            relations.insert(k.clone(), v.deep_clone());
        });
        self.views.iter().for_each(|(k, v)| {
            views.insert(k.clone(), v.deep_clone());
        });

        Self {
            relations,
            views,
            view_counter: self.view_counter,
        }
    }

    pub fn new_relation<T: Tuple + 'static>(&mut self, name: &str) -> Relation<T> {
        let relation: Table<T> = Table {
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        };
        self.relations.insert(name.to_owned(), Box::new(relation));
        Relation::new(name)
    }

    pub fn relation<T: Tuple + 'static>(&self, relation: &Relation<T>) -> Result<&Table<T>> {
        let result = self
            .relations
            .get(&relation.name)
            .and_then(|r| r.table().downcast_ref::<Table<T>>())
            .ok_or(anyhow!("relation not found"))?;
        Ok(result)
    }

    pub fn new_view<T: Tuple + 'static, E>(&mut self, expression: &E) -> View<T>
    where
        E: Expression<T> + 'static,
    {
        let reference = ViewRef(self.view_counter);

        let table: Table<T> = Table {
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        };
        self.views.insert(
            reference.clone(),
            Box::new(ViewEntry {
                table: table,
                expression: expression.duplicate(),
            }),
        );

        self.view_counter += 1;

        View::new(reference)
    }

    pub fn view<T: Tuple + 'static>(&self, view: &View<T>) -> Result<&Table<T>> {
        let result = self
            .views
            .get(&view.reference)
            .and_then(|v| v.view().downcast_ref::<ViewEntry<T>>())
            .ok_or(anyhow!("view not found"))?;
        Ok(&result.table)
    }

    pub fn update_views(&self) -> Result<()> {
        while self.relation_changed() || self.view_changed() {
            for view in self.views.iter() {
                view.1.update(&self)?
            }
        }
        Ok(())
    }

    fn relation_changed(&self) -> bool {
        let mut result = false;
        for table in self.relations.iter() {
            if table.1.changed() {
                result = true
            }
        }

        result
    }

    fn view_changed(&self) -> bool {
        let mut result = false;
        for view in self.views.iter() {
            if view.1.table().changed() {
                result = true
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuples_from_list() {
        {
            let tuples = Tuples::<i32>::from(vec![]);
            assert_eq!(Vec::<i32>::new(), tuples.items);
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4, 2, 1, 3]);
            assert_eq!(vec![1, 2, 3, 4, 5], tuples.items);
        }
        {
            let tuples = Tuples::<i32>::from(vec![3, 2, 2, 1, 3]);
            assert_eq!(vec![1, 2, 3], tuples.items);
        }
    }

    #[test]
    fn test_tuples_merge() {
        {
            let tuples = Tuples::<i32>::from(vec![]);
            assert_eq!(Vec::<i32>::new(), tuples.merge(vec![].into()).items);
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4]);
            assert_eq!(vec![2, 3, 4, 5], tuples.merge(vec![2, 3].into()).items);
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4, 4]);
            assert_eq!(vec![3, 4, 5], tuples.merge(vec![5, 3].into()).items);
        }
    }

    #[test]
    fn test_table_insert() {
        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                to_add: Rc::new(RefCell::new(vec![])),
            };
            relation.insert(vec![].into());
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.stable.borrow());
            assert_eq!(Vec::<i32>::new(), relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation: Table<i32> = Table {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                to_add: Rc::new(RefCell::new(vec![])),
            };
            relation.insert(vec![].into());
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.stable.borrow());
            assert_eq!(vec![1, 2, 3], relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation: Table<i32> = Table {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                to_add: Rc::new(RefCell::new(vec![])),
            };
            relation.insert(vec![5, 4].into());
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.stable.borrow());
            assert_eq!(vec![1, 2, 3], relation.recent.borrow().items);
            assert_eq!(
                Vec::<Tuples<i32>>::from(vec![vec![4, 5].into()]),
                *relation.to_add.borrow(),
            );
        }
    }

    #[test]
    fn test_table_changed() {
        {
            let relation: Table<i32> = Table {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                to_add: Rc::new(RefCell::new(vec![])),
            };
            relation.changed();
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.stable.borrow());
            assert_eq!(Vec::<i32>::new(), relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                to_add: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
            };
            assert!(relation.changed());
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.stable.borrow());
            assert_eq!(vec![1, 2], relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2].into())),
                to_add: Rc::new(RefCell::new(vec![])),
            };
            assert!(!relation.changed());
            assert_eq!(
                Vec::<Tuples<i32>>::from(vec![vec![1, 2].into()]),
                *relation.stable.borrow()
            );
            assert_eq!(Vec::<i32>::new(), relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2].into())),
                to_add: Rc::new(RefCell::new(vec![vec![3, 4].into()])),
            };
            assert!(relation.changed());
            assert_eq!(
                Vec::<Tuples<i32>>::from(vec![vec![1, 2].into()]),
                *relation.stable.borrow()
            );
            assert_eq!(vec![3, 4], relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
            };
            assert!(relation.changed());
            assert_eq!(
                Vec::<Tuples<i32>>::from(vec![vec![1, 2, 3, 4].into()]),
                *relation.stable.borrow()
            );
            assert_eq!(vec![5], relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }

        {
            let relation = Table::<i32> {
                stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                to_add: Rc::new(RefCell::new(vec![vec![1, 5].into()])),
            };
            assert!(relation.changed());
            assert_eq!(
                Vec::<Tuples<i32>>::from(vec![vec![1, 2, 3, 4].into()]),
                *relation.stable.borrow()
            );
            assert_eq!(vec![5], relation.recent.borrow().items);
            assert_eq!(Vec::<Tuples<i32>>::new(), *relation.to_add.borrow());
        }
    }

    #[test]
    fn test_database_new() {
        let database = Database::new();
        assert!(database.relations.is_empty());
        assert!(database.views.is_empty());
        assert_eq!(0, database.view_counter);
    }

    #[test]
    fn test_database_deep_clone() {
        {
            let database = Database::new();
            let cloned = database.deep_clone();
            assert!(cloned.relations.is_empty());
            assert!(cloned.views.is_empty());
            assert_eq!(0, cloned.view_counter);
        }

        {
            let mut relations: HashMap<String, Box<dyn TableTrait>> = HashMap::new();
            relations.insert(
                "a".to_string(),
                Box::new(Table::<i32> {
                    stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                    recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                    to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
                }),
            );
            relations.insert(
                "b".to_string(),
                Box::new(Table::<String> {
                    stable: Rc::new(RefCell::new(vec![vec!["A".to_string()].into()])),
                    recent: Rc::new(RefCell::new(vec!["B".to_string()].into())),
                    to_add: Rc::new(RefCell::new(vec![vec!["C".to_string()].into()])),
                }),
            );

            let mut views: HashMap<ViewRef, Box<dyn ViewEntryTrait>> = HashMap::new();
            views.insert(
                ViewRef(0),
                Box::new(ViewEntry {
                    table: Table::<i32> {
                        stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                        recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                        to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
                    },
                    expression: Box::new(Relation::new("r")),
                }),
            );

            let database = Database {
                relations,
                views,
                view_counter: 1,
            };

            let cloned = database.deep_clone();
            assert_eq!(2, cloned.relations.len());
            assert_eq!(1, cloned.views.len());
            assert_eq!(1, cloned.view_counter);
            assert_eq!(
                vec!["B".to_string()],
                cloned
                    .relations
                    .get("b")
                    .unwrap()
                    .table()
                    .downcast_ref::<Table<String>>()
                    .unwrap()
                    .recent
                    .borrow()
                    .items
                    .clone()
            );
        }
    }

    #[test]
    fn test_add_relation() {
        let mut database = Database::new();
        database.new_relation::<i32>("a");
        assert!(database.relations.get("a").is_some());
        assert!(database.relations.get("b").is_none());
    }

    #[test]
    fn test_get_relation() {
        let mut database = Database::new();
        let mut dummy = Database::new();
        let relation_i32 = database.new_relation::<i32>("a");
        let relation_string = dummy.new_relation::<String>("a");

        assert!(database.relation(&relation_i32).is_ok());
        assert!(database.relation(&relation_string).is_err());

        let _ = database.new_relation::<String>("a");
        assert!(database.relation(&relation_string).is_ok());
    }

    #[test]
    fn test_add_view() {
        let mut database = Database::new();
        database.new_view(&Relation::<i32>::new("a"));
        assert!(database.views.get(&ViewRef(0)).is_some());
        assert!(database.views.get(&ViewRef(1000)).is_none());
    }

    #[test]
    fn test_get_view() {
        let mut database = Database::new();
        let mut dummy = Database::new();
        let view_i32 = database.new_view(&Relation::<i32>::new("a"));
        let view_string_1 = dummy.new_view(&Relation::<String>::new("a"));

        assert!(database.view(&view_i32).is_ok());
        assert!(database.view(&view_string_1).is_err());

        let view_string_2 = database.new_view(&Relation::<String>::new("a"));
        assert!(database.view(&view_string_1).is_err());
        assert!(database.view(&view_string_2).is_ok());
    }

    #[test]
    fn test_relation_changed() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();

        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            database.relation(&r).unwrap()
        );

        assert!(database.relation_changed());
        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            database.relation(&r).unwrap()
        );

        assert!(!database.relation_changed());
        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
            },
            database.relation(&r).unwrap()
        );
    }

    #[test]
    fn test_view_changed() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        let v = database.new_view(&r);
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        database.relation_changed();
        database
            .views
            .get(&v.reference)
            .unwrap()
            .update(&database)
            .unwrap();

        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            database.view(&v).unwrap()
        );

        assert!(database.view_changed());
        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            database.view(&v).unwrap()
        );

        assert!(!database.view_changed());
        assert_eq!(
            &Table::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
            },
            database.view(&v).unwrap()
        );
    }

    #[test]
    fn test_update_views() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v_r = database.new_view(&r);
            let s = database.new_relation::<String>("s");
            let v_s = database.new_view(&s);

            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            s.insert(
                vec!["A".to_string(), "B".to_string(), "C".to_string()].into(),
                &database,
            )
            .unwrap();

            database.update_views().unwrap();

            assert_eq!(
                &Table::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                },
                database.view(&v_r).unwrap()
            );
            assert_eq!(
                &Table::<String> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![
                        "A".to_string(),
                        "B".to_string(),
                        "C".to_string()
                    ]
                    .into()])),
                },
                database.view(&v_s).unwrap()
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v_1 = database.new_view(&r);
            let v_2 = database.new_view(&v_1);

            r.insert(vec![1, 2, 3].into(), &database).unwrap();

            database.update_views().unwrap();

            assert_eq!(
                &Table::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                },
                database.view(&v_1).unwrap()
            );
            assert_eq!(
                &Table::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                },
                database.view(&v_2).unwrap()
            );
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = database.new_relation::<i32>("r");
            let _ = database.new_view(&r);
            let s = dummy.new_relation::<String>("s");
            let _ = database.new_view(&s);

            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            s.insert(
                vec!["A".to_string(), "B".to_string(), "C".to_string()].into(),
                &dummy,
            )
            .unwrap();

            assert!(database.update_views().is_err());
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.new_relation::<i32>("r");
            let _ = database.new_view(&r);
            let s = database.new_relation::<String>("s");
            let _ = database.new_view(&s);

            r.insert(vec![1, 2, 3].into(), &dummy).unwrap();
            s.insert(
                vec!["A".to_string(), "B".to_string(), "C".to_string()].into(),
                &database,
            )
            .unwrap();

            assert!(database.update_views().is_err());
        }
    }
}

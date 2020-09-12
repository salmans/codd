mod elements;
mod evaluate;
mod helpers;
mod validate_view;

use crate::{
    expression::{Expression, Relation, View},
    Error, Tuple,
};
use helpers::gallop;
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

trait InstanceExt {
    fn as_any(&self) -> &dyn Any;

    fn changed(&self) -> bool;

    fn duplicate(&self) -> Box<dyn InstanceExt>;
}

#[derive(Clone, Debug, PartialEq)]
struct Instance<T: Tuple> {
    stable: Rc<RefCell<Vec<Tuples<T>>>>,
    recent: Rc<RefCell<Tuples<T>>>,
    to_add: Rc<RefCell<Vec<Tuples<T>>>>,
}

impl<T: Tuple> Instance<T> {
    fn new() -> Self {
        Self {
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn insert(&self, tuples: Tuples<T>) {
        if !tuples.is_empty() {
            self.to_add.borrow_mut().push(tuples);
        }
    }
}

impl<T> InstanceExt for Instance<T>
where
    T: Tuple + 'static,
{
    fn as_any(&self) -> &dyn Any {
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

    fn duplicate(&self) -> Box<dyn InstanceExt> {
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

type RelationRef = String;

#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub(crate) struct ViewRef(i32);

trait MaterializedViewExt {
    fn as_any(&self) -> &dyn Any;

    fn instance(&self) -> &dyn InstanceExt;

    fn recalculate(&self, db: &Database) -> Result<(), Error>;

    fn duplicate(&self) -> Box<dyn MaterializedViewExt>;
}

struct MaterializedView<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    instance: Instance<T>,
    expression: E,
}

impl<T, E> MaterializedViewExt for MaterializedView<T, E>
where
    T: Tuple + 'static,
    E: Expression<T> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn instance(&self) -> &dyn InstanceExt {
        &self.instance
    }

    fn recalculate(&self, db: &Database) -> Result<(), Error> {
        let recent = evaluate::Incremental(db);
        let recent = self.expression.collect(&recent)?;
        self.instance.insert(recent);
        Ok(())
    }

    fn duplicate(&self) -> Box<dyn MaterializedViewExt> {
        Box::new(Self {
            instance: self.instance.clone(),
            expression: self.expression.clone(),
        })
    }
}

struct ViewEntry {
    instance: Box<dyn MaterializedViewExt>,
    up_view_refs: Vec<ViewRef>,
    up_relation_refs: Vec<RelationRef>,
    down_refs: Vec<ViewRef>,
}

impl ViewEntry {
    fn new<T, E>(view: MaterializedView<T, E>) -> Self
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static,
    {
        Self {
            instance: Box::new(view),
            up_view_refs: Vec::new(),
            up_relation_refs: Vec::new(),
            down_refs: Vec::new(),
        }
    }

    fn add_view_ref(&mut self, v: ViewRef) {
        self.down_refs.push(v)
    }

    fn duplicate(&self) -> Self {
        Self {
            instance: self.instance.duplicate(),
            up_view_refs: self.up_view_refs.clone(),
            up_relation_refs: self.up_relation_refs.clone(),
            down_refs: self.down_refs.clone(),
        }
    }
}

struct RelationEntry {
    instance: Box<dyn InstanceExt>,
    down_refs: Vec<ViewRef>,
}

impl RelationEntry {
    fn new<T>(view: Instance<T>) -> Self
    where
        T: Tuple + 'static,
    {
        Self {
            instance: Box::new(view),
            down_refs: Vec::new(),
        }
    }

    fn add_view_ref(&mut self, v: ViewRef) {
        self.down_refs.push(v)
    }

    fn duplicate(&self) -> Self {
        Self {
            instance: self.instance.duplicate(),
            down_refs: self.down_refs.clone(),
        }
    }
}

pub struct Database {
    relations: HashMap<RelationRef, RelationEntry>,
    views: HashMap<ViewRef, ViewEntry>,
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

    pub fn duplicate(&self) -> Self {
        let mut relations = HashMap::new();
        let mut views = HashMap::new();

        self.relations.iter().for_each(|(k, v)| {
            relations.insert(k.clone(), v.duplicate());
        });
        self.views.iter().for_each(|(k, v)| {
            views.insert(k.clone(), v.duplicate());
        });

        Self {
            relations,
            views,
            view_counter: self.view_counter,
        }
    }

    pub fn evaluate<T, E>(&self, expression: &E) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: Expression<T>,
    {
        expression.collect(&evaluate::Evaluator(self))
    }

    pub fn add_relation<T>(&mut self, name: &str) -> Result<Relation<T>, Error>
    where
        T: Tuple + 'static,
    {
        if !self.relations.contains_key(name) {
            self.relations
                .insert(name.to_owned(), RelationEntry::new(Instance::<T>::new()));
            Ok(Relation::new(name))
        } else {
            Err(Error::InstanceExists {
                name: name.to_string(),
            })
        }
    }

    pub fn insert<T>(&self, relation: &Relation<T>, tuples: Tuples<T>) -> Result<(), Error>
    where
        T: Tuple + 'static,
    {
        let instance = self.relation_instance(&relation)?;
        instance.insert(tuples);
        Ok(())
    }

    fn relation_instance<T>(&self, relation: &Relation<T>) -> Result<&Instance<T>, Error>
    where
        T: Tuple + 'static,
    {
        let result = self
            .relations
            .get(&relation.name)
            .and_then(|r| r.instance.as_any().downcast_ref::<Instance<T>>())
            .ok_or(Error::InstanceNotFound {
                name: relation.name.clone(),
            })?;
        Ok(result)
    }

    pub fn store_view<T, E>(&mut self, expression: &E) -> Result<View<T, E>, Error>
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static,
    {
        let mut validator = validate_view::Validator::new();
        expression.visit(&mut validator);

        if let Some(error) = validator.0 {
            return Err(error);
        }

        let mut elements = elements::Elements::new();
        expression.visit(&mut elements);

        let table: Instance<T> = Instance {
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        };
        let mut entry = ViewEntry::new(MaterializedView {
            instance: table,
            expression: expression.clone(),
        });

        let reference = ViewRef(self.view_counter);

        for r in elements.relations().iter() {
            entry.up_relation_refs.push(r.clone());
            self.relations
                .get_mut(r)
                .map(|rs| rs.add_view_ref(reference.clone()));
        }

        for r in elements.views().iter() {
            entry.up_view_refs.push(r.clone());
            self.views
                .get_mut(r)
                .map(|rs| rs.add_view_ref(reference.clone()));
        }
        self.views.insert(reference.clone(), entry);

        self.view_counter += 1;

        Ok(View::new(reference))
    }

    fn view_instance<T, E>(&self, view: &View<T, E>) -> Result<&Instance<T>, Error>
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static,
    {
        let result = self
            .views
            .get(&view.reference)
            .and_then(|v| v.instance.as_any().downcast_ref::<MaterializedView<T, E>>())
            .ok_or(Error::InstanceNotFound {
                name: format!("{:?}", view.reference),
            })?;
        Ok(&result.instance)
    }

    fn recalculate_view(&self, view_ref: &ViewRef) -> Result<(), Error> {
        if let Some(entry) = self.views.get(view_ref) {
            for r in entry.up_relation_refs.iter() {
                self.recalculate_relation(r)?;
            }
            for r in entry.up_view_refs.iter() {
                self.recalculate_view(r)?;
            }

            while entry.instance.instance().changed() {
                for r in entry.down_refs.iter() {
                    self.views.get(r).unwrap().instance.recalculate(&self)?;
                    self.recalculate_view(r)?;
                }
            }
        }

        Ok(())
    }

    fn recalculate_relation(&self, relation_ref: &RelationRef) -> Result<(), Error> {
        if let Some(entry) = self.relations.get(relation_ref) {
            while entry.instance.changed() {
                for r in entry.down_refs.iter() {
                    self.views.get(r).unwrap().instance.recalculate(&self)?;
                    self.recalculate_view(r)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Join, Project, Select};

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
            let relation = Instance::<i32> {
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
            let relation: Instance<i32> = Instance {
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
            let relation: Instance<i32> = Instance {
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
    fn test_insert() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add.borrow()[0]
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_ok());
            assert!(database.insert(&r, vec![1, 4].into()).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add.borrow()[0]
            );
            assert_eq!(
                Tuples::<i32>::from(vec![1, 4]),
                database.relation_instance(&r).unwrap().to_add.borrow()[1]
            );
        }
        {
            let database = Database::new();
            let r = Database::new().add_relation("r").unwrap(); // dummy database
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_err());
        }
    }

    #[test]
    fn test_table_changed() {
        {
            let relation: Instance<i32> = Instance {
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
            let relation = Instance::<i32> {
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
            let relation = Instance::<i32> {
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
            let relation = Instance::<i32> {
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
            let relation = Instance::<i32> {
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
            let relation = Instance::<i32> {
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
    fn test_database_duplicate() {
        {
            let database = Database::new();
            let cloned = database.duplicate();
            assert!(cloned.relations.is_empty());
            assert!(cloned.views.is_empty());
            assert_eq!(0, cloned.view_counter);
        }

        {
            let mut relations: HashMap<String, RelationEntry> = HashMap::new();
            relations.insert(
                "a".to_string(),
                RelationEntry::new(Instance::<i32> {
                    stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                    recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                    to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
                }),
            );
            relations.insert(
                "b".to_string(),
                RelationEntry::new(Instance::<String> {
                    stable: Rc::new(RefCell::new(vec![vec!["A".to_string()].into()])),
                    recent: Rc::new(RefCell::new(vec!["B".to_string()].into())),
                    to_add: Rc::new(RefCell::new(vec![vec!["C".to_string()].into()])),
                }),
            );

            let mut views: HashMap<ViewRef, ViewEntry> = HashMap::new();
            views.insert(
                ViewRef(0),
                ViewEntry::new(MaterializedView {
                    instance: Instance::<i32> {
                        stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                        recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                        to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
                    },
                    expression: Relation::new("r"),
                }),
            );

            let database = Database {
                relations,
                views,
                view_counter: 1,
            };

            let cloned = database.duplicate();
            assert_eq!(2, cloned.relations.len());
            assert_eq!(1, cloned.views.len());
            assert_eq!(1, cloned.view_counter);
            assert_eq!(
                vec!["B".to_string()],
                cloned
                    .relations
                    .get("b")
                    .unwrap()
                    .instance
                    .as_any()
                    .downcast_ref::<Instance<String>>()
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
        assert!(database.add_relation::<i32>("a").is_ok());
        assert!(database.add_relation::<i32>("a").is_err()); // duplicate
        assert!(database.relations.get("a").is_some());
        assert!(database.relations.get("b").is_none());
    }

    #[test]
    fn test_get_relation() {
        let mut database = Database::new();
        let mut dummy = Database::new();
        let relation_i32 = database.add_relation::<i32>("a").unwrap();
        let relation_string = dummy.add_relation::<String>("a").unwrap();

        assert!(database.relation_instance(&relation_i32).is_ok());
        assert!(database.relation_instance(&relation_string).is_err());
    }

    #[test]
    fn test_store_view() {
        {
            let mut database = Database::new();
            database.store_view(&Relation::<i32>::new("a")).unwrap();
            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            database
                .store_view(&Select::new(&Relation::<i32>::new("a"), |&t| t != 0))
                .unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            database
                .store_view(&Project::new(&Relation::<i32>::new("a"), |t| t + 1))
                .unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            database
                .store_view(&Join::new(
                    &Relation::<(i32, i32)>::new("a"),
                    &Relation::<(i32, i32)>::new("b"),
                    |t| t.0,
                    |t| t.0,
                    |_, &l, &r| (l, r),
                ))
                .unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            let view = database.store_view(&Relation::<i32>::new("a")).unwrap();
            database.store_view(&view).unwrap();
            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }
    }

    #[test]
    fn test_get_view() {
        let mut database = Database::new();
        let mut dummy = Database::new();
        let view_i32 = database.store_view(&Relation::<i32>::new("a")).unwrap();
        let view_string_1 = dummy.store_view(&Relation::<String>::new("a")).unwrap();

        assert!(database.view_instance(&view_i32).is_ok());
        assert!(database.view_instance(&view_string_1).is_err());

        let view_string_2 = database.store_view(&Relation::<String>::new("a")).unwrap();
        assert!(database.view_instance(&view_string_1).is_err());
        assert!(database.view_instance(&view_string_2).is_ok());
    }

    #[test]
    fn test_relation_changed() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r").unwrap();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        let r_inst = database.relation_instance(&r).unwrap();

        assert_eq!(
            &Instance::<i32> {
                to_add: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            r_inst
        );
        assert!(r_inst.changed());

        assert_eq!(
            &Instance::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                stable: Rc::new(RefCell::new(vec![])),
            },
            r_inst
        );
        assert!(!r_inst.changed());

        assert_eq!(
            &Instance::<i32> {
                to_add: Rc::new(RefCell::new(vec![])),
                recent: Rc::new(RefCell::new(vec![].into())),
                stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
            },
            r_inst
        );
    }

    #[test]
    fn test_view_changed() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v = database.store_view(&r).unwrap();

            let r_inst = database.relation_instance(&r).unwrap();
            let v_inst = database.view_instance(&v).unwrap();

            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            r_inst.changed();

            database
                .views
                .get(&v.reference)
                .unwrap()
                .instance
                .recalculate(&database)
                .unwrap();

            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );
            assert!(v_inst.changed());

            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![1, 2, 3].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );
            assert!(!v_inst.changed());

            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![1, 2, 3].into()])),
                },
                v_inst
            );
        }

        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v = database
                .store_view(&Select::new(&r, |t| t % 2 == 1))
                .unwrap();

            let r_inst = database.relation_instance(&r).unwrap();
            let v_inst = database.view_instance(&v).unwrap();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            r_inst.changed();

            database
                .views
                .get(&v.reference)
                .unwrap()
                .instance
                .recalculate(&database)
                .unwrap();

            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![vec![1, 3].into()])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(v_inst.changed());
            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![1, 3].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(!v_inst.changed());
            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![1, 3].into()])),
                },
                v_inst
            );
        }

        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v = database.store_view(&Project::new(&r, |t| t + 1)).unwrap();

            let r_inst = database.relation_instance(&r).unwrap();
            let v_inst = database.view_instance(&v).unwrap();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            r_inst.changed();

            database
                .views
                .get(&v.reference)
                .unwrap()
                .instance
                .recalculate(&database)
                .unwrap();

            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![vec![2, 3, 4, 5].into()])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(v_inst.changed());
            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![2, 3, 4, 5].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(!v_inst.changed());
            assert_eq!(
                &Instance::<i32> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![2, 3, 4, 5].into()])),
                },
                v_inst
            );
        }

        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let v = database
                .store_view(&Join::new(&r, &s, |t| t.0, |t| t.0, |&k, _, &r| (k, r.1)))
                .unwrap();

            let r_inst = database.relation_instance(&r).unwrap();
            let s_inst = database.relation_instance(&s).unwrap();
            let v_inst = database.view_instance(&v).unwrap();

            database
                .insert(&r, vec![(1, 2), (2, 3), (3, 4)].into())
                .unwrap();
            database
                .insert(&s, vec![(2, 3), (3, 4), (4, 5)].into())
                .unwrap();
            r_inst.changed();
            s_inst.changed();

            database
                .views
                .get(&v.reference)
                .unwrap()
                .instance
                .recalculate(&database)
                .unwrap();

            assert_eq!(
                &Instance::<(i32, i32)> {
                    to_add: Rc::new(RefCell::new(vec![vec![(2, 3), (3, 4)].into()])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(v_inst.changed());
            assert_eq!(
                &Instance::<(i32, i32)> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![(2, 3), (3, 4)].into())),
                    stable: Rc::new(RefCell::new(vec![])),
                },
                v_inst
            );

            assert!(!v_inst.changed());
            assert_eq!(
                &Instance::<(i32, i32)> {
                    to_add: Rc::new(RefCell::new(vec![])),
                    recent: Rc::new(RefCell::new(vec![].into())),
                    stable: Rc::new(RefCell::new(vec![vec![(2, 3), (3, 4)].into()])),
                },
                v_inst
            );
        }
    }
}

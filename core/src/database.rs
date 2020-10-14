/*! Implements a minimal database with the following features:
* Relation and view instances are generic over [`Tuple`] types.
* Supports incremental view update by keeping track of recently added tuples.
* Relation instances monotonically grow (supports insertion but not deletion).

[`Database`]: ../trait.Tuple.html
*/
mod evaluate;
mod expression_ext;
mod helpers;
mod instance;
mod validate;

use crate::{
    expression::{dependency, view::ViewRef},
    Error, Expression, Relation, Tuple, View,
};
use expression_ext::ExpressionExt;
pub use instance::Tuples;
use std::{
    cell::Cell,
    collections::{HashMap, HashSet},
};

use instance::{DynInstance, Instance};

/// Contains the information about an instance in the database.
struct RelationEntry {
    /// Is the `Instance` containing the tuples of this relation.
    instance: Box<dyn DynInstance>,

    /// Contains references to the views that this relation appears in their
    /// expression. These are the views that depend on the content of this relation.
    dependent_views: HashSet<ViewRef>,

    /// A flag that indicating if this relation is being stabilized.
    stabilizing: Cell<bool>,
}

impl RelationEntry {
    /// Creates a new `RelationEntry` with the given `instance`.
    fn new<T>() -> Self
    where
        T: Tuple + 'static,
    {
        Self {
            instance: Box::new(Instance::<T>::new()),
            dependent_views: HashSet::new(),
            stabilizing: Cell::new(false),
        }
    }

    /// Adds a dependency from a view (identified by `view_ref`) to this relation.
    fn add_dependent_view(&mut self, view_ref: ViewRef) {
        self.dependent_views.insert(view_ref);
    }
}

impl Clone for RelationEntry {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone_box(),
            dependent_views: self.dependent_views.clone(),
            stabilizing: self.stabilizing.clone(),
        }
    }
}

use instance::{DynViewInstance, ViewInstance};

/// Contains the information about a view in the database.
struct ViewEntry {
    /// Is the underlying `Instance` storing the tuples of the view.
    instance: Box<dyn DynViewInstance>,

    /// Contains references (relation names) to the relations that
    /// appear in the view's expression. These are the relations to
    /// which the content of this view depends.
    dependee_relations: HashSet<String>,

    /// Contains references to the views that appear in the view's
    /// expression. These are the views to which the content of this
    /// view depends.
    dependee_views: HashSet<ViewRef>,

    /// Contains references to the views that this view appears in
    /// their expressions. These are the views that depend on the
    /// content of this view.
    dependent_views: HashSet<ViewRef>,

    /// A flag that indicating if this view is being stabilized.
    stabilizing: Cell<bool>,
}

impl ViewEntry {
    /// Creates a new `ViewEntry` with the given `view_instance`.
    fn new<T, E>(view_instance: ViewInstance<T, E>) -> Self
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        Self {
            instance: Box::new(view_instance),
            dependee_relations: HashSet::new(),
            dependee_views: HashSet::new(),
            dependent_views: HashSet::new(),
            stabilizing: Cell::new(false),
        }
    }

    /// Adds a dependency from a view (identified by `view_ref`) to this view.
    fn add_dependent_view(&mut self, view_ref: ViewRef) {
        self.dependent_views.insert(view_ref);
    }
}

impl Clone for ViewEntry {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone_box(),
            dependee_views: self.dependee_views.clone(),
            dependee_relations: self.dependee_relations.clone(),
            dependent_views: self.dependent_views.clone(),
            stabilizing: self.stabilizing.clone(),
        }
    }
}

/// Is a database that stores tuples in relation instances and offers and maintains views
/// over its data.
///
/// **Example**:
/// ```rust
/// use codd::{Database, Select};
///
/// // create a new database:
/// let mut db = Database::new();
///
/// // add a new relation "numbers" with tuples of type `u32` to `db`:
/// let numbers = db.add_relation::<u32>("numbers").unwrap();
///
/// // create a view for odd numbers in `numbers`:
/// let odds = db.store_view(&Select::new(&numbers, |i| i % 2 == 1)).unwrap();
///
/// // insert some items in `numbers`:
/// db.insert(&numbers, vec![4, 8, 15, 16, 23, 42].into()).unwrap();
///
/// // query `db` for `numbers` and `odds`:
/// let numbers_data = db.evaluate(&numbers).unwrap();
/// let odds_data = db.evaluate(&odds).unwrap();
///
/// assert_eq!(vec![4, 8, 15, 16, 23, 42], numbers_data.into_tuples());
/// assert_eq!(vec![15, 23], odds_data.into_tuples());
///
/// // go nuts:
/// db.insert(&numbers, vec![8, 888, 23, 1001, 8008, 101].into()).unwrap();
///
/// // query `db` again:
/// let numbers_data = db.evaluate(&numbers).unwrap();
/// let odds_data = db.evaluate(&odds).unwrap();
///
/// assert_eq!(vec![4, 8, 15, 16, 23, 42, 101, 888, 1001, 8008], numbers_data.into_tuples());
/// assert_eq!(vec![15, 23, 101, 1001], odds_data.into_tuples());
/// ```
pub struct Database {
    relations: HashMap<String, RelationEntry>,
    views: HashMap<ViewRef, ViewEntry>,
    view_counter: i32,
}

impl Database {
    /// Creates a new empty database.
    pub fn new() -> Self {
        Self {
            relations: HashMap::new(),
            views: HashMap::new(),
            view_counter: 0,
        }
    }

    /// Evaluates `expression` in the database and returns the result in a `Tuples` object.
    pub fn evaluate<T, E>(&self, expression: &E) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        expression.collect_recent(&evaluate::Evaluator::new(self))
    }

    /// Adds a new relation instance identified by `name` to the database and returns the a
    /// corresponding `Relation` object.
    pub fn add_relation<T>(&mut self, name: &str) -> Result<Relation<T>, Error>
    where
        T: Tuple + 'static,
    {
        if !self.relations.contains_key(name) {
            self.relations
                .insert(name.into(), RelationEntry::new::<T>());
            Ok(Relation::new(name))
        } else {
            Err(Error::InstanceExists { name: name.into() })
        }
    }

    /// Inserts tuples in the relation `Instance` for `relation`.
    pub fn insert<T>(&self, relation: &Relation<T>, tuples: Tuples<T>) -> Result<(), Error>
    where
        T: Tuple + 'static,
    {
        let instance = self.relation_instance(&relation)?;
        instance.insert(tuples);
        Ok(())
    }

    /// Returns the instance for `relation` if it exists.
    fn relation_instance<T>(&self, relation: &Relation<T>) -> Result<&Instance<T>, Error>
    where
        T: Tuple + 'static,
    {
        let result = self
            .relations
            .get(relation.name())
            .and_then(|r| r.instance.as_any().downcast_ref::<Instance<T>>())
            .ok_or(Error::InstanceNotFound {
                name: relation.name().into(),
            })?;
        Ok(result)
    }

    /// Stores a new view over `expression` and returns the corresponding [`View`] expression.
    ///
    /// [`View`]: ./expression/struct.View.html
    pub fn store_view<T, E>(&mut self, expression: &E) -> Result<View<T, E>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        // `validator` rejects views over `Difference` (not supported):
        validate::validate_view_expression(expression)?;

        let (relation_deps, view_deps) = dependency::expression_dependencies(expression);

        let mut entry = ViewEntry::new(ViewInstance::new(expression.clone()));
        let reference = ViewRef(self.view_counter);

        // track relation dependencies of this view:
        for r in relation_deps.into_iter() {
            self.relations
                .get_mut(&r)
                .map(|rs| rs.add_dependent_view(reference.clone()));
            entry.dependee_relations.insert(r);
        }

        // track view dependencies of this view:
        for r in view_deps.into_iter() {
            self.views
                .get_mut(&r)
                .map(|rs| rs.add_dependent_view(reference.clone()));
            entry.dependee_views.insert(r.clone());
        }

        entry.instance.initialize(self)?;

        self.views.insert(reference.clone(), entry);
        self.view_counter += 1;

        Ok(View::new(reference))
    }

    /// Returns the instance for `view` if it exists.
    fn view_instance<T, E>(&self, view: &View<T, E>) -> Result<&Instance<T>, Error>
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static,
    {
        let result = self
            .views
            .get(view.reference())
            .and_then(|v| v.instance.as_any().downcast_ref::<ViewInstance<T, E>>())
            .ok_or(Error::InstanceNotFound {
                name: format!("{:?}", view.reference()),
            })?;
        Ok(result.instance())
    }

    /// Stabilizes the view identified by `view_ref` by stabilizing its dependees and
    /// dependencies. It also applies `changed()` on the view's instance, moving all
    /// relevant `to_add` tuples to `recent` and `recent` tuples to `stable`.
    fn stabilize_view(&self, view_ref: &ViewRef) -> Result<(), Error> {
        if let Some(entry) = self.views.get(view_ref) {
            // do nothing if the view is already stabilizing:
            if entry.stabilizing.get() {
                return Ok(());
            }

            entry.stabilizing.set(true);

            for r in entry.dependee_relations.iter() {
                self.stabilize_relation(r)?;
            }
            for r in entry.dependee_views.iter() {
                self.stabilize_view(r)?;
            }

            while entry.instance.instance().changed() {
                for r in entry.dependent_views.iter() {
                    self.views.get(r).unwrap().instance.stabilize(&self)?;
                    self.stabilize_view(r)?;
                }
            }

            entry.stabilizing.set(false);
        }

        Ok(())
    }

    /// Stabilizes the relation identified by `name`. It also stabilizes
    /// all views depending on this `name`.
    fn stabilize_relation(&self, name: &str) -> Result<(), Error> {
        if let Some(entry) = self.relations.get(name) {
            // do nothing if relation is already stabilizing:
            if entry.stabilizing.get() {
                return Ok(());
            }

            entry.stabilizing.set(true);

            while entry.instance.changed() {
                for r in entry.dependent_views.iter() {
                    self.views.get(r).unwrap().instance.stabilize(&self)?;
                    self.stabilize_view(r)?;
                }
            }

            entry.stabilizing.set(false);
        }

        Ok(())
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        let mut relations = HashMap::new();
        let mut views = HashMap::new();

        self.relations.iter().for_each(|(k, v)| {
            relations.insert(k.clone(), v.clone());
        });
        self.views.iter().for_each(|(k, v)| {
            views.insert(k.clone(), v.clone());
        });

        Self {
            relations,
            views,
            view_counter: self.view_counter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Join, Project, Select};

    #[test]
    fn test_insert() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add()[0]
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_ok());
            assert!(database.insert(&r, vec![1, 4].into()).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add()[0]
            );
            assert_eq!(
                Tuples::<i32>::from(vec![1, 4]),
                database.relation_instance(&r).unwrap().to_add()[1]
            );
        }
        {
            let database = Database::new();
            let r = Database::new().add_relation("r").unwrap(); // dummy database
            assert!(database.insert(&r, vec![1, 2, 3].into()).is_err());
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
    fn test_clone_database() {
        {
            let database = Database::new();
            let cloned = database.clone();
            assert!(cloned.relations.is_empty());
            assert!(cloned.views.is_empty());
            assert_eq!(0, cloned.view_counter);
        }
        {
            let mut database = Database::new();
            let a = database.add_relation::<i32>("a").unwrap();
            let v = database.store_view(&a).unwrap();
            database.insert(&a, vec![1, 2, 3].into()).unwrap();

            let cloned = database.clone();
            database.insert(&a, vec![1, 4].into()).unwrap();

            assert_eq!(
                vec![1, 2, 3, 4],
                database.evaluate(&v).unwrap().into_tuples()
            );
            assert_eq!(vec![1, 2, 3], cloned.evaluate(&v).unwrap().into_tuples());

            cloned.insert(&a, vec![1, 5].into()).unwrap();
            assert_eq!(
                vec![1, 2, 3, 4],
                database.evaluate(&v).unwrap().into_tuples()
            );
            assert_eq!(vec![1, 2, 3, 5], cloned.evaluate(&v).unwrap().into_tuples());
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
            let a = database.add_relation::<i32>("a").unwrap();
            database.store_view(&a).unwrap();
            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }
        {
            let mut database = Database::new();
            let _ = database.add_relation::<i32>("a").unwrap();
            database.store_view(&Relation::<i32>::new("a")).unwrap();
            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }
        {
            let mut database = Database::new();
            assert!(database.store_view(&Relation::<i32>::new("a")).is_err());
        }

        {
            let mut database = Database::new();
            let a = database.add_relation::<i32>("a").unwrap();
            database.store_view(&Select::new(&a, |&t| t != 0)).unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            let a = database.add_relation::<i32>("a").unwrap();
            database.store_view(&Project::new(&a, |t| t + 1)).unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            let a = database.add_relation::<(i32, i32)>("a").unwrap();
            let b = database.add_relation::<(i32, i32)>("b").unwrap();
            database
                .store_view(&Join::new(&a, &b, |t| t.0, |t| t.0, |_, &l, &r| (l, r)))
                .unwrap();

            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }

        {
            let mut database = Database::new();
            let a = database.add_relation::<i32>("a").unwrap();
            let view = database.store_view(&a).unwrap();
            database.store_view(&view).unwrap();
            assert!(database.views.get(&ViewRef(0)).is_some());
            assert!(database.views.get(&ViewRef(1)).is_some());
            assert!(database.views.get(&ViewRef(1000)).is_none());
        }
    }

    #[test]
    fn test_get_view() {
        let mut database = Database::new();
        let _ = database.add_relation::<i32>("a").unwrap();
        let view = database.store_view(&Relation::<i32>::new("a")).unwrap();

        assert!(database.view_instance(&view).is_ok());
    }
}

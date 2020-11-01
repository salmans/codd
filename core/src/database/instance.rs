use super::{evaluate, expression_ext::ExpressionExt, helpers::gallop, Database};
use crate::{expression::Expression, Error, Tuple};
use std::any::Any;
use std::{
    cell::{Ref, RefCell},
    ops::Deref,
    rc::Rc,
};

/// Is a wrapper around a vector of tuples. As an invariant, the content of `Tuples` is sorted.
///
/// **Note**: `Tuples` is borrowed from `Relation` in [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[derive(Clone, Debug, PartialEq)]
pub struct Tuples<T: Tuple> {
    /// Is the vector of tuples in this instance.
    items: Vec<T>,
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
    /// Merges the instances of the reciver with `other` and returns a new `Tuples`
    /// instance.
    pub(crate) fn merge(self, other: Self) -> Self {
        let mut tuples = Vec::with_capacity(self.items.len() + other.items.len());
        tuples.extend(self.items.into_iter());
        tuples.extend(other.items.into_iter());
        tuples.into()
    }

    /// Returns an immutable reference to the tuples of the receiver.
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Consumes the receiver and returns the underlying (sorted) vector of tuples.
    #[inline(always)]
    pub fn into_tuples(self) -> Vec<T> {
        self.items
    }
}

impl<T: Tuple> Deref for Tuples<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl<T: Tuple> core::ops::DerefMut for Tuples<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

/// Is used to store database `Instance`s in a map by hiding their (generic) type.
pub(super) trait DynInstance {
    /// Returns the instance as `Any`
    fn as_any(&self) -> &dyn Any;

    /// Returns true if the instance has been affected by last updates. It also moves all
    /// `to_add` tuples to `recent` and `recent` tuples to `stable`.
    fn changed(&self) -> bool;

    /// Clones the instance in a `Box`.
    fn clone_box(&self) -> Box<dyn DynInstance>;
}

/// Is used to store `ViewInstance`s in a map by hiding their (generic) types.
pub(super) trait DynViewInstance {
    /// Returns the view instance as `Any`.
    fn as_any(&self) -> &dyn Any;

    /// Returns the `Instance` storing the tuples of the view as a trait object.
    fn instance(&self) -> &dyn DynInstance;

    /// Initializes the view with the existing tuples in `db`.
    fn initialize(&self, db: &Database) -> Result<(), Error>;

    /// Stabilizes the view from the `recent` tuples in the instances of `db`.
    fn stabilize(&self, db: &Database) -> Result<(), Error>;

    /// Clones the instance in a `Box`.
    fn clone_box(&self) -> Box<dyn DynViewInstance>;
}

/// Contains the tuples of a relation in the database.
///
/// **Note**: `Instance` is a replica of `Variable` in [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[derive(Debug, PartialEq)]
pub(super) struct Instance<T: Tuple> {
    /// Is the set of tuples that are already considered when updating views.
    stable: Rc<RefCell<Vec<Tuples<T>>>>,

    /// Is the set of tuples that have not yet been reflected in views.
    recent: Rc<RefCell<Tuples<T>>>,

    /// Is the set of tuples to add: they may be duplicates of existing tuples
    /// in which case they are ignored.
    to_add: Rc<RefCell<Vec<Tuples<T>>>>,
}

impl<T: Tuple> Instance<T> {
    /// Creates a new empty isntance.
    pub fn new() -> Self {
        Self {
            stable: Rc::new(RefCell::new(Vec::new())),
            recent: Rc::new(RefCell::new(Vec::new().into())),
            to_add: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Adds a `Tuples` instance to `to_add` tuples. These tuples will be ultimately
    /// added to the instance if they already don't exist.
    pub fn insert(&self, tuples: Tuples<T>) {
        if !tuples.is_empty() {
            self.to_add.borrow_mut().push(tuples);
        }
    }

    /// Returns an immutable reference (of type `std::cell::Ref`) to the stable tuples
    /// of this instance.
    #[inline(always)]
    pub fn stable(&self) -> Ref<Vec<Tuples<T>>> {
        self.stable.borrow()
    }

    /// Returns an immutable reference (of type `std::cell::Ref`) to the recent tuples
    /// of this instance.
    #[inline(always)]
    pub fn recent(&self) -> Ref<Tuples<T>> {
        self.recent.borrow()
    }

    /// Returns an immutable reference (of type `std::cell::Ref`) to the candidates to
    /// be added to the recent tuples of this instance (if they already don't exist).
    #[inline(always)]
    pub fn to_add(&self) -> Ref<Vec<Tuples<T>>> {
        self.to_add.borrow()
    }
}

impl<T: Tuple> Clone for Instance<T> {
    fn clone(&self) -> Self {
        Self {
            stable: Rc::new(RefCell::new(self.stable.borrow().clone())),
            recent: Rc::new(RefCell::new(self.recent.borrow().clone())),
            to_add: Rc::new(RefCell::new(self.to_add.borrow().clone())),
        }
    }
}

impl<T> DynInstance for Instance<T>
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
                    slice.is_empty() || &slice[0] != x
                });
            }
            *self.recent.borrow_mut() = to_add;
        }

        !self.recent.borrow().is_empty()
    }

    fn clone_box(&self) -> Box<dyn DynInstance> {
        let mut to_add = Vec::new();
        for batch in self.to_add.borrow().iter() {
            to_add.push(batch.clone());
        }

        let recent = (*self.recent.borrow()).clone();

        let mut stable: Vec<Tuples<T>> = Vec::new();
        for batch in self.stable.borrow().iter() {
            stable.push(batch.clone());
        }

        Box::new(Self {
            stable: Rc::new(RefCell::new(stable)),
            recent: Rc::new(RefCell::new(recent)),
            to_add: Rc::new(RefCell::new(to_add)),
        })
    }
}

/// Is a wrapper around the `Instance` storing the tuples of a view and
/// the relational expression to which the view evaluates.
pub(super) struct ViewInstance<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    /// Is the `Instance` storing the tuples of the view.
    instance: Instance<T>,

    /// Is the view expression.
    expression: E,
}

impl<T, E> ViewInstance<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    pub fn new(expression: E) -> Self {
        Self {
            instance: Instance::new(),
            expression,
        }
    }

    /// Returns the `Instance` storing the tuples of this view.
    pub fn instance(&self) -> &Instance<T> {
        &self.instance
    }
}

impl<T, E> DynViewInstance for ViewInstance<T, E>
where
    T: Tuple + 'static,
    E: ExpressionExt<T> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn instance(&self) -> &dyn DynInstance {
        &self.instance
    }

    fn initialize(&self, db: &Database) -> Result<(), Error> {
        let incremental = evaluate::IncrementalCollector::new(db);
        let stable = self.expression.collect_stable(&incremental)?;

        for batch in stable {
            self.instance.insert(batch);
        }
        Ok(())
    }

    fn stabilize(&self, db: &Database) -> Result<(), Error> {
        let incremental = evaluate::IncrementalCollector::new(db);
        let recent = self.expression.collect_recent(&incremental)?;

        self.instance.insert(recent);
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn DynViewInstance> {
        Box::new(Self {
            instance: self.instance.clone(),
            expression: self.expression.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clone_instance() {
        {
            let instance = Instance::<bool>::new();
            assert_eq!(instance, instance.clone());
        }
        {
            let instance = Instance::<i32> {
                stable: Rc::new(RefCell::new(vec![vec![1, 2].into()])),
                recent: Rc::new(RefCell::new(vec![2, 3, 4].into())),
                to_add: Rc::new(RefCell::new(vec![vec![4, 5].into()])),
            };
            let cloned = instance.clone();
            assert_eq!(instance, cloned);
        }
    }

    #[test]
    fn test_tuples_from_list() {
        {
            let tuples = Tuples::<i32>::from(vec![]);
            assert_eq!(Vec::<i32>::new(), tuples.items());
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4, 2, 1, 3]);
            assert_eq!(vec![1, 2, 3, 4, 5], tuples.items());
        }
        {
            let tuples = Tuples::<i32>::from(vec![3, 2, 2, 1, 3]);
            assert_eq!(vec![1, 2, 3], tuples.items());
        }
    }

    #[test]
    fn test_tuples_merge() {
        {
            let tuples = Tuples::<i32>::from(vec![]);
            assert_eq!(Vec::<i32>::new(), tuples.merge(vec![].into()).items());
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4]);
            assert_eq!(vec![2, 3, 4, 5], tuples.merge(vec![2, 3].into()).items());
        }
        {
            let tuples = Tuples::<i32>::from(vec![5, 4, 4]);
            assert_eq!(vec![3, 4, 5], tuples.merge(vec![5, 3].into()).items());
        }
    }

    #[test]
    fn test_instance_insert() {
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
    fn test_instance_changed() {
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
}

//! Storage for relations and views: weighted, sorted batches with incremental update.
use super::{evaluate, expression_ext::ExpressionExt, helpers, Database};
use crate::{expression::Expression, Error, Tuple};
use std::{
    any::Any,
    cell::{Ref, RefCell},
    ops::Deref,
    rc::Rc,
};

/// Is a sorted set of tuples without duplicates: the result of evaluating an expression and
/// the argument of [`Database::insert`] and [`Database::retract`].
///
/// **Note**: [`Tuples`] is borrowed from `Relation` in [`datafrog`].
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

/// Is a sorted, consolidated list of weighted tuples (a Z-set): every tuple appears at most
/// once and no weight is zero. Batches are the unit of storage and of incremental change.
#[derive(Clone, Debug, PartialEq)]
pub struct Batch<T: Tuple> {
    items: Vec<(T, isize)>,
}

impl<T: Tuple> Batch<T> {
    /// Creates an empty batch.
    pub fn empty() -> Self {
        Self { items: Vec::new() }
    }

    /// Creates a batch from arbitrary weighted tuples, summing the weights of equal tuples
    /// and dropping tuples whose total weight is zero.
    pub fn from_weighted(mut items: Vec<(T, isize)>) -> Self {
        items.sort_by(|a, b| a.0.cmp(&b.0));
        let mut result: Vec<(T, isize)> = Vec::with_capacity(items.len());
        for (t, w) in items {
            match result.last_mut() {
                Some(last) if last.0 == t => last.1 += w,
                _ => result.push((t, w)),
            }
        }
        result.retain(|(_, w)| *w != 0);
        Self { items: result }
    }

    /// Creates a batch that assigns `weight` to every tuple of a set.
    pub fn from_set(tuples: &Tuples<T>, weight: isize) -> Self {
        if weight == 0 {
            return Self::empty();
        }
        Self::from_sorted(tuples.iter().map(|t| (t.clone(), weight)).collect())
    }

    /// Creates a batch from a sorted, consolidated list. The caller guarantees the invariant.
    pub(crate) fn from_sorted(items: Vec<(T, isize)>) -> Self {
        debug_assert!(items.windows(2).all(|w| w[0].0 < w[1].0));
        debug_assert!(items.iter().all(|(_, w)| *w != 0));
        Self { items }
    }

    /// Returns the weighted tuples of the batch.
    #[inline(always)]
    pub fn items(&self) -> &[(T, isize)] {
        &self.items
    }

    /// Consumes the batch and returns its weighted tuples.
    #[inline(always)]
    pub fn into_items(self) -> Vec<(T, isize)> {
        self.items
    }

    /// Returns true if the batch has no tuples.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of tuples in the batch.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns the weight of `tuple` in the batch (zero if absent).
    pub fn weight_of(&self, tuple: &T) -> isize {
        match self.items.binary_search_by(|x| x.0.cmp(tuple)) {
            Ok(i) => self.items[i].1,
            Err(_) => 0,
        }
    }

    /// Returns the tuples of the batch, without weights, as a sorted list.
    pub fn keys(&self) -> Vec<T> {
        self.items.iter().map(|(t, _)| t.clone()).collect()
    }

    /// Merges two batches by adding weights.
    pub fn merge(self, other: Self) -> Self {
        let mut result = Vec::with_capacity(self.items.len() + other.items.len());
        let mut left = self.items.into_iter().peekable();
        let mut right = other.items.into_iter().peekable();
        loop {
            use std::cmp::Ordering;
            match (left.peek(), right.peek()) {
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    Ordering::Less => result.push(left.next().unwrap()),
                    Ordering::Greater => result.push(right.next().unwrap()),
                    Ordering::Equal => {
                        let (t, wl) = left.next().unwrap();
                        let (_, wr) = right.next().unwrap();
                        if wl + wr != 0 {
                            result.push((t, wl + wr));
                        }
                    }
                },
                (Some(_), None) => result.push(left.next().unwrap()),
                (None, Some(_)) => result.push(right.next().unwrap()),
                (None, None) => break,
            }
        }
        Self { items: result }
    }

    /// Keys the batch for joining.
    pub(crate) fn keyed<K: Tuple>(&self, key: impl Fn(&T) -> K) -> Keyed<'_, K, T> {
        let mut items: Vec<(K, &T, isize)> =
            self.items.iter().map(|(t, w)| (key(t), t, *w)).collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Keyed { items }
    }

    /// Returns the tuples with a positive weight as a set.
    pub fn positives(&self) -> Tuples<T> {
        Tuples {
            items: self
                .items
                .iter()
                .filter(|(_, w)| *w > 0)
                .map(|(t, _)| t.clone())
                .collect(),
        }
    }
}

/// Is a list of `(key, tuple, weight)` entries of a batch, sorted by key.
pub(crate) struct Keyed<'a, K, T> {
    items: Vec<(K, &'a T, isize)>,
}

impl<'a, K, T> Keyed<'a, K, T> {
    /// Returns the keyed entries.
    #[inline(always)]
    pub(crate) fn items(&self) -> &[(K, &'a T, isize)] {
        &self.items
    }
}

/// Sums a list of batches into one.
pub(crate) fn consolidate<T: Tuple>(batches: &[Rc<Batch<T>>]) -> Batch<T> {
    batches
        .iter()
        .fold(Batch::empty(), |acc, b| acc.merge((**b).clone()))
}

/// Is used to store instances of a database in a map by hiding their (generic) type.
pub(super) trait DynInstance {
    /// Returns the instance as [`Any`]
    fn as_any(&self) -> &dyn Any;

    /// Returns true if the instance has been affected by last updates. It also moves all
    /// `to_add` tuples to `recent` and `recent` tuples to `stable`.
    fn changed(&self) -> bool;

    /// Clones the instance in a [`Box`].
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

    /// Clones the instance in a [`Box`].
    fn clone_box(&self) -> Box<dyn DynViewInstance>;
}

/// Contains the weighted tuples of a relation or a view.
///
/// The content of an instance is the sum of its `stable` batches. Changes arrive in `to_add`,
/// are consolidated into `recent` by [`changed`](DynInstance::changed) so that dependent views
/// can be updated from them, and are then folded into `stable`.
///
/// A relation has *set semantics*: its weights are kept at zero or one, so inserting a tuple
/// twice or retracting an absent tuple is a no-op. A view keeps the number of derivations of
/// each tuple as its weight.
///
/// **Note**: `Instance` mirrors `Variable` in [`datafrog`].
///
/// [`datafrog`]: https://github.com/rust-lang/datafrog
#[derive(Debug)]
pub(super) struct Instance<T: Tuple> {
    /// Batches already reflected in dependent views. Batches are immutable and shared.
    stable: RefCell<Vec<Rc<Batch<T>>>>,

    /// The latest change, not yet reflected in dependent views.
    recent: RefCell<Rc<Batch<T>>>,

    /// Pending changes.
    to_add: RefCell<Vec<Batch<T>>>,

    /// How the weight of a tuple accumulates.
    weights: Weights,
}

/// Is how an [`Instance`] accumulates the weight of a tuple.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Weights {
    /// Weights stay zero or one: the set semantics of a relation.
    Set,
    /// Weights count derivations, as in a view.
    Counts,
}

impl Weights {
    /// Returns the weight of a tuple of weight `old` after a change of `weight`.
    fn apply(self, old: isize, weight: isize) -> isize {
        match self {
            Weights::Set => (old + weight > 0) as isize,
            Weights::Counts => old + weight,
        }
    }
}

impl<T: Tuple> Instance<T> {
    /// Creates a new empty instance.
    pub fn new(weights: Weights) -> Self {
        Self {
            stable: RefCell::new(Vec::new()),
            recent: RefCell::new(Rc::new(Batch::empty())),
            to_add: RefCell::new(Vec::new()),
            weights,
        }
    }

    /// Queues a change to the instance.
    pub fn insert(&self, batch: Batch<T>) {
        if !batch.is_empty() {
            self.to_add.borrow_mut().push(batch);
        }
    }

    /// Returns the stable batches of this instance.
    #[inline(always)]
    pub fn stable(&self) -> Ref<'_, Vec<Rc<Batch<T>>>> {
        self.stable.borrow()
    }

    /// Returns the recent change of this instance.
    #[inline(always)]
    pub fn recent(&self) -> Ref<'_, Rc<Batch<T>>> {
        self.recent.borrow()
    }

    /// Returns the pending changes of this instance.
    #[cfg(test)]
    pub fn to_add(&self) -> Ref<'_, Vec<Batch<T>>> {
        self.to_add.borrow()
    }

    /// Returns the total stable weight of each of the sorted `keys`.
    pub fn counts_stable(&self, keys: &[T]) -> Vec<isize> {
        helpers::counts_in(&self.stable.borrow(), keys)
    }
}

impl<T: Tuple> Clone for Instance<T> {
    fn clone(&self) -> Self {
        Self {
            stable: RefCell::new(self.stable.borrow().clone()),
            recent: RefCell::new(self.recent.borrow().clone()),
            to_add: RefCell::new(self.to_add.borrow().clone()),
            weights: self.weights,
        }
    }
}

impl<T: Tuple> PartialEq for Instance<T> {
    fn eq(&self, other: &Self) -> bool {
        *self.stable.borrow() == *other.stable.borrow()
            && *self.recent.borrow() == *other.recent.borrow()
            && *self.to_add.borrow() == *other.to_add.borrow()
            && self.weights == other.weights
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
        // fold the previous change into the stable batches, keeping them geometrically sized:
        if !self.recent.borrow().is_empty() {
            let recent = std::mem::replace(&mut *self.recent.borrow_mut(), Rc::new(Batch::empty()));
            let mut recent = Rc::try_unwrap(recent).unwrap_or_else(|rc| (*rc).clone());
            while self
                .stable
                .borrow()
                .last()
                .map(|x| x.len() <= 2 * recent.len())
                == Some(true)
            {
                let last = self.stable.borrow_mut().pop().unwrap();
                let last = Rc::try_unwrap(last).unwrap_or_else(|rc| (*rc).clone());
                recent = recent.merge(last);
            }
            if !recent.is_empty() {
                self.stable.borrow_mut().push(Rc::new(recent));
            }
        }

        // fold the pending changes, in order, into the next change. Order matters for a
        // relation: inserting a present tuple is a no-op, so an insert followed by a retract
        // must remove the tuple even though the weights sum to zero.
        let pending: Vec<Batch<T>> = std::mem::take(&mut *self.to_add.borrow_mut());
        if !pending.is_empty() {
            let mut keys: Vec<T> = pending
                .iter()
                .flat_map(|b| b.items().iter().map(|(t, _)| t.clone()))
                .collect();
            keys.sort_unstable();
            keys.dedup();
            let mut current = self.counts_stable(&keys);
            let mut delta = vec![0isize; keys.len()];
            for batch in pending {
                for (t, w) in batch.items() {
                    let i = keys.binary_search(t).unwrap();
                    let old = current[i];
                    let new = self.weights.apply(old, *w);
                    delta[i] += new - old;
                    current[i] = new;
                }
            }
            let items = keys
                .into_iter()
                .zip(delta)
                .filter(|(_, w)| *w != 0)
                .collect();
            *self.recent.borrow_mut() = Rc::new(Batch::from_sorted(items));
        }

        !self.recent.borrow().is_empty()
    }

    fn clone_box(&self) -> Box<dyn DynInstance> {
        Box::new(self.clone())
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
            instance: Instance::new(Weights::Counts),
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
            self.instance
                .insert(Rc::try_unwrap(batch).unwrap_or_else(|rc| (*rc).clone()));
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

    fn batch<T: Tuple>(items: Vec<(T, isize)>) -> Batch<T> {
        Batch::from_weighted(items)
    }

    #[test]
    fn test_tuples_from_list() {
        assert_eq!(Vec::<i32>::new(), Tuples::<i32>::from(vec![]).items());
        assert_eq!(
            vec![1, 2, 3, 4, 5],
            Tuples::<i32>::from(vec![5, 4, 2, 1, 3]).items()
        );
        assert_eq!(
            vec![1, 2, 3],
            Tuples::<i32>::from(vec![3, 2, 2, 1, 3]).items()
        );
    }

    #[test]
    fn test_batch_from_weighted_consolidates() {
        let b = batch(vec![(3, 1), (1, 2), (3, -1), (2, 1), (2, -1)]);
        assert_eq!(vec![(1, 2)], b.items());
        assert_eq!(2, b.weight_of(&1));
        assert_eq!(0, b.weight_of(&2));
    }

    #[test]
    fn test_batch_merge() {
        let l = batch(vec![(1, 1), (2, 1), (4, 1)]);
        let r = batch(vec![(2, -1), (3, 1), (4, 2)]);
        assert_eq!(vec![(1, 1), (3, 1), (4, 3)], l.merge(r).items());
    }

    #[test]
    fn test_relation_set_semantics() {
        let instance = Instance::<i32>::new(Weights::Set);
        instance.insert(batch(vec![(1, 1), (2, 1)]));
        instance.insert(batch(vec![(2, 1), (3, 1)]));
        assert!(instance.changed());
        assert_eq!(vec![(1, 1), (2, 1), (3, 1)], instance.recent().items());

        // duplicates and retractions of absent tuples are no-ops; weights stay at one:
        instance.insert(batch(vec![(1, 1), (4, -1), (3, -1)]));
        assert!(instance.changed());
        assert_eq!(vec![(3, -1)], instance.recent().items());
        assert!(!instance.changed());
        assert_eq!(
            vec![(1, 1), (2, 1)],
            consolidate(&instance.stable()).items()
        );
        assert_eq!(
            vec![1, 2],
            instance
                .counts_stable(&[1, 2, 3])
                .iter()
                .enumerate()
                .filter(|(_, c)| **c > 0)
                .map(|(i, _)| i + 1)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_view_keeps_derivation_counts() {
        let instance = Instance::<i32>::new(Weights::Counts);
        instance.insert(batch(vec![(1, 2)]));
        assert!(instance.changed());
        instance.insert(batch(vec![(1, -1)]));
        assert!(instance.changed());
        assert_eq!(vec![(1, -1)], instance.recent().items());
        assert!(!instance.changed());
        assert_eq!(vec![(1, 1)], consolidate(&instance.stable()).items());
    }

    #[test]
    fn test_clone_shares_nothing_mutable() {
        let instance = Instance::<i32>::new(Weights::Set);
        instance.insert(batch(vec![(1, 1)]));
        instance.changed();
        let cloned = instance.clone();
        cloned.insert(batch(vec![(2, 1)]));
        cloned.changed();
        cloned.changed(); // the second call folds the change into the stable batches
        instance.changed();
        assert_eq!(vec![(1, 1)], consolidate(&instance.stable()).items());
        assert_eq!(vec![(1, 1), (2, 1)], consolidate(&cloned.stable()).items());
    }
}

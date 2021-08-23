use crate::{
    expression::{view::ViewRef, *},
    Error, Tuple, Tuples,
};

/// Extends [`Expression`] with methods required for incremental database update.
pub trait ExpressionExt<T: Tuple>: Expression<T> {
    /// Visits this node by a [`RecentCollector`] and returns the recent tuples of the
    /// database according to the logic implemented by `collector`.
    ///
    /// **Note**:
    /// Recent tuples are those tuples that got inserted into relation instances of a
    /// database before any dependent (materialized) views are updated.
    fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: RecentCollector;

    /// Visits this node by a [`StableCollector`] and returns the stable tuples of the
    /// database according to the logic implemented by `collector`.
    ///
    /// **Note**:
    /// Stable tuples are those tuples that have already been reflected in (materialized)
    /// views that are affected by those tuples.
    fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: StableCollector;

    /// Returns an iterator over the relation dependencies of this expression. These are
    /// the name of relations that show up in the receiver expression.
    fn relation_dependencies(&self) -> &[String];

    /// Returns an iterator over the view dependencies of this expression. These are
    /// references to views that show up in the receiver expression.
    fn view_dependencies(&self) -> &[ViewRef];
}

impl<T, E> ExpressionExt<T> for &E
where
    T: Tuple,
    E: ExpressionExt<T>,
{
    fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: RecentCollector,
    {
        (*self).collect_recent(collector)
    }

    fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: StableCollector,
    {
        (*self).collect_stable(collector)
    }

    fn relation_dependencies(&self) -> &[String] {
        (*self).relation_dependencies()
    }

    fn view_dependencies(&self) -> &[ViewRef] {
        (*self).view_dependencies()
    }
}

impl<T, E> ExpressionExt<T> for Box<E>
where
    T: Tuple,
    E: ExpressionExt<T>,
{
    fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: RecentCollector,
    {
        (**self).collect_recent(collector)
    }

    fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: StableCollector,
    {
        (**self).collect_stable(collector)
    }

    fn relation_dependencies(&self) -> &[String] {
        (**self).relation_dependencies()
    }

    fn view_dependencies(&self) -> &[ViewRef] {
        (**self).view_dependencies()
    }
}

/// Is the trait of objects that implement the logic for collecting the recent tuples of
/// a database when the visited expression is evaluated.
pub trait RecentCollector {
    /// Collects the recent tuples for the [`Full`] expression.
    fn collect_full<T>(&self, full: &Full<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    /// Collects the recent tuples for the [`Empty`] expression.
    fn collect_empty<T>(&self, empty: &Empty<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    /// Collects the recent tuples for a [`Singleton`] expression.
    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    /// Collects the recent tuples for a [`Relation`] expression.
    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static;

    /// Collects the recent tuples for a [`Select`] expression.
    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>;

    /// Collects the recent tuples for a [`Union`] expression.    
    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the recent tuples for an [`Intersect`] expression.    
    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the recent tuples for a [`Difference`] expression.    
    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the recent tuples for a [`Project`] expression.    
    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>;

    /// Collects the recent tuples for a [`Product`] expression.    
    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>;

    /// Collects the recent tuples for a [`Join`] expression.    
    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>;

    /// Collects the recent tuples for a [`View`] expression.
    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static;
}

/// Is the trait of objects that implement the logic for collecting the stable tuples of
/// a database when the visited expression is evaluated.
pub trait StableCollector {
    /// Collects the stable tuples for the [`Full`] expression.    
    fn collect_full<T>(&self, full: &Full<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    /// Collects the stable tuples for the [`Empty`] expression.        
    fn collect_empty<T>(&self, empty: &Empty<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    /// Collects the stable tuples for a [`Singleton`] expression.        
    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    /// Collects the stable tuples for a [`Relation`] expression.            
    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static;

    /// Collects the stable tuples for a [`Select`] expression.            
    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>;

    /// Collects the stable tuples for a [`Union`] expression.            
    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the stable tuples for an [`Intersect`] expression.            
    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the stable tuples for a [`Difference`] expression.            
    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>;

    /// Collects the stable tuples for a [`Project`] expression.            
    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>;

    /// Collects the stable tuples for a [`Product`] expression.            
    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>;

    /// Collects the stable tuples for a [`Join`] expression.            
    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>;

    /// Collects the stable tuples for a [`View`] expression.            
    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static;
}

mod r#impl {
    use super::{ExpressionExt, RecentCollector, StableCollector};
    use crate::{
        expression::view::{View, ViewRef},
        Error, Tuple, Tuples,
    };

    impl<T, E> ExpressionExt<T> for View<T, E>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_view(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_view(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            &[]
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Intersect;

    impl<T, L, R> ExpressionExt<T> for Intersect<T, L, R>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_intersect(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_intersect(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Union;

    impl<T, L, R> ExpressionExt<T> for Union<T, L, R>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_union(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_union(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Difference;

    impl<T, L, R> ExpressionExt<T> for Difference<T, L, R>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_difference(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_difference(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Empty;

    impl<T> ExpressionExt<T> for Empty<T>
    where
        T: Tuple,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_empty(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_empty(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            &[]
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            &[]
        }
    }

    use crate::expression::Full;

    impl<T> ExpressionExt<T> for Full<T>
    where
        T: Tuple,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_full(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_full(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            &[]
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            &[]
        }
    }

    use crate::expression::Join;

    impl<K, L, R, Left, Right, T> ExpressionExt<T> for Join<K, L, R, Left, Right, T>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_join(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_join(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Mono;

    impl<T: Tuple + 'static> ExpressionExt<T> for Mono<T> {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            match self {
                Mono::Full(exp) => exp.collect_recent(collector),
                Mono::Empty(exp) => exp.collect_recent(collector),
                Mono::Singleton(exp) => exp.collect_recent(collector),
                Mono::Relation(exp) => exp.collect_recent(collector),
                Mono::Select(exp) => exp.collect_recent(collector),
                Mono::Project(exp) => exp.collect_recent(collector),
                Mono::Union(exp) => exp.collect_recent(collector),
                Mono::Intersect(exp) => exp.collect_recent(collector),
                Mono::Difference(exp) => exp.collect_recent(collector),
                Mono::Product(exp) => exp.collect_recent(collector),
                Mono::Join(exp) => exp.collect_recent(collector),
                Mono::View(exp) => exp.collect_recent(collector),
            }
        }
        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            match self {
                Mono::Full(exp) => exp.collect_stable(collector),
                Mono::Empty(exp) => exp.collect_stable(collector),
                Mono::Singleton(exp) => exp.collect_stable(collector),
                Mono::Relation(exp) => exp.collect_stable(collector),
                Mono::Select(exp) => exp.collect_stable(collector),
                Mono::Project(exp) => exp.collect_stable(collector),
                Mono::Union(exp) => exp.collect_stable(collector),
                Mono::Intersect(exp) => exp.collect_stable(collector),
                Mono::Difference(exp) => exp.collect_stable(collector),
                Mono::Product(exp) => exp.collect_stable(collector),
                Mono::Join(exp) => exp.collect_stable(collector),
                Mono::View(exp) => exp.collect_stable(collector),
            }
        }

        fn relation_dependencies(&self) -> &[String] {
            match self {
                Mono::Full(exp) => exp.relation_dependencies(),
                Mono::Empty(exp) => exp.relation_dependencies(),
                Mono::Singleton(exp) => exp.relation_dependencies(),
                Mono::Relation(exp) => exp.relation_dependencies(),
                Mono::Select(exp) => exp.relation_dependencies(),
                Mono::Project(exp) => exp.relation_dependencies(),
                Mono::Union(exp) => exp.relation_dependencies(),
                Mono::Intersect(exp) => exp.relation_dependencies(),
                Mono::Difference(exp) => exp.relation_dependencies(),
                Mono::Product(exp) => exp.relation_dependencies(),
                Mono::Join(exp) => exp.relation_dependencies(),
                Mono::View(exp) => exp.relation_dependencies(),
            }
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            match self {
                Mono::Full(exp) => exp.view_dependencies(),
                Mono::Empty(exp) => exp.view_dependencies(),
                Mono::Singleton(exp) => exp.view_dependencies(),
                Mono::Relation(exp) => exp.view_dependencies(),
                Mono::Select(exp) => exp.view_dependencies(),
                Mono::Project(exp) => exp.view_dependencies(),
                Mono::Union(exp) => exp.view_dependencies(),
                Mono::Intersect(exp) => exp.view_dependencies(),
                Mono::Difference(exp) => exp.view_dependencies(),
                Mono::Product(exp) => exp.view_dependencies(),
                Mono::Join(exp) => exp.view_dependencies(),
                Mono::View(exp) => exp.view_dependencies(),
            }
        }
    }

    use crate::expression::Product;

    impl<L, R, Left, Right, T> ExpressionExt<T> for Product<L, R, Left, Right, T>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_product(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_product(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Project;

    impl<S, T, E> ExpressionExt<T> for Project<S, T, E>
    where
        S: Tuple,
        T: Tuple,
        E: ExpressionExt<S>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_project(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_project(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Relation;

    impl<T> ExpressionExt<T> for Relation<T>
    where
        T: Tuple + 'static,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_relation(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_relation(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            &[]
        }
    }

    use crate::expression::Select;

    impl<T, E> ExpressionExt<T> for Select<T, E>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_select(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_select(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            self.relation_deps()
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            self.view_deps()
        }
    }

    use crate::expression::Singleton;

    impl<T> ExpressionExt<T> for Singleton<T>
    where
        T: Tuple,
    {
        fn collect_recent<C>(&self, collector: &C) -> Result<crate::Tuples<T>, Error>
        where
            C: RecentCollector,
        {
            collector.collect_singleton(&self)
        }

        fn collect_stable<C>(&self, collector: &C) -> Result<Vec<crate::Tuples<T>>, Error>
        where
            C: StableCollector,
        {
            collector.collect_singleton(&self)
        }

        fn relation_dependencies(&self) -> &[String] {
            &[]
        }

        fn view_dependencies(&self) -> &[ViewRef] {
            &[]
        }
    }
}

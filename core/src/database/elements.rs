use super::{RelationRef, ViewRef};
use crate::{expression::Visitor, Tuple};

pub struct Elements {
    relations: Vec<RelationRef>,
    views: Vec<ViewRef>,
}

impl Elements {
    pub fn new() -> Self {
        Self {
            relations: Vec::new(),
            views: Vec::new(),
        }
    }

    pub fn relations(&self) -> &Vec<RelationRef> {
        &self.relations
    }

    pub fn views(&self) -> &Vec<ViewRef> {
        &self.views
    }
}

impl Visitor for Elements {
    fn visit_relation<T>(&mut self, relation: &crate::Relation<T>)
    where
        T: Tuple,
    {
        self.relations.push(relation.name.clone());
    }

    fn visit_view<T, E>(&mut self, view: &crate::View<T, E>)
    where
        T: crate::Tuple,
        E: crate::Expression<T>,
    {
        self.views.push(view.reference.clone());
    }
}

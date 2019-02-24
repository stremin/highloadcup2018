use std::collections::BinaryHeap;

pub struct TopN<T> {
    heap: BinaryHeap<T>,
    limit: usize,
}

impl<T: Ord> TopN<T> {
    pub fn new(limit: usize) -> TopN<T> {
        TopN { heap: BinaryHeap::with_capacity(limit + 1), limit }
    }

    pub fn push(&mut self, t: T) {
        if self.heap.len() < self.limit {
            self.heap.push(t);
            return;
        }
        if &t >= self.heap.peek().unwrap() {
            return;
        }
        self.heap.push(t);
        self.heap.pop();
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn into_sorted_vec(self) -> Vec<T> {
        self.heap.into_sorted_vec()
    }

    pub fn clear(&mut self) {
        self.heap.clear()
    }
}

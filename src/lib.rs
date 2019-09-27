use std::cell::RefCell;
use combine::stream::{Positioned, Resetable, StreamErrorFor, StreamOnce};
use combine::stream::easy::Errors;
use std::collections::VecDeque;
use std::collections::BTreeSet;
use std::io::Read;
use std::rc::Rc;

const CHUNK_SIZE: usize = 8096;

pub type CheckPointId = usize;

pub type CheckPoint = Rc<RefCell<usize>>;

type CheckPointSet = Rc<RefCell<BTreeSet<CheckPoint>>>;

pub struct ElasticBufferedReadStream<R: Read> {
    raw_read: R,
    buffer: VecDeque<[u8; CHUNK_SIZE]>,
    checkpoints: CheckPointSet,
    cursor_pos: usize,
    offset: u64, // The capacity of this parameter limits the size of the stream
}

impl<R: Read> ElasticBufferedReadStream<R> {
    pub fn new(read: R) -> Self {
        Self {
            raw_read: read,
            buffer: VecDeque::new(),
            checkpoints: Rc::new(RefCell::new(BTreeSet::new())),
            cursor_pos: 0,
            offset: 0,
        }
    }
}

impl<R: Read> StreamOnce for ElasticBufferedReadStream<R> {
    type Item = u8;
    type Range = u8; // TODO: Change it when we implement RangeStream
    type Position = u64;
    type Error = Errors<u8, u8, u64>;

    fn uncons(&mut self) -> Result<u8, StreamErrorFor<Self>> {
        let chunk_index = self.cursor_pos / CHUNK_SIZE;
        let item_index = self.cursor_pos % CHUNK_SIZE;

        if self.buffer.len() <= chunk_index {
            self.buffer.push_back([0; CHUNK_SIZE]);
            self.raw_read.read(self.buffer.back_mut().unwrap())?;
        }

        let chunk = self.buffer.get(chunk_index).unwrap(); // We can unwrap because self.buffer.len() > chunk_index
        let item = chunk[item_index]; //  item_index < CHUNK_SIZE
        self.cursor_pos += 1;

        Ok(item)
    }
}

impl<R: Read> Positioned for ElasticBufferedReadStream<R> {
    fn position(&self) -> Self::Position {
        self.offset + self.cursor_pos as u64
    }
}

impl<R: Read> Resetable for ElasticBufferedReadStream<R> {
    type Checkpoint = CheckPoint;

    fn checkpoint(&self) -> Self::Checkpoint {
        let cp = Rc::new(RefCell::new(self.cursor_pos));
        self.checkpoints.borrow_mut().insert(cp.clone());

        cp
    }

    fn reset(&mut self, checkpoint: Self::Checkpoint) {
        self.cursor_pos = *checkpoint.borrow();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

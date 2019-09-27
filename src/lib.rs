use std::cell::{RefCell, Cell};
use combine::stream::{Positioned, Resetable, StreamErrorFor, StreamOnce};
use combine::stream::easy::Errors;
use std::collections::VecDeque;
use std::io::Read;
use std::rc::{Rc, Weak};

const CHUNK_SIZE: usize = 8096;

pub type CheckPointId = usize;

pub type InternalCheckPoint = Cell<usize>;

#[derive(Clone)]
pub struct CheckPoint(Rc<InternalCheckPoint>);

impl CheckPoint {
    fn new(pos: usize) -> CheckPoint {
        CheckPoint(Rc::new(Cell::new(pos)))
    }

    fn inner(&self) -> usize {
        self.0.get()
    }
}

pub struct CheckPointHandler(Weak<InternalCheckPoint>);

impl CheckPointHandler {
    fn from_checkpoint(cp: &CheckPoint) -> CheckPointHandler {
        let weak_ref = Rc::downgrade(&cp.0);

        CheckPointHandler(weak_ref)
    }
}

struct CheckPointSet(RefCell<VecDeque<CheckPointHandler>>);

impl CheckPointSet {
    fn new() -> CheckPointSet {
        CheckPointSet(RefCell::new(VecDeque::new()))
    }

    fn insert(&self, pos: usize) -> CheckPoint {
        let cp = CheckPoint::new(pos);
        self.0.borrow_mut().push_back(CheckPointHandler::from_checkpoint(&cp));

        cp
    }

    fn min(&self) -> Option<usize> {
        let mut min: Option<usize> = None;

        self.0.borrow_mut().retain(|cp| {
            if let Some(intern) = cp.0.upgrade() {
                let pos = intern.get();

                min = min.map(|val| val.min(pos));

                true
            } else {
                false
            }
        });

        min
    }
}

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
            checkpoints: CheckPointSet::new(),
            cursor_pos: 0,
            offset: 0,
        }
    }

    fn free_useless_chunks(&mut self) {
        // TODO: implement free_useless_chunks using CheckPointSet::min
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
            self.free_useless_chunks();
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
        self.checkpoints.insert(self.cursor_pos)
    }

    fn reset(&mut self, checkpoint: Self::Checkpoint) {
        self.cursor_pos = checkpoint.inner();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

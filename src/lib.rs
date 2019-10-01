use std::cell::{RefCell, Cell};
use combine::stream::{Positioned, Resetable, StreamErrorFor, StreamOnce};
use combine::stream::easy::Errors;
use std::collections::VecDeque;
use std::io::Read;
use std::rc::{Rc, Weak};
use core::num::NonZeroUsize;

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

                min = Some(if let Some(min_val) = min {
                    min_val.min(pos)
                } else {
                    pos
                });

                true
            } else {
                false
            }
        });

        min
    }

    fn sub_offset(&self, value: usize) {
        for cp in self.0.borrow().iter() {
            let handled = cp.0.upgrade().unwrap(); // sub_offset has to be called right after min, so there is no unhandled value
            handled.set(handled.get() - value);
        }
    }
}

pub struct ElasticBufferedReadStream<R: Read> {
    raw_read: R,
    buffer: VecDeque<[u8; CHUNK_SIZE]>,
    eof: Option<NonZeroUsize>,
    checkpoints: CheckPointSet,
    cursor_pos: usize,
    offset: u64, // The capacity of this parameter limits the size of the stream
}

impl<R: Read> ElasticBufferedReadStream<R> {
    pub fn new(read: R) -> Self {
        Self {
            raw_read: read,
            buffer: VecDeque::new(),
            eof: None,
            checkpoints: CheckPointSet::new(),
            cursor_pos: 0,
            offset: 0,
        }
    }

    fn free_useless_chunks(&mut self) { // TODO : sub_offset
        let min_checkpoint_pos = self.checkpoints.min();
        if let Some(min) = min_checkpoint_pos {
            self.buffer.drain(..min%CHUNK_SIZE);
        } else {
            self.buffer.drain(..self.buffer.len());
        }
    }
}

fn read_exact_or_eof<R: Read>(reader: &mut R, mut chunk: &mut [u8]) -> std::io::Result<Option<NonZeroUsize>> {
    while !chunk.is_empty() {
        match reader.read(chunk) {
            Ok(0) => break,
            Ok(n) => { let tmp = chunk; chunk = &mut tmp[n..];}
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {},
            Err(e) => return Err(e),
        }
    }

    Ok(NonZeroUsize::new(chunk.len()))
}

impl<R: Read> StreamOnce for ElasticBufferedReadStream<R> {
    type Item = u8;
    type Range = u8; // TODO: Change it when we implement RangeStream
    type Position = u64;
    type Error = Errors<u8, u8, u64>;

    fn uncons(&mut self) -> Result<u8, StreamErrorFor<Self>> {
        let chunk_index = self.cursor_pos / CHUNK_SIZE;
        let item_index = self.cursor_pos % CHUNK_SIZE;

        assert!(chunk_index <= self.buffer.len());

        if chunk_index == self.buffer.len() {
            assert!(self.eof.is_none());
            self.free_useless_chunks();
            self.buffer.push_back([0; CHUNK_SIZE]);
            self.eof = read_exact_or_eof(&mut self.raw_read, self.buffer.back_mut().unwrap())?;
        }

        if chunk_index == self.buffer.len() - 1 {
            if let Some(eof_pos_from_right) = self.eof {
                if item_index >= CHUNK_SIZE - eof_pos_from_right.get() {
                    return Err(StreamErrorFor::<Self>::end_of_input());
                }
            }
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
    use super::*;
    use combine::stream::StreamErrorFor;

    #[test]
    fn it_uncons() {
        let fake_read = &b"This is the text !"[..];
        let mut stream = ElasticBufferedReadStream::new(fake_read);
        assert_eq!(stream.uncons(), Ok(b'T'));
        assert_eq!(stream.uncons(), Ok(b'h'));
        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        for _ in 0..12 {
            stream.uncons().unwrap();
        }

        assert_eq!(stream.uncons(), Ok(b'!'));
        assert_eq!(stream.uncons(), Err(StreamErrorFor::<ElasticBufferedReadStream<&[u8]>>::end_of_input()));
    }
}

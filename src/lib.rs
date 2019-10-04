use combine::stream::easy::Errors;
use combine::stream::{Positioned, Resetable, StreamErrorFor, StreamOnce};
use core::num::NonZeroUsize;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::Read;
use std::rc::{Rc, Weak};

const ITEM_INDEX_SIZE: usize = 13;
const ITEM_INDEX_MASK: usize = (1 << ITEM_INDEX_SIZE) - 1;
pub const CHUNK_SIZE: usize = 1 << ITEM_INDEX_SIZE;

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

struct CheckPointSet(RefCell<Vec<CheckPointHandler>>);

impl CheckPointSet {
    fn new() -> CheckPointSet {
        CheckPointSet(RefCell::new(Vec::new()))
    }

    fn insert(&self, pos: usize) -> CheckPoint {
        let cp = CheckPoint::new(pos);
        self.0
            .borrow_mut()
            .push(CheckPointHandler::from_checkpoint(&cp));

        cp
    }

    fn min(&self) -> Option<usize> {
        let mut min: Option<usize> = None;

        self.0.borrow_mut().retain(|cp| {
            if let Some(intern) = cp.0.upgrade() {
                let pos = intern.get();

                let min_val = min.map_or(pos, |min_val| pos.min(min_val));
                min = Some(min_val);

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

    fn chunk_index(&self) -> usize {
        self.cursor_pos >> ITEM_INDEX_SIZE
    }

    fn item_index(&self) -> usize {
        self.cursor_pos & ITEM_INDEX_MASK
    }

    fn free_useless_chunks(&mut self) {
        let checkpoint_pos_min = self.checkpoints.min();
        let global_pos_min =
            checkpoint_pos_min.map_or(self.cursor_pos, |cp_min| cp_min.min(self.cursor_pos));
        let drain_quantity = global_pos_min / CHUNK_SIZE;

        self.buffer.drain(..drain_quantity);

        let offset_delta = drain_quantity * CHUNK_SIZE;
        self.cursor_pos -= offset_delta;
        self.offset += offset_delta as u64;
        self.checkpoints.sub_offset(offset_delta);
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

fn read_exact_or_eof<R: Read>(
    reader: &mut R,
    mut chunk: &mut [u8],
) -> std::io::Result<Option<NonZeroUsize>> {
    while !chunk.is_empty() {
        match reader.read(chunk) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = chunk;
                chunk = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
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
        assert!(self.chunk_index() <= self.buffer.len());

        if self.chunk_index() == self.buffer.len() {
            assert!(self.eof.is_none());
            self.free_useless_chunks();
            self.buffer.push_back([0; CHUNK_SIZE]);
            self.eof = read_exact_or_eof(&mut self.raw_read, self.buffer.back_mut().unwrap())?;
        }

        if self.chunk_index() == self.buffer.len() - 1 {
            if let Some(eof_pos_from_right) = self.eof {
                if self.item_index() >= CHUNK_SIZE - eof_pos_from_right.get() {
                    return Err(StreamErrorFor::<Self>::end_of_input());
                }
            }
        }

        let chunk = self.buffer.get(self.chunk_index()).unwrap(); // We can unwrap because self.buffer.len() > chunk_index
        let item = chunk[self.item_index()]; //  item_index < CHUNK_SIZE
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
    fn it_uncons_on_one_chunk() {
        let fake_read = &b"This is the text !"[..];
        let mut stream = ElasticBufferedReadStream::new(fake_read);
        assert_eq!(stream.uncons(), Ok(b'T'));
        assert_eq!(stream.uncons(), Ok(b'h'));
        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        for _ in 0..12 {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'!'));
        assert_eq!(
            stream.uncons(),
            Err(StreamErrorFor::<ElasticBufferedReadStream<&[u8]>>::end_of_input())
        );
    }

    #[test]
    fn it_uncons_on_multiple_chunks() {
        let mut fake_read = String::with_capacity(CHUNK_SIZE * 3);

        let beautiful_sentence = "This is a sentence, what a beautiful sentence !";
        let number_of_sentences = CHUNK_SIZE * 3 / beautiful_sentence.len();
        for _ in 0..number_of_sentences {
            fake_read += beautiful_sentence;
        }

        let mut stream = ElasticBufferedReadStream::new(fake_read.as_bytes());

        assert_eq!(stream.uncons(), Ok(b'T'));
        assert_eq!(stream.uncons(), Ok(b'h'));
        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        let first_sentence_of_next_chunk_dist =
            CHUNK_SIZE + beautiful_sentence.len() - CHUNK_SIZE % beautiful_sentence.len();
        for _ in 0..first_sentence_of_next_chunk_dist {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        for _ in 0..first_sentence_of_next_chunk_dist {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'a'));
        assert_eq!(stream.uncons(), Ok(b' '));

        let dist_to_last_char = number_of_sentences * beautiful_sentence.len()
            - 10 // Letters already read : "This is a "
            - 2 * first_sentence_of_next_chunk_dist
            - 1;
        for _ in 0..dist_to_last_char {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'!'));
        assert_eq!(
            stream.uncons(),
            Err(StreamErrorFor::<ElasticBufferedReadStream<&[u8]>>::end_of_input())
        );
    }

    #[test]
    fn it_resets_on_checkpoint() {
        let mut fake_read = String::with_capacity(CHUNK_SIZE * 3);

        let beautiful_sentence = "This is a sentence, what a beautiful sentence !";
        let number_of_sentences = CHUNK_SIZE * 3 / beautiful_sentence.len();
        for _ in 0..number_of_sentences {
            fake_read += beautiful_sentence;
        }

        let mut stream = ElasticBufferedReadStream::new(fake_read.as_bytes());

        let first_sentence_of_next_chunk_dist =
            CHUNK_SIZE + beautiful_sentence.len() - CHUNK_SIZE % beautiful_sentence.len();
        for _ in 0..first_sentence_of_next_chunk_dist {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'T'));
        assert_eq!(stream.uncons(), Ok(b'h'));
        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        let cp = stream.checkpoint();

        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        stream.reset(cp);
        let cp = stream.checkpoint();

        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));

        for _ in 0..first_sentence_of_next_chunk_dist {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.uncons(), Ok(b'a'));

        stream.reset(cp);

        assert_eq!(stream.uncons(), Ok(b'i'));
        assert_eq!(stream.uncons(), Ok(b's'));
        assert_eq!(stream.uncons(), Ok(b' '));
    }

    #[test]
    fn it_free_useless_memory_when_reading_new_chunk() {
        let mut fake_read = String::with_capacity(CHUNK_SIZE * 3);

        let beautiful_sentence = "This is a sentence, what a beautiful sentence !";
        let number_of_sentences = CHUNK_SIZE * 3 / beautiful_sentence.len();
        for _ in 0..number_of_sentences {
            fake_read += beautiful_sentence;
        }

        let mut stream = ElasticBufferedReadStream::new(fake_read.as_bytes());

        let cp = stream.checkpoint();
        assert_eq!(stream.buffer_len(), 0);
        assert_eq!(stream.uncons(), Ok(b'T'));
        assert_eq!(stream.buffer_len(), 1);

        for _ in 0..CHUNK_SIZE {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.buffer_len(), 2);

        stream.reset(cp);

        assert_eq!(stream.uncons(), Ok(b'T'));

        for _ in 0..2 * CHUNK_SIZE {
            assert!(stream.uncons().is_ok());
        }

        assert_eq!(stream.buffer_len(), 1);
    }
}

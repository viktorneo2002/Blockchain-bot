use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;

pub struct SpscRing<T> {
    buffer: Vec<CachePadded<UnsafeCell<T>>>,
    capacity: usize,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

impl<T> SpscRing<T> {
    pub fn with_capacity_pow2(size: usize) -> Self {
        assert!(size.is_power_of_two(), "Capacity must be power of two");
        
        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(CachePadded::new(UnsafeCell::new(unsafe { std::mem::zeroed() })));
        }
        
        Self {
            buffer,
            capacity: size,
            mask: size - 1,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }
    
    pub fn push(&mut self, value: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Relaxed);
        
        if head.wrapping_sub(tail) == self.capacity {
            return Err(value);
        }
        
        unsafe {
            let idx = head & self.mask;
            *self.buffer[idx].get() = value;
        }
        
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }
    
    pub fn pop(&mut self) -> Option<T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        
        if tail == head {
            return None;
        }
        
        let idx = tail & self.mask;
        let value = unsafe {
            std::ptr::read(self.buffer[idx].get())
        };
        
        self.tail.store(tail.wrapping_add(1), Ordering::Relaxed);
        Some(value)
    }
}

unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

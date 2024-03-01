use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    thread::{self, sleep, spawn, JoinHandle},
    time::Duration,
};

use libc::_SC_CLK_TCK;

fn cpu_usage() -> u64 {
    let content = std::fs::read_to_string("/proc/stat").unwrap();
    let cpu_line = content.lines().next().unwrap().trim();
    let mut columns = cpu_line.split(char::is_whitespace).skip(2);
    let user = columns.next().unwrap().parse::<u64>().unwrap();
    let sys = columns.nth(1).unwrap().parse::<u64>().unwrap();
    user + sys
}

fn process_usage() -> u64 {
    let content = std::fs::read_to_string("/proc/self/stat").unwrap();
    let cpu_line = content.lines().next().unwrap().trim();
    let mut columns = cpu_line.split(char::is_whitespace).skip(13);
    let user = columns.next().unwrap().parse::<u64>().unwrap();
    let sys = columns.next().unwrap().parse::<u64>().unwrap();
    user + sys
}

type FnType = dyn FnOnce() + Send + 'static;

pub struct CollaborativeThreadPool {
    /// Sender to send work to the workers.
    work_channnel_sender: crossbeam_channel::Sender<Box<FnType>>,
    /// Receiver for workers to obtain work.
    work_channnel_receiver: crossbeam_channel::Receiver<Box<FnType>>,
    /// Control thread handle.
    control_thread: Option<JoinHandle<Vec<JoinHandle<()>>>>,
    /// How many cores should currently be used.
    cores_to_use: Arc<AtomicUsize>,
    /// How many threads are not parked.
    unparked_threads: Arc<AtomicUsize>,
    /// How often to check for rescaling the number of cores that should be used.
    rescale_period: Duration,
    /// Coordinate a shutdown or not yet.
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct CollaborativeThreadPoolOptions {
    pub rescale_period: Duration,
    pub const_extra_threads: usize,
    pub overcommit_factor: f64,
}

impl Default for CollaborativeThreadPoolOptions {
    fn default() -> Self {
        // clock tick frequency in Hz
        let frequency = _SC_CLK_TCK;
        let period_ms = 1000 / frequency;
        Self {
            rescale_period: Duration::from_millis(period_ms as u64),
            const_extra_threads: 0,
            overcommit_factor: 1.0,
        }
    }
}

impl CollaborativeThreadPool {
    pub fn new(opts: CollaborativeThreadPoolOptions) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(0);
        let capacity = num_cpus::get();
        let cores_to_use = Arc::new(AtomicUsize::new(0));
        let unparked_threads = Arc::new(AtomicUsize::new(capacity));
        let shutdown = Arc::new(AtomicBool::new(false));
        let rescale_period = opts.rescale_period;

        // for control thread
        let c = Arc::clone(&cores_to_use);
        let sd = Arc::clone(&shutdown);
        let ut = Arc::clone(&unparked_threads);

        let mut s = Self {
            work_channnel_sender: sender,
            work_channnel_receiver: receiver,
            cores_to_use,
            unparked_threads,
            rescale_period,
            shutdown,
            control_thread: None,
        };

        let mut thread_handles = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            thread_handles.push(s.spawn(thread_handles.len()));
        }
        let const_extra_threads = opts.const_extra_threads;
        let overcommit_factor = opts.overcommit_factor;

        let control_thread = spawn(move || {
            let mut c_usage = 0;
            let mut p_usage = 0;
            let mut current_cores = c.load(std::sync::atomic::Ordering::Relaxed);
            loop {
                if sd.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let new_p_usage = process_usage();
                let new_c_usage = cpu_usage();
                let cpu_diff = new_c_usage - c_usage;
                let proc_diff = new_p_usage - p_usage;
                // avoid div by 0
                if cpu_diff > 0 {
                    let cpu_portion = proc_diff as f64 / cpu_diff as f64;
                    let cores = capacity as f64 * cpu_portion;
                    let cores = cores * overcommit_factor;
                    let cores = cores.ceil() as usize;
                    let cores = if cores == 0 { 1 } else { cores };
                    let cores = cores + const_extra_threads;
                    // println!(
                    //     "cpu diff {}, proc diff {}, portion {}, cores {}",
                    //     cpu_diff, proc_diff, cpu_portion, cores,
                    // );
                    // only store the new value if it will be different
                    if cores != current_cores {
                        c.store(cores, std::sync::atomic::Ordering::Relaxed);
                        current_cores = cores;
                        let unparked = ut.load(std::sync::atomic::Ordering::Relaxed);
                        for thread in thread_handles.iter().take(current_cores).skip(unparked) {
                            thread.thread().unpark()
                        }
                    }
                }
                // else no change, try again next time
                c_usage = new_c_usage;
                p_usage = new_p_usage;
                sleep(rescale_period);
            }
            thread_handles
        });

        s.control_thread = Some(control_thread);

        s
    }

    fn spawn(&mut self, index: usize) -> JoinHandle<()> {
        // spawn another thread
        let receiver = self.work_channnel_receiver.clone();
        let cores_to_use = Arc::clone(&self.cores_to_use);
        let ut = Arc::clone(&self.unparked_threads);
        let shutdown = Arc::clone(&self.shutdown);
        let rescale_period = self.rescale_period;
        spawn(move || {
            loop {
                if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let ctu = cores_to_use.load(std::sync::atomic::Ordering::Relaxed);
                if index > ctu {
                    // don't process any items yet, try again next time
                    ut.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    thread::park();
                    ut.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                let Ok(f) = receiver.recv_timeout(rescale_period) else {
                    // try again to see if we should be running
                    continue;
                };
                f()
            }
        })
    }

    pub fn submit<F>(&mut self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.work_channnel_sender.send(Box::new(f)).unwrap();
    }

    pub fn shutdown(self) {
        let Self {
            work_channnel_sender,
            work_channnel_receiver,
            control_thread,
            cores_to_use: _,
            unparked_threads: _,
            rescale_period: _,
            shutdown,
        } = self;
        shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(work_channnel_sender);
        drop(work_channnel_receiver);
        if let Some(ct) = control_thread {
            let thread_handles = ct.join().unwrap();
            for t in thread_handles {
                // don't care about the error at this point
                let _ = t.join();
            }
        }
    }
}